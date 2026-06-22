from autogen_agentchat.agents import AssistantAgent
# from pydantic import BaseModel, Field
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional,Dict,Any
from src.utils.log_utils import setup_logger
from src.core.prompts import reading_agent_prompt
from src.core.model_client import create_default_client, create_reading_model_client
from src.core.state_models import BackToFrontData
from src.core.state_models import State,ExecutionState
from src.services.chroma_client import ChromaClient
from src.knowledge.knowledge import knowledge_base
from src.core.config import config
from src.infra import redis_client
from src.infra.locks import async_redis_lock
from src.tasks.pdf_pipeline import build_fulltext_map
from src.utils.concurrency import gather_in_batches, run_with_retry
import re, json, ast
import asyncio

logger = setup_logger(__name__)

class KeyMethodology(BaseModel):
    name: Optional[str] = Field(default=None, description="方法名称（如“Transformer-based Sentiment Classifier”）")
    principle: Optional[str] = Field(default=None, description="核心原理")
    novelty: Optional[str] = Field(default=None, description="创新点（如“首次引入领域自适应预训练”）")


class ExtractedPaperData(BaseModel):
    # paper_id: str = Field(default=None, description="论文ID")
    core_problem: str = Field(default=None, description="核心问题")
    key_methodology: KeyMethodology = Field(default=None, description="关键方法")
    datasets_used: List[str] = Field(default=[], description="使用的数据集")
    evaluation_metrics: List[str] = Field(default=[], description="评估指标")
    main_results: str = Field(default="", description="主要结果")
    limitations: str = Field(default="", description="局限性")
    contributions: List[str] = Field(default=[], description="贡献")
    # author_institutions: Optional[str]  # 如“Stanford University, Department of CS”
    
    # 清理空字符串和列表
    @field_validator("datasets_used", "evaluation_metrics", "contributions", mode="before")
    @classmethod
    def _validate_list_fields(cls, v):
        if v is None or v == "":
            return []
        if isinstance(v, str):
            return [v]
        return v

    @field_validator("core_problem", "main_results", "limitations", mode="before")
    @classmethod
    def _validate_str_fields(cls, v):
        if v is None:
            return ""
        return str(v)

# 创建一个新的Pydantic模型来包装列表
class ExtractedPapersData(BaseModel):
    papers: List[ExtractedPaperData] = Field(default=[], description="提取的论文数据列表")

def create_read_agent() -> AssistantAgent:
    """每次创建独立的阅读智能体实例。

    并发阅读多篇论文时，复用同一 AssistantAgent 会共享内部 model_context，
    造成上下文串扰。为每篇论文使用独立实例可避免该问题。
    """
    return AssistantAgent(
        name="read_agent",
        model_client=create_reading_model_client(),
        system_message=reading_agent_prompt,
        output_content_type=ExtractedPaperData,
        model_client_stream=True
    )


def build_reading_task(paper: Dict[str, Any], fulltext: Optional[str]) -> str:
    """构造喂给 reading_agent 的任务文本。

    若拿到 PDF 全文则附上全文(可能截断)；否则回退到仅使用论文元数据/摘要。
    """
    meta_keys = ["paper_id", "title", "authors", "summary", "published", "primary_category"]
    meta = {k: paper.get(k) for k in meta_keys if paper.get(k) is not None}
    meta_str = json.dumps(meta, ensure_ascii=False)
    if fulltext:
        return (
            "请基于以下论文信息提取结构化要点。\n"
            f"## 论文元数据\n{meta_str}\n\n"
            f"## 论文全文(可能为截断内容)\n{fulltext}"
        )
    return str(paper)

def sanitize_metadata(paper: Dict[str, Any]) -> Dict[str, Any]:
    new_meta = {}
    for k, v in paper.items():
        if v is None:
            continue
        if isinstance(v, list):
            new_meta[k] = ", ".join(str(x) for x in v)
        elif isinstance(v, dict):
            new_meta[k] = json.dumps(v, ensure_ascii=False)
        else:
            new_meta[k] = v
    return new_meta


async def add_papers_to_kb(papers:Optional[List[Dict[str, Any]]], extracted_papers: ExtractedPapersData, task_id: Optional[str] = None):
    """将提取的论文数据添加到知识库。

    二次开发修复：
    原实现用 config.set("tmp_db_id", db_id) 写全局单例，并发任务会互相覆盖，
    导致检索阶段读到别的任务的临时库。现改为：
    - 用分布式锁保护临时库创建 (避免并发重复创建)
    - 把 db_id 按 task_id 存入 Redis (task 级隔离)，供 retrieval_tool 读取
    """
    embedding_dic = config.get("embedding-model")
    embedding_provider = embedding_dic.get("model-provider")
    provider_dic = config.get(embedding_provider)
    
    embed_info = {
        "name": embedding_dic.get("model"),
        "dimension": embedding_dic.get("dimension"),
        "base_url": provider_dic.get("base_url"),
        "api_key": provider_dic.get("api_key"),
    }
    kb_type = config.get("KB_TYPE")
    # 分布式锁保护：同一 task 的临时库创建串行，避免并发重复创建
    lock_name = f"tmp_kb:{task_id or 'global'}"
    async with async_redis_lock(lock_name, timeout=120.0, blocking_timeout=30.0):
        database_info = await knowledge_base.create_database(
            "临时知识库", "用于存储临时提取的论文数据，仅用于本次报告的生成，用完即删", kb_type=kb_type, embed_info=embed_info, llm_info=None,
        )
    db_id = database_info["db_id"]
    # 按 task_id 存入 Redis，实现任务级隔离 (取代全局 config.set)
    if task_id:
        try:
            await redis_client.get_async_redis().set(
                redis_client.task_db_id_key(task_id), db_id, ex=86400
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("写入任务级 db_id 失败，回退到全局 config: %s", exc)
            config.set("tmp_db_id", db_id)
    else:
        config.set("tmp_db_id", db_id)  # 本地回退
    
    # 注释掉原本的代码，因为papers中包含了一些None值，导致报错
    # documents = [json.dumps(paper.model_dump(), ensure_ascii=False) for paper in extracted_papers.papers],
    # metadatas = [paper for paper in papers],
    # ids = [str(i) for i in range(len(papers))]
    
    documents=[json.dumps(paper.model_dump(),ensure_ascii=False) for paper in extracted_papers.papers]
    sanitized_metadatas = []
    if papers:
        for paper in papers:
           # new_meta = {}
           # for k, v in paper.items():
            #     if isinstance(v, list):
            #         new_meta[k] = ", ".join(str(x) for x in v)
            #     else:
            #         new_meta[k] = v
            # sanitized_metadatas.append(new_meta)
            sanitized_metadatas.append(sanitize_metadata(paper))          
    metadatas = sanitized_metadatas
    
    # # 确保 ids, metadatas 和 documents 长度一致
    # # 注意：这里假设 extracted_papers.papers 和 papers 是一一对应的
    # min_len = min(len(documents), len(metadatas))
    # documents = documents[:min_len]
    # metadatas = metadatas[:min_len]
    # ids = [str(i) for i in range(min_len)]
    ids = [str(i) for i in range(len(documents))] 
    
    data = {
        "documents": documents,
        "metadatas": metadatas,
        "ids": ids,
    }

    await knowledge_base.add_processed_content(db_id, data)


async def reading_node(state: State) -> State:
    """搜索论文节点"""
    state_queue = state["state_queue"]
    current_state = state["value"]
    current_state.current_step = ExecutionState.READING
    await state_queue.put(BackToFrontData(step=ExecutionState.READING,state="initializing",data=None))

    papers = current_state.search_results or []

    # === PDF 全文管线：下载 -> 解析 -> 分块 -> 喂给 reading_agent ===
    await state_queue.put(BackToFrontData(step=ExecutionState.READING,state="thinking",data="正在下载并解析论文PDF全文\n"))
    fulltext_map = await build_fulltext_map(papers)
    current_state.paper_contents = fulltext_map
    await state_queue.put(BackToFrontData(step=ExecutionState.READING,state="thinking",data=f"PDF全文解析完成，成功 {len(fulltext_map)}/{len(papers)} 篇\n"))

    # 为每篇论文构造阅读任务（含全文，缺失时回退到摘要元数据）
    reading_inputs = [
        build_reading_task(paper, fulltext_map.get(str(paper.get("paper_id") or "")))
        for paper in papers
    ]

    # 并发治理：信号量限流 + 分批 + tenacity 重试；每篇使用独立 read_agent 实例
    async def _read_one(task_input: str):
        agent = create_read_agent()
        return await run_with_retry(lambda: agent.run(task=task_input))

    results = await gather_in_batches(
        [(lambda inp=inp: _read_one(inp)) for inp in reading_inputs],
        return_exceptions=True,
    )

    # 合并结果
    extracted_papers = ExtractedPapersData()
    # 注释掉原本的代码，防止数据格式导致报错
    # for result in results:
    #     if result.messages[-1].content:
    #         parsed_paper = result.messages[-1].content
    #         extracted_papers.papers.append(parsed_paper)   
    
    # 清洗和预处理获取的数据    
    successful_papers = []
    for i, result in enumerate(results):
        # 并发重试后仍失败的论文会以异常形式返回，跳过以不中断整体流程
        if isinstance(result, Exception):
            logger.error(f"阅读论文失败(已重试): {result}")
            continue
        raw_content = result.messages[-1].content
        # logger.info(f"Reading Agent Raw Output: {raw_content}") # 打印原始输出
        
        if isinstance(raw_content, ExtractedPaperData):
            extracted_papers.papers.append(raw_content)
            successful_papers.append(papers[i])
            continue
        if isinstance(raw_content, dict):
            data = raw_content
        elif isinstance(raw_content, str):
            clean_content = raw_content.strip()
            if clean_content.startswith("```"):
                clean_content = re.sub(r"^```(?:json)?\s*", "", clean_content)
                clean_content = re.sub(r"\s*```$", "", clean_content)
            try:
                data = json.loads(clean_content)
            except json.JSONDecodeError:
                try:
                    data = ast.literal_eval(clean_content)
                except Exception:
                    logger.error(f"Failed to parse content as JSON or Python dict: {clean_content}")
                    continue
        else:
            logger.error(f"Unsupported content type: {type(raw_content)}")
            continue

        # 清理 Markdown 代码块
        # 3. 数据结构修正（处理列表包裹或 {"papers": ...} 包裹）
        if isinstance(data, list):
            if len(data) > 0:
                data = data[0] # 取第一个
            else:
                logger.warning("Parsed content is an empty list.")
                continue
        
        if isinstance(data, dict):
            # 如果被包裹在 "papers" 键中
            if "papers" in data and isinstance(data["papers"], list):
                if len(data["papers"]) > 0:
                    data = data["papers"][0]
            # 如果被包裹在 "paper" 键中
            elif "paper" in data and isinstance(data["paper"], dict):
                data = data["paper"]
        
        try:
            # 4. 验证并转换
            parsed_paper = ExtractedPaperData.model_validate(data)
            extracted_papers.papers.append(parsed_paper)
            successful_papers.append(papers[i])
        except Exception as e:
            logger.error(f"Validation failed for data: {data}. Error: {e}")
            # extracted_papers.papers.append(ExtractedPaperData()) 


     # 还得存入向量数据库中
    # await add_papers_to_kb(papers,extracted_papers)
    await add_papers_to_kb(successful_papers, extracted_papers, task_id=current_state.task_id)
        
    current_state.extracted_data = extracted_papers
    await state_queue.put(BackToFrontData(step=ExecutionState.READING,state="completed",data=f"论文阅读完成，共阅读 {len(extracted_papers.papers)} 篇论文"))
    return {"value": current_state}


if __name__ == "__main__":
    paper = {
        'core_problem': 'Despite the rapid introduction of autonomous vehicles, public misunderstanding and mistrust are prominent issues hindering their acceptance.'
    }
    chroma_client = ChromaClient()
    chroma_client.add_documents(
        documents=[paper],
        metadatas=[paper],
    )   
