from asyncio import Queue
from typing import List, Dict, Any, Optional,TypedDict
from pydantic import BaseModel, Field
from enum import Enum


class BackToFrontData(BaseModel):
    step: str
    state: str
    data: Any


class ExecutionState(str, Enum):
    """工作流执行状态枚举"""
    INITIALIZING = "initializing"
    SEARCHING = "searching"
    READING = "reading"
    PARSING = "parsing"
    EXTRACTING = "extracting"
    ANALYZING = "analyzing"
    WRITING_DIRECTOR = "writing_director"
    SECTION_WRITING = "section_writing"
    WRITING = "writing"
    REPORTING = "reporting"
    COMPLETED = "completed"
    FAILED = "failed"
    FINISHED = "finished"

class KeyMethodology(BaseModel):
    name: str  # 方法名称（如“Transformer-based Sentiment Classifier”）
    principle: str  # 核心原理
    novelty: str  # 创新点（如“首次引入领域自适应预训练”）


class ExtractedPaperData(BaseModel):
    paper_id: str  # 关联搜索结果的paper_id
    core_problem: str
    key_methodology: KeyMethodology
    datasets_used: List[str]  # 如["IMDB Dataset (50k reviews)", "SST-2"]
    evaluation_metrics: List[str]
    main_results: str  # 含关键数值，如“在IMDB上Accuracy达92.5%，优于BERT的89.3%”
    limitations: str
    contributions: List[str]
    # author_institutions: Optional[str]  # 如“Stanford University, Department of CS”
    # extract_source: dict  # 溯源：记录每个维度的提取章节，如{"core_problem": "Abstract, Introduction"}

# 创建一个新的Pydantic模型来包装列表
class ExtractedPapersData(BaseModel):
    papers: List[ExtractedPaperData]

class AnalysisResults(BaseModel):
    """分析模块产生的结构化结果"""
    topic_clusters: Optional[Dict[str, List[str]]] = Field(default=None, description="主题聚类, key: 主题名, value: 相关论文标识/要点列表")
    trend_analysis: Optional[Dict[int, int]] = Field(default=None, description="趋势分析, key: 年份, value: 论文数量")
    method_comparison: Optional[List[Dict[str, Any]]] = Field(default=None, description="方法对比表格数据")
    influential_authors: Optional[List[str]] = Field(default=None, description="高产作者列表")
    influential_institutions: Optional[List[str]] = Field(default=None, description="核心机构列表")
    cluster_themes: Optional[List[str]] = Field(default=None, description="各聚类主题名称列表")
    global_summary: Optional[str] = Field(default=None, description="全局分析的叙述性总结文本")

    def to_prompt_text(self) -> str:
        """将结构化分析转为供写作层使用的提示文本（结构驱动，而非裸 JSON 字符串）。"""
        parts: List[str] = []
        if self.global_summary:
            parts.append("## 全局分析\n" + self.global_summary)
        if self.cluster_themes:
            parts.append("## 主题聚类\n" + "\n".join(f"- {t}" for t in self.cluster_themes))
        if self.method_comparison:
            lines = []
            for m in self.method_comparison:
                if not isinstance(m, dict):
                    continue
                theme = m.get("theme", "")
                method = m.get("method", "")
                principle = m.get("principle") or ""
                lines.append(f"- [{theme}] {method}：{principle}")
            if lines:
                parts.append("## 方法对比\n" + "\n".join(lines))
        return "\n\n".join(parts)

class NodeError(BaseModel):
    search_node_error: Optional[str] = Field(default=None, description="搜索节点错误信息")
    reading_node_error: Optional[str] = Field(default=None, description="阅读节点错误信息")
    analyse_node_error: Optional[str] = Field(default=None, description="分析节点错误信息")
    writing_node_error: Optional[str] = Field(default=None, description="写作节点错误信息")
    report_node_error: Optional[str] = Field(default=None, description="报告生成节点错误信息")
    error: Optional[str] = Field(default=None, description="错误信息")

class PaperAgentState(BaseModel):
    """LangGraph工作流的全局状态对象"""
    # 用户输入
    frontend_data: Optional[BackToFrontData] = Field(default=None, description="前端展示数据")
    agent_logs: Dict[str, str] = Field(default_factory=dict, description="各智能体执行日志，key为智能体名称")
    user_request: str = Field(description="用户的原始输入请求")
    max_papers: int = Field(default=50, description="最大论文数量")
    task_id: Optional[str] = Field(default=None, description="请求级任务ID，用于状态隔离")
    
    # 执行状态
    current_step: ExecutionState = Field(default=ExecutionState.INITIALIZING, description="当前执行步骤")
    error: Optional[NodeError] = Field(default=None, description="错误信息")
    
    # 数据流
    # search_results: List[PaperMetadata] = Field(default_factory=list, description="检索到的论文元数据列表")
    search_results: Optional[List[Dict[str, Any]]] = Field(default_factory=list, description="检索到的论文元数据列表")
    paper_contents: Optional[Dict[str, str]] = Field(default_factory=dict, description="解析后的论文全文字典, key: paper_id, value: 文本内容")
    extracted_data: Optional[ExtractedPapersData] = Field(default_factory=list, description="提取后的结构化信息列表")
    analyse_results: Optional[AnalysisResults] = Field(default=None, description="分析洞察结果(结构化对象 AnalysisResults)")
    outline: Optional[str] = Field(default=None, description="报告大纲")
    writted_sections: Optional[List[str]] = Field(default=None, description="已写章节内容")
    report_markdown: Optional[str] = Field(default=None, description="最终生成的Markdown报告内容")
    
    # 配置与上下文
    llm_provider: Any = Field(default=None, description="LLM提供者实例", exclude=True)  # 排除序列化
    config: Dict[str, Any] = Field(default_factory=dict, description="运行时配置")

class State(TypedDict):
    """LangGraph兼容的状态定义"""
    state_queue: Queue
    value: PaperAgentState
    task_id: Optional[str]

class ConfigSchema(TypedDict):
    """LangGraph兼容的配置定义"""
    state_queue: Queue
    value: Dict[str, Any]