"""
PDF 全文管线: 下载 -> 解析 -> 分块 -> (供 reading_agent 使用)。

设计要点:
- 优雅降级: 任一论文的下载/解析失败都不会中断整体流程，缺失全文时
  reading_agent 会回退到使用摘要等元数据。
- 并发治理: 下载阶段统一通过 src.utils.concurrency 的信号量+分批+重试执行。
- 可配置: 全部参数来自 system_params.yaml 的 pdf_pipeline.* 段。

配置 (system_params.yaml -> pdf_pipeline):
- enable              : 是否启用全文管线
- download_dir        : PDF 缓存目录
- request_timeout     : 单次下载超时(秒)
- max_retries         : 单篇下载最大重试次数
- chunk_size          : 分块大小(字符)
- chunk_overlap       : 分块重叠(字符)
- max_chars_per_paper : 喂给阅读模型的全文最大字符数(防止超长)
"""

import os
from typing import Any, Dict, List, Optional

import aiohttp

from src.core.config import config
from src.utils.concurrency import gather_in_batches, run_with_retry
from src.utils.log_utils import setup_logger

logger = setup_logger(__name__)


def _cfg(key: str, default: Any) -> Any:
    return config.get(f"pdf_pipeline.{key}", default)


def is_enabled() -> bool:
    return config.get_bool("pdf_pipeline.enable", True)


def _download_dir() -> str:
    directory = str(_cfg("download_dir", "data/pdf_cache"))
    if not os.path.isabs(directory):
        directory = os.path.abspath(directory)
    os.makedirs(directory, exist_ok=True)
    return directory


async def _fetch_pdf_bytes(url: str) -> bytes:
    timeout = aiohttp.ClientTimeout(total=float(_cfg("request_timeout", 60)))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.read()


async def download_pdf(pdf_url: str, paper_id: str) -> Optional[str]:
    """下载单篇 PDF 到本地缓存，返回本地路径；失败返回 None。"""
    if not pdf_url:
        return None
    safe_id = (paper_id or "paper").replace("/", "_").replace(":", "_")
    path = os.path.join(_download_dir(), f"{safe_id}.pdf")

    # 命中缓存直接复用
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path

    try:
        data = await run_with_retry(
            lambda: _fetch_pdf_bytes(pdf_url),
            max_attempts=int(_cfg("max_retries", 3)),
        )
        with open(path, "wb") as f:
            f.write(data)
        return path
    except Exception as e:  # noqa: BLE001
        logger.warning(f"下载PDF失败 [{paper_id}] {pdf_url}: {e}")
        return None


def parse_pdf_to_text(pdf_path: str) -> str:
    """将 PDF 解析为纯文本(使用 PyMuPDF)。失败返回空串。"""
    try:
        import fitz  # PyMuPDF
    except Exception as e:  # noqa: BLE001
        logger.error(f"未安装 PyMuPDF(fitz)，无法解析PDF: {e}")
        return ""
    try:
        text_parts: List[str] = []
        with fitz.open(pdf_path) as doc:
            for page in doc:
                text_parts.append(page.get_text("text"))
        return "\n".join(text_parts).strip()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"解析PDF失败 {pdf_path}: {e}")
        return ""


def chunk_text(
    text: str,
    chunk_size: Optional[int] = None,
    overlap: Optional[int] = None,
) -> List[str]:
    """按字符滑动窗口对文本分块。"""
    if not text:
        return []
    size = int(chunk_size or _cfg("chunk_size", 1500))
    ov = int(overlap if overlap is not None else _cfg("chunk_overlap", 150))
    size = max(size, 1)
    ov = max(min(ov, size - 1), 0)

    chunks: List[str] = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + size, n)
        chunks.append(text[start:end])
        if end >= n:
            break
        start = end - ov
    return chunks


async def fetch_paper_fulltext(paper: Dict[str, Any]) -> str:
    """针对单篇论文执行 下载->解析->分块->截断，返回可直接喂给模型的全文。"""
    paper_id = str(paper.get("paper_id") or "")
    pdf_url = paper.get("pdf_url") or ""

    path = await download_pdf(pdf_url, paper_id)
    if not path:
        return ""

    text = parse_pdf_to_text(path)
    if not text:
        return ""

    # 分块后按预算拼接，控制喂给模型的总长度
    chunks = chunk_text(text)
    merged = "\n\n".join(chunks)
    max_chars = int(_cfg("max_chars_per_paper", 12000))
    if max_chars > 0 and len(merged) > max_chars:
        merged = merged[:max_chars]
    return merged


async def build_fulltext_map(papers: List[Dict[str, Any]]) -> Dict[str, str]:
    """并发为所有论文构建 {paper_id: 全文文本} 映射。

    任一论文失败都会被跳过(优雅降级)，不会影响其余论文。
    """
    if not is_enabled() or not papers:
        return {}

    factories = [(lambda p=p: fetch_paper_fulltext(p)) for p in papers]
    results = await gather_in_batches(factories, return_exceptions=True)

    fulltext_map: Dict[str, str] = {}
    for paper, res in zip(papers, results):
        pid = str(paper.get("paper_id") or "")
        if isinstance(res, Exception):
            logger.warning(f"获取全文异常 [{pid}]: {res}")
            continue
        if res:
            fulltext_map[pid] = res

    logger.info(
        f"PDF全文管线完成: {len(fulltext_map)}/{len(papers)} 篇成功解析全文"
    )
    return fulltext_map
