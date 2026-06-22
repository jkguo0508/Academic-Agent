# Paper-Agent 后端镜像 (FastAPI 网关 + Celery worker 共用一个镜像)
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple/

WORKDIR /app

# 系统依赖 (PyMuPDF / mysqlclient 编译需要)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential default-libmysqlclient-dev pkg-config curl \
    && rm -rf /var/lib/apt/lists/*

# 先装依赖，利用分层缓存
COPY pyproject.toml requirements-refactor.txt ./
RUN pip install --upgrade pip && \
    pip install -r requirements-refactor.txt && \
    pip install \
      requests python-dotenv pyyaml tenacity arxiv aiohttp \
      langchain-community simhash pymupdf \
      "pyautogen>=0.2.20,<0.3.0" "autogen-ext[openai]>=0.7.4" "autogen-agentchat>=0.7.4" \
      scikit-learn fastapi sse-starlette uvicorn langgraph chromadb \
      python-docx markdownify pandas beautifulsoup4 python-multipart

# 再拷贝源码
COPY . .

EXPOSE 8000

# 默认启动网关；worker 在 docker-compose 中用不同 command 覆盖
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
