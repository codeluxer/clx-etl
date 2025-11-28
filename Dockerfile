FROM python:3.13-bookworm

# 设置时区和避免交互式
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive
ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

# 安装系统依赖（包含 git 和构建常用工具）
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 安装 Poetry（官方推荐方式）
RUN curl -sSL https://install.python-poetry.org | python3 -

# 将 Poetry 加入 PATH
ENV PATH="/root/.local/bin:$PATH"

# 先复制 Poetry 的依赖文件（分层缓存更高效率）
COPY pyproject.toml poetry.lock* ./

# 安装依赖（不创建 venv，直接装到系统环境）
RUN poetry install --no-interaction --no-ansi --without dev

# 复制项目源码
COPY . .

# 默认启动命令（你可按需修改）
CMD ["python", "src/main.py"]
