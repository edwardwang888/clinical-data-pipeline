# One image for every Python service (listener, consumer, producer, tests);
# docker-compose.yml picks the command.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv==0.11.32

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Dependencies first, so code changes don't reinstall them. Exact versions from uv.lock.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen

COPY . .
