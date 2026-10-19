# Image for the local demo (cloud emulator, trade worker, web UI). Build context: repo root.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH=/app/lambda:/app/local_app

RUN pip install --no-cache-dir uv

WORKDIR /app

# Dependencies first (cached layer): exactly the locked 'local' group, nothing else.
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --only-group local --no-install-project

COPY lambda ./lambda
COPY local_app ./local_app

RUN useradd --create-home app && chown -R app:app /app
USER app

EXPOSE 9090
CMD ["python", "local_app/web_app.py"]
