FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

COPY pyproject.toml ./

COPY src/ ./src/

RUN touch README.md

RUN uv pip install --system .

ENTRYPOINT ["python", "-m", "src.main"]

CMD ["--help"]