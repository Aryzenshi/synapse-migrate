FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev default-libmysqlclient-dev && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md ./
COPY synapse ./synapse
COPY dashboard ./dashboard
COPY demo ./demo
COPY tests/fixtures ./tests/fixtures
RUN pip install --no-cache-dir '.[mysql,postgresql]'
EXPOSE 8000 8501
