# Stage 1 - Python requirements
FROM python:3.10-slim AS requirements-stage
WORKDIR /tmp
RUN pip install poetry==1.6.1
COPY ./pyproject.toml ./poetry.lock* /tmp/
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes --without dev

# Stage 2 - build
FROM python:3.10-slim
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ffmpeg
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*
COPY --from=requirements-stage /tmp/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8080
CMD exec gunicorn -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8080 main:app