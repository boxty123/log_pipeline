#==============================================================================
# Stage 1: builder
#==============================================================================
FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

#==============================================================================
# Stage 2: runtime
#==============================================================================
FROM python:3.12-slim
RUN useradd --system --no-create-home --uid 10002 workeruser
WORKDIR /app
COPY --from=builder /install /usr/local
COPY consumer.py .
USER workeruser


CMD ["python", "-u", "consumer.py"]