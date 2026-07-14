# =============================================================================
# Stage 1: builder
# =============================================================================
FROM python:3.12-slim AS builder

WORKDIR /build

## 의존성 먼저 설치
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# =============================================================================
# Stage 2: runtime 
# =============================================================================
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home --uid 10001 apiuser

WORKDIR /app


COPY --from=builder /install /usr/local

COPY main.py .

USER apiuser

EXPOSE 8000

# 컨테이너 자체 헬스체크: Redis 연결까지 확인하는 /healthz 사용
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1

# 프로덕션 기동: uvicorn 워커 (트래픽 증가 시 --workers 값 또는 컨테이너 수평 확장)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]