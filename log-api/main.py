"""
Log Ingestion API Server
------------------------
POST /api/v1/logs 로 JSON 로그를 받아 Redis Streams에 비동기 적재하는 수집 서버.

설계 포인트:
- API는 로그를 '검증 → 큐에 push'만 하고 200을 반환 
- Redis 연결은 커넥션 풀로 재사용
- 스트림 길이 상한(MAXLEN ~)으로 Redis 메모리 폭주 방지
"""

import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 설정 (환경변수 주입 — docker-compose에서 오버라이드)
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_KEY = os.getenv("STREAM_KEY", "logs:stream")
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "1000000")) 

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("log-api")


# ---------------------------------------------------------------------------
# 요청 스키마
# ---------------------------------------------------------------------------
class GameLog(BaseModel):
    """게임 클라이언트/서버가 보내는 로그 한 건."""

    event_type: str = Field(..., examples=["stage_clear", "item_purchase"])
    user_id: str = Field(..., examples=["user_12345"])
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: float | None = Field(
        default=None, description="이벤트 발생 시각(epoch). 없으면 서버 수신 시각 사용"
    )


class IngestResponse(BaseModel):
    status: str
    log_id: str
    stream_id: str


# ---------------------------------------------------------------------------
# 앱 라이프사이클: Redis 커넥션 풀 생성/정리
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=3,
        health_check_interval=30,
    )
    logger.info("Redis pool created (%s:%s, stream=%s)", REDIS_HOST, REDIS_PORT, STREAM_KEY)
    yield
    await app.state.redis.aclose()
    logger.info("Redis pool closed")


app = FastAPI(title="Game Log Ingestion API", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# 엔드포인트
# ---------------------------------------------------------------------------
@app.post(
    "/api/v1/logs",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
)
async def ingest_log(log: GameLog) -> IngestResponse:
    """로그를 받아 Redis Stream에 push하고 200 OK 반환.
    """
    log_id = str(uuid.uuid4())
    entry = {
        "log_id": log_id,
        "event_type": log.event_type,
        "user_id": log.user_id,
        "payload": json.dumps(log.payload, ensure_ascii=False),
        "timestamp": str(log.timestamp or time.time()),
        "received_at": str(time.time()),
    }
    try:
        stream_id = await app.state.redis.xadd(
            STREAM_KEY,
            entry,
            maxlen=STREAM_MAXLEN,
            approximate=True,
        )
    except redis.RedisError as exc:
        logger.error("Redis push failed: %s", exc)
        # 큐가 죽었을 때는 503 → 클라이언트/LB가 재시도하도록 유도
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="log queue unavailable, retry later",
        ) from exc

    return IngestResponse(status="accepted", log_id=log_id, stream_id=stream_id)


@app.get("/healthz")
async def healthz():
    """컨테이너 헬스체크용: Redis 연결까지 확인."""
    try:
        await app.state.redis.ping()
    except redis.RedisError:
        raise HTTPException(status_code=503, detail="redis unreachable")
    return {"status": "ok"}