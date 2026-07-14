"""
Log Consumer Worker
-------------------
Redis Streams(logs:stream)에서 로그를 Consumer Group으로 읽어
MongoDB(logs.game_logs)에 배치 적재하는 워커.

설계 포인트:
- Consumer Group + XACK: 적재 성공 후에만 ACK → 워커가 죽어도 로그 유실 없음
  (미ACK 메시지는 PEL에 남아 재기동 시 재처리)
- 배치 읽기(COUNT) + insert_many: 대량 트래픽에서 DB 왕복 최소화
- BLOCK 대기: 폴링 낭비 없이 새 로그 도착 즉시 처리
- 워커 수평 확장: 같은 그룹에 컨슈머를 추가하면 자동으로 로그 분배
"""

import json
import logging
import os
import signal
import socket
import time

import redis
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError

# ---------------------------------------------------------------------------
# 환경변수
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_KEY = os.getenv("STREAM_KEY", "logs:stream")
GROUP_NAME = os.getenv("GROUP_NAME", "log-workers")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", socket.gethostname())

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "logs")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "game_logs")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "5000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("log-worker")

_shutdown = False


def _handle_signal(signum, _frame):
    """SIGTERM/SIGINT 수신 시 현재 배치까지 처리 후 종료 (graceful shutdown)."""
    global _shutdown
    logger.info("signal %s received, shutting down after current batch", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def parse_entry(msg_id: str, fields: dict) -> dict:
    """Redis Stream 엔트리를 MongoDB 도큐먼트로 변환."""
    doc = dict(fields)
    # stream_id를 _id로 사용 -> MongoDB에서 중복키 발생 시 BulkWriteError로 무시 가능
    doc["_id"] = msg_id
    try:
        doc["payload"] = json.loads(doc.get("payload", "{}"))
    except json.JSONDecodeError:
        pass  # 파싱 실패 시 원문 그대로 보존
    for key in ("timestamp", "received_at"):
        if key in doc:
            try:
                doc[key] = float(doc[key])
            except ValueError:
                pass
    return doc


def ensure_group(r: redis.Redis) -> None:
    """스트림/컨슈머 그룹이 없으면 생성"""
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        logger.info("consumer group '%s' created on '%s'", GROUP_NAME, STREAM_KEY)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
        logger.info("consumer group '%s' already exists", GROUP_NAME)


def connect():
    """Redis/MongoDB 연결 확인 후 커넥션 객체 반환."""
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        # 블로킹 XREADGROUP(block=BLOCK_MS)이 서버 응답을 기다리는 동안
        # 소켓 read가 먼저 끊기지 않도록 block보다 넉넉히 준다
        socket_timeout=BLOCK_MS / 1000 + 5,
    )
    r.ping()
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    mongo.admin.command("ping")
    logger.info("connected: redis=%s:%s mongo=%s", REDIS_HOST, REDIS_PORT, MONGO_URI)
    return r, mongo


def main() -> None:
    r, mongo = connect()
    ensure_group(r)
    collection = mongo[MONGO_DB][MONGO_COLLECTION]

    # 이전 워커가 ACK하지 못하고 죽은 메시지부터 회수 ("0"), 이후 새 메시지(">") 처리
    pending_first = True
    logger.info("worker '%s' started (batch=%d)", CONSUMER_NAME, BATCH_SIZE)

    while not _shutdown:
        read_id = "0" if pending_first else ">"
        try:
            resp = r.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_KEY: read_id},
                count=BATCH_SIZE,
                block=None if pending_first else BLOCK_MS,
            )
        except redis.TimeoutError:
            # 유휴 스트림에서 블로킹 read가 만료된 상황
            continue
        except redis.RedisError as exc:
            # 진짜 연결 장애만 종료 → Docker restart 정책이 재기동,
            # 미ACK 메시지는 PEL에 남아 재기동 시 회수됨 
            logger.error("redis read failed: %s, exiting for restart", exc)
            raise

        if not resp or not resp[0][1]:
            pending_first = False  # 밀린 메시지 소진 → 신규 메시지 대기 모드
            continue

        entries = resp[0][1]
        docs = [parse_entry(msg_id, fields) for msg_id, fields in entries]
        ids = [msg_id for msg_id, _ in entries]

        try:
            collection.insert_many(docs, ordered=False)
        except BulkWriteError as bwe:
            # 중복키(11000)=이미 적재된 로그이므로 무시.
            write_errors = bwe.details.get("writeErrors", [])
            fatal = [e for e in write_errors if e.get("code") != 11000]
            if fatal:
                logger.error("mongo insert failed (%s), will retry batch", fatal)
                time.sleep(3)
                continue
            logger.info("skipped %d duplicate logs (already ingested)", len(write_errors))
        except PyMongoError as exc:
            # DB 장애 시 ACK하지 않음 → 메시지는 PEL에 남아 재처리 보장
            logger.error("mongo insert failed (%s), will retry batch", exc)
            time.sleep(3)
            continue

        r.xack(STREAM_KEY, GROUP_NAME, *ids)
        logger.info("flushed %d logs to mongodb (last=%s)", len(ids), ids[-1])

    logger.info("worker stopped")


if __name__ == "__main__":
    main()