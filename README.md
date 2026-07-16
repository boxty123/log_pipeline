# Game Log Ingestion Pipeline

대량의 글로벌 게임 트래픽을 가정한 로그 수집 파이프라인입니다. FastAPI 수집 서버가 `POST /api/v1/logs`로 JSON 로그를 받아 Redis Streams 버퍼에 넣고, Consumer Worker가 배치 처리를 통해 MongoDB에 적재합니다.

- 요청 흐름:  client → api(:8000) → redis stream → consumer → mongodb
- 포트 개방:  api의 8000 하나만 호스트에 노출. redis/mongo는 `log-net` 내부 전용.

## 1. 문제 정의

- **R1. 버스트 트래픽 흡수** — 로그가 폭증해도 수집이 밀리거나 멈추지 않아야 합니다.
- **R2. 무손실** — 컴포넌트가 죽거나 재시작되는 상황에서도 유실을 최소화해야 합니다.
- **R3. 네트워크 격리 & 최소 노출** — 모든 컴포넌트는 compose 가상 네트워크 안에서만 통신하고, 외부에는 로그 수신 API 포트(8000)만 개방해야 합니다.
- **R4. 재현 가능한 기동** — `docker compose up` 한 번으로 전체 파이프라인이 올바른 순서로 기동되고, 장애 시 스스로 복구되어야 합니다.

## 2. 기술적 의사결정

**Redis Queue를 기반으로 MongoDB를 결합한 하이브리드 구조를 선택했습니다.**

### 왜 큐를 앞에 두는가 — 글로벌 대량 트래픽을 가정

**R1(버스트 트래픽 흡수)** - 로그가 순간적으로 폭증할 때 .log 파일이나 DB에 동기로 쓰면, I/O 지연이 HTTP 응답 지연으로 연결되고, 게임 서버 쪽 커넥션이 쌓이면서 로그 시스템 장애가 게임 서비스 장애로 번질 수 있습니다.

**R2(무손실)**- API는 Redis Stream에 `XADD`하고 즉시 응답합니다. 피크에는 로그가 큐에 쌓였다가 트래픽이 잦아들면 컨슈머가 처리하므로, MongoDB가 느려지거나 재시작 중이어도 수집은 멈추지 않습니다.

### 왜 Redis Streams인가 — at-least-once 보장

Redis의 일반 List(`LPUSH`/`BRPOP`)는 워커가 메시지를 꺼낸 직후 죽으면 메시지가 유실됩니다. Streams는 **Consumer Group과 ACK 메커니즘**을 제공해, 워커가 적재를 확정한 뒤에만 메시지를 완료 처리할 수 있으므로 at-least-once를 보장합니다 

반면 Kafka는 단일 호스트 docker-compose 규모에서 브로커+컨트롤러 구성과 파티션 관리에 부담이 있을 것으로 판단했습니다.

### 왜 MongoDB인가

게임 로그는 이벤트 타입마다 payload 스키마가 모두 다릅니다. 스키마리스 문서 저장에는 NoSQL 계열 DB가 적합하다고 판단했습니다. 또한 `.log` 파일 저장은 이후 조회가 어렵다고 판단했습니다.

### 로그 스키마 — 공통 필드 + 자유 payload

이벤트마다 데이터가 제각각인 게임 로그 특성을 고려해, **공통 필드와 이벤트별 데이터를 분리**하는 방식으로 정의했습니다.

요청 본문 (`POST /api/v1/logs`):

```json
{
  "event_type": "stage_clear",              // 필수 — 로그 종류
  "user_id": "user_12345",                  // 필수 — 행위 주체
  "payload": { "stage": 1, "score": 100 },  // 자유 형식 — 이벤트별 데이터
  "timestamp": 1783920026.15                // 선택 — 이벤트 발생 시각
}
```

- **공통 필드 + 자유 payload** — 모든 로그가 공유하는 건 `user_id`와 `event_type`뿐입니다. 이 둘만 필수로 검증하고 나머지는 `payload`(임의 JSON)로 열어, 새 이벤트 타입이 생겨도 API 수정이 필요없습니다.
- **`timestamp`** — 클라이언트가 발생 시각을 주면 쓰고, 없으면 서버 수신 시각으로 채웁니다.
- **서버가 추가하는 메타** — 수집 시 `log_id`(UUID)·`received_at`을 추가하고, Redis가 발급한 `stream_id`를 MongoDB `_id`로 사용합니다.

### 유실 방지

- **at-least-once (`XACK` + PEL)** — 적재가 성공한 뒤에만 `XACK`합니다. ACK 전에 워커가 죽으면 메시지는 PEL에 남고, 재기동 시 미처리분부터 회수해 재처리하므로 유실되지 않습니다.
- **중복 제거 (`_id = stream_id`)** — at-least-once는 "적재 성공 후 `XACK` 직전 크래시" 시 같은 로그를 재처리해 중복을 만들 수 있습니다. MongoDB 문서의 `_id`로 Redis stream_id를 사용해 중복 적재를 방지합니다.
- **Redis AOF (`appendonly yes`, `appendfsync everysec`)** — Redis 재시작에도 스트림 버퍼가 보존됩니다.
- **MongoDB 볼륨** — 적재된 로그는 네임드 볼륨에 저장됩니다.

## 3. 실행 가이드

### 3-1. 사전 요구사항

- **Docker Engine** + **Docker Compose v2** 
- 호스트의 **8000 포트가 비어 있어야 합니다** 

### 3-2. 기동

```bash
docker compose up -d --build
```

### 3-3. `docker compose up`이 실제로 하는 일

1. **이미지 준비** — `build:`가 있는 `api`·`consumer`는 각 Dockerfile로 빌드, `redis:7-alpine`·`mongo:7`은 pull.
2. **네트워크 생성** — 내부 전용 브리지 `log-net` 생성. 컨테이너는 **서비스명(`redis`, `mongo`)을 DNS로** 통신.
3. **볼륨 생성** — `redis-data`·`mongo-data` 네임드 볼륨 생성.
4. **의존성 순서 결정** — `depends_on`에 따라 `redis`·`mongo`를 먼저 기동.
5. **healthcheck 게이팅** — `condition: service_healthy`로 redis·mongo가 healthy 될 때까지 API·워커 기동 보류
6. **앱 레벨 연결** — 게이트 통과 후 API는 Redis 풀, 워커는 컨슈머 그룹을 생성.
7. **복구** — 이후 각 컨테이너는 `restart: unless-stopped`로 크래시 시 자동 재기동.

### 3-4. 기동 확인

```bash
docker compose ps
```

API 문서: `http://localhost:8000/docs`


### 3-5. 종료

```bash
docker compose down -v    # 볼륨까지 삭제
```

## 4. 검증 결과


### 4-1. 가상의 게임 로그 전송

<img width="600" alt="Image" src="https://github.com/user-attachments/assets/88a85753-b41a-4de9-b54e-692bb481f0af" />


### 4-2. Redis Stream 도달 확인

```bash
docker exec log-redis redis-cli XRANGE logs:stream - + COUNT 5
```

출력:

```text
1784045683657-0
log_id
2dfc8c4f-3603-4bd1-9a5b-8cb81a9f24d8
event_type
stage_clear
user_id
user_12345
payload
{"stage": 1, "score": 100}
timestamp
1783920026.15
received_at
1784045683.65675
```


### 4-3. MongoDB 최종 적재 확인

```bash
docker exec log-mongo mongosh --quiet logs \
  --eval 'print("count=" + db.game_logs.countDocuments()); db.game_logs.find().pretty()'
```

출력:

```text
count=1
[
  {
    _id: '1784045683657-0',
    log_id: '2dfc8c4f-3603-4bd1-9a5b-8cb81a9f24d8',
    event_type: 'stage_clear',
    user_id: 'user_12345',
    payload: { stage: 1, score: 100 },
    timestamp: 1783920026.15,
    received_at: 1784045683.65675
  }
]
```


### 4-4. 유실 방지 검증

동일 stream_id 재삽입 시 거부.

```bash
docker exec log-mongo mongosh --quiet logs --eval '
  try { db.game_logs.insertOne({_id:"1784045683657-0"}); }
  catch (e) { print("rejected code=" + e.code); }'
```

출력:

```text
rejected code=11000      # 중복키 거부
```


### 4-5. 입력 검증

필수 필드(`event_type`) 누락 시 FastAPI/Pydantic이 422를 반환:

```bash
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8000/api/v1/logs \
  -H "Content-Type: application/json" -d '{"user_id":"no_event"}'
```

출력:

<img width="600" alt="Image" src="https://github.com/user-attachments/assets/0590d17c-7466-4304-9b52-87357c231ce7" />


## 5. 프로젝트 구조

```
supercent_log/
├── docker-compose.yml       # api + redis + consumer + mongo
├── README.md
├── log-api/                 # 로그 수집 API (외부 개방 포트: 8000)
│   ├── Dockerfile
│   ├── main.py
│   ├── requirements.txt
│   └── .dockerignore
└── consumer/                # Redis Stream → MongoDB 적재
    ├── Dockerfile
    ├── consumer.py
    ├── requirements.txt
    └── .dockerignore
```

## 6. 세부 설계 결정 (Design Notes) 


- **멀티스테이지 빌드 + non-root** — builder에서 설치한 의존성만 런타임 이미지로 복사해 경량화, 전용 시스템 유저(uid 10001/10002)로 실행해 탈취 시 피해 최소화
- **복구 위임** — 워커는 앱 레벨 재시도 없이 연결 장애 시 종료하고, 미ACK 메시지는 PEL에 남아 재기동 시 회수되므로 유실이 없음 (재기동은 3-3의 `restart` 정책이 담당)
- **유휴 스트림 블로킹 처리** — `XREADGROUP ... BLOCK` 대기 중 로그 없어 나는 타임아웃(`redis.TimeoutError`)은 정상 처리, 진짜 연결 장애만 크래시→재시작
- **스트림 상한(`MAXLEN ~ 1,000,000`)** — 워커가 오래 죽어도 Redis 메모리 무한 증가 방지. 