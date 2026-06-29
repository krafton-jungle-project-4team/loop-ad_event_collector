# loop-ad_event_collector

Loop Ad Event Collector는 SDK와 데모 쇼핑몰에서 오는 HTTP 이벤트를 받아 Kafka
topic `loop-ad.events.raw`로 발행하는 서버입니다. 이 서버의 외부 연결은 Kafka뿐이며,
요청 body는 검증 후 Kafka message value로 그대로 보존합니다.

## Requirements

- Go 1.25 이상
- 접근 가능한 Kafka broker
- 발행 대상 Kafka topic

## HTTP API

| Method | Path | 설명 |
|---|---|---|
| `GET` | `/health` | ECS/NLB health check. 정상일 때 `200`과 `ok`를 반환합니다. |
| `POST` | `/` | SDK 기본 endpoint용 ingest path입니다. |
| `POST` | `/events` | 명시적 ingest path입니다. |

요청 `Content-Type`은 `application/json`이어야 하며 본문은 최대 256 KiB입니다.
브라우저 SDK 호출을 위해 ingest path는 `OPTIONS` preflight와
`Access-Control-Allow-Origin: *`를 지원합니다. 요청 body는
`loop-ad_event_sdk`의 payload 형식으로 검증합니다.

성공하면 `202 Accepted`와 아래 응답을 반환합니다.

```json
{"accepted":1}
```

주요 오류 응답:

- `400 Bad Request`: 빈 본문, 잘못된 JSON, SDK payload 검증 실패
- `413 Payload Too Large`: 256 KiB 초과
- `415 Unsupported Media Type`: `application/json`이 아닌 Content-Type
- `503 Service Unavailable`: Kafka 발행 실패

## Required Env

기본값 없이 시작 시점에 검증합니다.

| Env | 설명 |
|---|---|
| `LOOPAD_ENV` | 실행 환경 이름 |
| `LOOPAD_SERVICE_ID` | 서비스 식별자. 반드시 `event-collector`여야 합니다. |
| `PORT` | HTTP listen port. `0.0.0.0:${PORT}`로 listen합니다. |
| `LOOPAD_KAFKA_BOOTSTRAP_BROKERS` | 쉼표로 구분한 Kafka bootstrap broker 목록 |
| `LOOPAD_KAFKA_SECURITY_PROTOCOL` | Kafka 보안 프로토콜. 현재 `SASL_PLAINTEXT`만 지원합니다. |
| `LOOPAD_KAFKA_SASL_MECHANISM` | Kafka SASL 메커니즘. 현재 `SCRAM-SHA-512`만 지원합니다. |
| `LOOPAD_KAFKA_USERNAME` | Kafka SASL 사용자 이름 |
| `LOOPAD_KAFKA_PASSWORD` | Kafka SASL 비밀번호. secret env로 주입합니다. |
| `LOOPAD_EVENT_TOPIC` | 원본 이벤트 Kafka topic |

서버는 시작하자마자 `.env` 파일이 있으면 먼저 로드한 뒤, 실제 환경변수 전체를
파싱하고 검증합니다. `.env`가 없으면 ECS처럼 주입된 환경변수만 사용합니다.
필수 env가 없거나 형식이 틀리면 Kafka 연결 전에 바로 실패합니다.
현재 collector는 `SASL_PLAINTEXT`와 `SCRAM-SHA-512` Kafka 연결을 사용합니다.

로컬 예시는 [.env.example](.env.example)에 있습니다.

## Local Development

```bash
go test ./...
go vet ./...
```

Kafka가 준비된 상태에서 서버를 실행합니다.

```bash
cp .env.example .env
go run ./cmd/collector
```

health check:

```bash
curl -i http://localhost:8080/health
```

event ingest:

```bash
curl -i -X POST http://localhost:8080/ \
  -H 'Content-Type: application/json' \
  -H 'X-Request-Id: req_local_001' \
  -d '{
    "project_id": "demo-shoppingmall",
    "event_id": "evt_local_001",
    "user_id": "u_001",
    "session_id": "s_001",
    "event_time": "2026-06-27T10:00:00.000+09:00",
    "event_name": "product_view",
    "campaign_id": "cmp_001",
    "creative_id": "cr_001",
    "properties_json": "{\"page\":{\"path\":\"/products/sku-1\"}}"
  }'
```

## Kafka Message

- `key`: 특정 파티션별로 고정할 필요가 없어 비워 둡니다.
- `value`: HTTP 요청 본문 원문 JSON 바이트입니다.

Collector는 Kafka value를 다시 marshal하거나 ClickHouse row 형태로 변환하지
않습니다. ClickHouse 적재, 컬럼 매핑, 집계용 변환은 Kafka 이후 consumer의 책임입니다.

## Event Validation

검증 기준은 `loop-ad_event_sdk`의 `LoopAdEventPayload`입니다.

- 최상위 JSON 객체여야 합니다.
- SDK payload에 없는 최상위 필드는 거부합니다.
- `project_id`, `event_id`, `user_id`, `session_id`, `event_time`,
  `event_name`, `properties_json`은 비어 있으면 안 됩니다.
- `event_time`은 RFC3339/RFC3339Nano 문자열이어야 합니다.
- `properties_json`은 JSON 객체 문자열이어야 합니다.
- 숫자 필드는 JSON 숫자여야 하며 `quantity`는 0 이상 정수여야 합니다.

## Contributing

개발 기여 흐름과 문서 작성 규칙은 [CONTRIBUTING.md](CONTRIBUTING.md)를
참고합니다. 별도 문서가 꼭 필요하지 않은 내용은 README에 유지합니다.
