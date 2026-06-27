# Kafka Event Payload

Event Collector는 HTTP 이벤트를 검증한 뒤 Kafka topic `loop-ad.events.raw`에
발행합니다. Collector가 직접 연결하는 외부 시스템은 Kafka뿐입니다.

## Message

- `key`: 비워 둡니다.
- `value`: HTTP request body 원문 JSON bytes입니다.

Collector는 Kafka value를 다시 marshal하거나 ClickHouse row 형태로 변환하지
않습니다. ClickHouse 적재, 컬럼 매핑, 집계용 변환은 Kafka 이후 consumer의 책임입니다.

## Validation

검증 기준은 `loop-ad_event_sdk`의 `LoopAdEventPayload`입니다.

- top-level JSON object여야 합니다.
- SDK payload에 없는 top-level field는 거부합니다.
- `project_id`, `event_id`, `user_id`, `session_id`, `event_time`,
  `event_name`, `properties_json`은 비어 있으면 안 됩니다.
- `event_time`은 RFC3339/RFC3339Nano 문자열이어야 합니다.
- `properties_json`은 JSON object 문자열이어야 합니다.
- 숫자 필드는 JSON number여야 하며 `quantity`는 0 이상 정수여야 합니다.
