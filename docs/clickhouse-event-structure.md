# ClickHouse Event Structure

Event Collector는 HTTP payload를 Kafka topic `loop-ad.events.raw`로 발행합니다.
Kafka consumer 또는 ClickHouse Kafka engine은 이 메시지를 `JSONEachRow`로 읽어
`loopad.raw_events`에 적재합니다.

현재 기준 스키마는 `loop-ad_local-data-source_contract/clickhouse/schema.sql`의
`loopad.raw_events`입니다.

| Column | Type | Collector mapping |
|---|---|---|
| `event_id` | `String` | SDK `event_id`. Kafka key로도 사용합니다. |
| `user_id` | `String` | SDK `user_id`. 익명 이벤트는 SDK에서 보내지 않는 것을 기본으로 합니다. |
| `campaign_id` | `String` | SDK `campaign_id`. 없으면 빈 문자열로 들어갑니다. |
| `creative_id` | `String` | SDK `creative_id`. 없으면 빈 문자열로 들어갑니다. |
| `event_type` | `LowCardinality(String)` | SDK `event_name`, 또는 raw 입력의 `event_type`. |
| `occurred_at` | `DateTime64(3, 'UTC')` | SDK `event_time`을 UTC millisecond 문자열로 변환합니다. |
| `request_id` | `String` | `X-Request-Id`, payload `request_id`, 또는 Collector 생성값. |
| `payload` | `String` | raw `payload`가 있으면 사용하고, 없으면 원본 HTTP JSON을 compact한 문자열로 보존합니다. |

예시 HTTP payload:

```json
{
  "project_id": "demo-shoppingmall",
  "event_id": "evt_001",
  "user_id": "u_001",
  "session_id": "s_001",
  "event_time": "2026-06-27T10:00:00.000+09:00",
  "event_name": "product_view",
  "campaign_id": "cmp_001",
  "creative_id": "cr_001",
  "properties_json": "{\"page\":{\"path\":\"/products/sku-1\"}}"
}
```

Kafka에 발행되는 ClickHouse row:

```json
{
  "event_id": "evt_001",
  "user_id": "u_001",
  "campaign_id": "cmp_001",
  "creative_id": "cr_001",
  "event_type": "product_view",
  "occurred_at": "2026-06-27 01:00:00.000",
  "request_id": "req_...",
  "payload": "{\"project_id\":\"demo-shoppingmall\",...}"
}
```
