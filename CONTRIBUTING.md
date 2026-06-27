# Contributing

Loop Ad Event Collector는 HTTP 이벤트를 검증하고 Kafka에 발행하는 작은 Go
서비스입니다. 변경은 이 책임 범위 안에서 작게 유지합니다.

## Scope

- Collector는 HTTP 요청을 받고 Kafka에 원문 JSON을 발행합니다.
- ClickHouse row 생성, 컬럼 매핑, 집계 처리는 이 repo의 책임이 아닙니다.
- 이벤트 payload 계약을 바꿀 때는 SDK payload 형식, collector 검증 코드,
  README 예시를 함께 갱신합니다.
- 환경변수를 바꿀 때는 `.env.example`, README의 설정 표, 배포 설정을 함께 확인합니다.

## Development Workflow

1. `.env.example`을 복사해 로컬 환경변수를 준비합니다.

   ```bash
   cp .env.example .env
   ```

2. Kafka가 준비된 상태에서 서버를 실행합니다.

   ```bash
   go run ./cmd/collector
   ```

3. 변경 전후로 검증 명령을 실행합니다.

   ```bash
   go fix ./...
   gofmt -w <changed-go-files>
   go test ./...
   go vet ./...
   ```
