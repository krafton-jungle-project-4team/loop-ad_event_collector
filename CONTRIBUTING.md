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

## Documentation

- README는 프로젝트 개요, 실행 방법, HTTP API, Kafka 메시지 형식, 이벤트 검증
  계약을 담습니다.
- 별도 문서는 꼭 필요한 경우에만 추가합니다. 기여 흐름은 이 파일에 둡니다.
- 참고한 외부 계약이나 작성 배경처럼 독자가 바로 실행하는 데 필요하지 않은 정보는
  README에 나열하지 않습니다.
- 새 개발 문서를 만들 때는 먼저 Diátaxis 유형을 하나 고릅니다: tutorial,
  how-to guide, reference, explanation.
- 문서가 여러 목적을 섞기 시작하면 목적별로 나눕니다.
- The Good Docs Project 템플릿을 참고해 파일 이름과 위치를 정합니다.
