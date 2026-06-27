FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/loop-ad_event_collector ./cmd/collector

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/loop-ad_event_collector /usr/local/bin/loop-ad_event_collector
WORKDIR /workspace
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/loop-ad_event_collector"]
