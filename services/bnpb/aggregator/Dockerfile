FROM golang:1.27.4-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS build
WORKDIR /app/aggregator
COPY telemetry/go.mod telemetry/go.sum ../telemetry/
COPY aggregator/go.mod aggregator/go.sum ./
RUN go mod download
COPY telemetry/ ../telemetry/
COPY aggregator/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /aggregator ./cmd/aggregator \
 && CGO_ENABLED=0 go build -trimpath -o /data-tool ./cmd/data-tool

FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf
RUN adduser -D -u 10001 atlas
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /aggregator /usr/local/bin/aggregator
COPY --from=build /data-tool /usr/local/bin/data-tool
USER atlas
EXPOSE 8083
HEALTHCHECK --interval=10s --timeout=3s CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8083/health"]
ENTRYPOINT ["aggregator"]
