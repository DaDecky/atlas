FROM golang:1.27.4-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /notifier ./cmd/notifier

FROM alpine:3.24
RUN adduser -D -u 10001 atlas && mkdir -p /data && chown atlas:atlas /data
COPY --from=build /notifier /usr/local/bin/notifier
USER atlas
EXPOSE 8093
HEALTHCHECK --interval=10s --timeout=3s CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8093/health"]
ENTRYPOINT ["notifier"]
