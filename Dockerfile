FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

ENV CGO_ENABLED=0
ENV GOOS=linux

COPY . .

RUN go build -ldflags="-w -s" -o /tmp/monitor cmd/monitor/main.go
RUN go build -ldflags="-w -s" -o /tmp/publisher cmd/publisher/main.go
RUN go build -ldflags="-w -s" -o /tmp/controller cmd/controller/main.go

FROM alpine:3.21

# Install certificate authority for TLS connections
RUN apk add --no-cache ca-certificates

WORKDIR /

COPY --from=builder /tmp/monitor   /dist/monitor
COPY --from=builder /tmp/publisher /dist/publisher
COPY --from=builder /tmp/controller /bin/controller

ENTRYPOINT ["/bin/controller"]