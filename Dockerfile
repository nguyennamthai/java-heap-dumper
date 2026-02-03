FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

ENV CGO_ENABLED=0
ENV GOOS=linux
RUN mkdir -p /dist

COPY . .

RUN go build -ldflags="-w -s" -o /dist/monitor cmd/monitor/main.go
RUN go build -ldflags="-w -s" -o /dist/publisher cmd/publisher/main.go
RUN go build -ldflags="-w -s" -o /dist/controller cmd/controller/main.go

FROM alpine:3.21

# Install certificate authority for TLS connections
RUN apk add --no-cache ca-certificates

WORKDIR /

COPY --from=builder /dist/monitor   /dist/monitor
COPY --from=builder /dist/publisher /dist/publisher
COPY --from=builder /dist/controller /bin/controller

ENTRYPOINT ["/bin/controller"]