FROM golang:1.26 AS builder

ARG SERVICE=gateway
WORKDIR /src

COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/base-debian12

ARG SERVICE=gateway
ENV SERVICE_HTTP_ADDR=:8080

COPY --from=builder /out/service /service

EXPOSE 8080

ENTRYPOINT ["/service"]

