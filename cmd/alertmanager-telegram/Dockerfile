FROM golang:alpine AS builder
WORKDIR /src
COPY cmd/alertmanager-telegram/main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/proxy main.go

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/proxy /proxy
USER nonroot:nonroot
ENTRYPOINT ["/proxy"]
