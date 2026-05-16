# Stage 1: Modules caching
FROM golang:alpine AS modules
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

# Stage 2: Build
FROM golang:alpine AS builder
COPY --from=modules /go/pkg/mod /go/pkg/mod
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/tracker ./cmd/server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/processor ./cmd/processor/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/auth ./cmd/auth-server/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/management ./cmd/management/main.go

# Stage 3: Final image
FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/tracker /tracker
COPY --from=builder /bin/processor /processor
COPY --from=builder /bin/auth /auth
COPY --from=builder /bin/management /management
USER nonroot:nonroot
ENTRYPOINT ["/tracker"]
