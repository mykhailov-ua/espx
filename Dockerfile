# Stage 1: Modules caching
FROM golang:1.25-alpine AS modules
WORKDIR /src
COPY go.mod ./
# COPY go.sum ./ # Not present yet, but good to have in mind
RUN go mod download

# Stage 2: Build
FROM golang:1.25-alpine AS builder
COPY --from=modules /go/pkg/mod /go/pkg/mod
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/app ./cmd/server/main.go

# Stage 3: Final image
FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
