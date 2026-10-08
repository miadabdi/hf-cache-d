# hf-cache-d image: static binary, nothing else.
#
# The builder matches the local toolchain (go1.26); go.mod's `go 1.22`
# directive stays authoritative for language features and CI's version.
# gcr.io/distroless/static:nonroot runs as uid 65532 with no shell, no
# package manager — the binary is the whole image.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/hf-cache-d ./cmd/hf-cache-d

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/hf-cache-d /hf-cache-d
# Configuration is env-only (deploy/config.example.env documents the vars).
EXPOSE 8080
ENTRYPOINT ["/hf-cache-d"]
