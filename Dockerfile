# Cross-compiles on the build host (pure Go, CGO_ENABLED=0) for each
# --platform target, so multi-arch builds need no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w" -o /out/k8s-agent .

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/k8s-agent /k8s-agent
USER nonroot:nonroot
ENTRYPOINT ["/k8s-agent"]
