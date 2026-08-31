# Copyright 2025 Veloxpack.io
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build once on the builder host; cross-compile release arches in this stage.
# Do not use TARGETARCH here so the builder cache is shared across platforms.
FROM --platform=$BUILDPLATFORM golang:1.27.0 AS builder
ARG TARGETOS
ARG RCLONE_BACKEND_MODE=all

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY cmd/rcloneplugin/ cmd/rcloneplugin
COPY pkg/ pkg/
COPY internal/ internal/

# Extract version information for build flags
ARG GIT_COMMIT
ARG BUILD_DATE
ARG DRIVER_VERSION=latest

# Cross-compile static binaries for all release arches in one builder invocation.
RUN RCLONE_VERSION=$(awk '/^[[:space:]]+github\.com\/rclone\/rclone v/{print $2; exit}' go.mod | sed 's/^v//') && \
    for arch in amd64 arm64; do \
      CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${arch} \
      go build -a \
      -ldflags="-X github.com/veloxpack/csi-driver-rclone/pkg/rclone.driverVersion=${DRIVER_VERSION} -X github.com/veloxpack/csi-driver-rclone/pkg/rclone.gitCommit=${GIT_COMMIT} -X github.com/veloxpack/csi-driver-rclone/pkg/rclone.buildDate=${BUILD_DATE} -X github.com/veloxpack/csi-driver-rclone/pkg/rclone.rcloneVersion=${RCLONE_VERSION} -s -w -extldflags '-static'" \
      -trimpath \
      -tags "netgo ${RCLONE_BACKEND_MODE}" \
      -o rcloneplugin.${arch} \
      cmd/rcloneplugin/main.go; \
    done

# Runtime image for the target platform only (matching binary copied from builder).
FROM registry.k8s.io/build-image/debian-base:bookworm-v1.0.8
ARG TARGETARCH
WORKDIR /

# Install required dependencies
RUN apt-get update && apt upgrade -y && clean-install ca-certificates fuse3 tzdata && \
    rm -rf /var/cache/apt/* /tmp/*

# Enable allow_other for FUSE mounts
RUN printf '%s\n' \
    "# Allow rclone to use the --allow-other flag" \
    "user_allow_other" >> /etc/fuse.conf

COPY --from=builder /workspace/rcloneplugin.${TARGETARCH} /rcloneplugin

ENTRYPOINT ["/rcloneplugin"]
