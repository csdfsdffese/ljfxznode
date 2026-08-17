# Build go
FROM golang:1.26.0-alpine AS builder
WORKDIR /app
COPY . .
ENV CGO_ENABLED=0
RUN GOEXPERIMENT=jsonv2 go mod download
RUN GOEXPERIMENT=jsonv2 go build -v -o ljfxznode

# Release
FROM  alpine
# 安装必要的工具包
RUN  apk --update --no-cache add tzdata ca-certificates \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN mkdir /etc/ljfxznode/
COPY --from=builder /app/ljfxznode /usr/local/bin

ENTRYPOINT [ "ljfxznode", "server", "--config", "/etc/ljfxznode/config.json"]
