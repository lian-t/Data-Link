FROM golang:1.17.11-alpine3.16
#FROM golang:1.19
# Set destination for COPY

RUN set -eux \
    && sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories \
    && apk add alpine-conf \
    && /sbin/setup-timezone -z "Asia/Shanghai" \
    && apk del alpine-conf

WORKDIR /app

ENV GOPATH= \
    GOPROXY=https://proxy.golang.com.cn,direct \
    GOPRIVATE=git.mycompany.com,github.com/my/private
# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code. Note the slash at the end, as explained in
# https://docs.docker.com/engine/reference/builder/#copy
COPY . ./

# Build
RUN cd app/datalink && CGO_ENABLED=0 GOOS=linux go build -o datalink2 main.go

# Optional:
# To bind to a TCP port, runtime parameters must be supplied to the docker command.
# But we can document in the Dockerfile what ports
# the application is going to listen on by default.
# https://docs.docker.com/engine/reference/builder/#expose
EXPOSE 6085

# Run
CMD ["app/datalink/datalink2", "-f", "config.toml"]
