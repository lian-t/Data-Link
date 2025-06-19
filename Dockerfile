FROM debian:latest

RUN mv /etc/localtime /etc/localtime.bak && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime

WORKDIR /app/linkadmin
COPY linkadmin /app/linkadmin

# 设置工作目录
WORKDIR /app/app/datalink

# 将二进制文件从编译环境中复制到Alpine镜像
COPY output/data-link /app/app/datalink

WORKDIR /app
COPY output/.env /app
COPY output/config.toml /app

RUN mkdir runtime && chmod 755 runtime && \
    chown root:root runtime

# 暴露应用程序运行的端口（如果有需要）
EXPOSE 6085

# 运行可执行文件
CMD ["/app/app/datalink/data-link", "-f", "config.toml"]