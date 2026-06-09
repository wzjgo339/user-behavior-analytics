FROM nginx:latest
COPY nginx.conf /etc/nginx/nginx.conf
COPY html /usr/share/nginx/html
# 删除到 /dev/stdout 的软链接，改为真实文件（Filebeat 需要）
RUN rm -f /var/log/nginx/access.log /var/log/nginx/error.log && \
    touch /var/log/nginx/access.log /var/log/nginx/error.log
