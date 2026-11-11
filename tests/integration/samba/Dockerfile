FROM alpine:3.22
RUN apk add --no-cache samba \
    && mkdir -p /shares/data \
    && chmod 777 /shares/data \
    && adduser -D testuser \
    && printf "testpass\ntestpass\n" | smbpasswd -a -s testuser
COPY smb.conf /etc/samba/smb.conf
EXPOSE 445
HEALTHCHECK --interval=2s --timeout=3s --retries=15 CMD nc -z localhost 445
CMD ["smbd", "--foreground", "--no-process-group", "--debug-stdout"]
