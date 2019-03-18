FROM golang:1.11
ARG VERSION=none
WORKDIR /opt/faustian/src
COPY ./ ./
RUN CGO_ENABLED=0 go build -ldflags "-X main.BuildTime=`date -u '+%Y-%m-%d_%I:%M:%S%p'` -X main.Version=$VERSION" -a -installsuffix cgo -o ../faustian ./faustian/main.go

FROM scratch
COPY --from=0 /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=0 /opt/faustian/faustian /
COPY --from=0 /usr/share/zoneinfo /usr/share/zoneinfo
# Create the /tmp directory
WORKDIR /tmp
WORKDIR /
ENTRYPOINT ["/faustian"]
