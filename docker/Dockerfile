FROM rabbitmq:management-alpine
RUN apk --update add openssl

env RABBITMQ_USE_LONGNAME=true
RUN mkdir -p /etc/rabbitmq/ssl/

COPY rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
COPY wait-for-rabbitmq /bin/wait-for-rabbitmq
COPY generate-certs /etc/rabbitmq/ssl/generate-certs
COPY openssl.cnf /etc/rabbitmq/ssl/openssl.cnf

RUN /etc/rabbitmq/ssl/generate-certs && \
    chown -R rabbitmq.rabbitmq /etc/rabbitmq/ssl/*
