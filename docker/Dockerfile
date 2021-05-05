FROM rabbitmq:management-alpine
RUN apk --update add openssl

env RABBITMQ_USE_LONGNAME=true
RUN mkdir -p /etc/rabbitmq/ssl/

COPY ./files/rabbitmq.conf /etc/rabbitmq/rabbitmq.conf
COPY ./files/wait-for-rabbitmq /bin/wait-for-rabbitmq
COPY ./files/generate-certs /etc/rabbitmq/ssl/generate-certs
COPY ./files/openssl.cnf /etc/rabbitmq/ssl/openssl.cnf

RUN /etc/rabbitmq/ssl/generate-certs && \
    chown -R rabbitmq.rabbitmq /etc/rabbitmq/ssl/*
