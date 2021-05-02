#!/bin/bash
set -o pipefail
set -e

# Build RabbitMQ container and start it.
docker rm rabbitmqdev -f || true
docker build -t rabbitmqdev ./docker/
docker run -d --hostname rmq.eandersson.net --name rabbitmqdev -p 5671:5671 -p 5672:5672 -p 15671:15671 -p 15672:15672 rabbitmqdev
docker cp rabbitmqdev:/etc/rabbitmq/ssl/ ./amqpstorm/tests/resources/

# Wait for RabbitMQ to startup properly.
docker exec rabbitmqdev wait-for-rabbitmq

# Add user.
docker exec rabbitmqdev rabbitmqctl add_user 'eandersson' '2a55f70a841f18b'
docker exec rabbitmqdev rabbitmqctl -p / set_permissions 'eandersson' '.*' '.*' '.*'
docker exec rabbitmqdev rabbitmqctl set_user_tags eandersson administrator

# Confirm all ports are reachable.
nc -zv rmq.eandersson.net 5671  || exit 1
nc -zv rmq.eandersson.net 5672  || exit 1
nc -zv rmq.eandersson.net 15671 || exit 1
nc -zv rmq.eandersson.net 15672 || exit 1

# Run tests.
nosetests -v -l DEBUG --logging-level=DEBUG --with-coverage --cover-package=amqpstorm --with-timer --timer-top-n 10
