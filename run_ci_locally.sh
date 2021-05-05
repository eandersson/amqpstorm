#!/bin/bash
set -o pipefail
set -e

# Build RabbitMQ container and start it.
docker rm amqpstormdev -f || true
docker build -t amqpstormdev ./docker/
docker run -d --hostname rmq.eandersson.net --name amqpstormdev -p 5671:5671 -p 5672:5672 -p 15671:15671 -p 15672:15672 amqpstormdev
docker cp amqpstormdev:/etc/rabbitmq/ssl/ ./amqpstorm/tests/resources/

# Wait for RabbitMQ to startup properly.
docker exec amqpstormdev wait-for-rabbitmq

# Add user.
docker exec amqpstormdev rabbitmqctl add_user 'eandersson' '2a55f70a841f18b'
docker exec amqpstormdev rabbitmqctl -p / set_permissions 'eandersson' '.*' '.*' '.*'
docker exec amqpstormdev rabbitmqctl set_user_tags eandersson administrator

# Confirm all ports are reachable.
nc -zv rmq.eandersson.net 5671  || exit 1
nc -zv rmq.eandersson.net 5672  || exit 1
nc -zv rmq.eandersson.net 15671 || exit 1
nc -zv rmq.eandersson.net 15672 || exit 1

# Run tests.
nosetests -v -l DEBUG --logging-level=DEBUG --with-coverage --cover-package=amqpstorm --with-timer --timer-top-n 10
flake8 --ignore=F821 amqpstorm/
