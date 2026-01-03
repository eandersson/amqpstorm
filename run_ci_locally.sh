#!/bin/bash
set -o pipefail
set -e

# Build RabbitMQ container and start it.
docker rm amqpstormdev -f || true
docker build -t amqpstormdev ./docker/
docker run -d --hostname rmq.eandersson.net --name amqpstormdev -p 5671:5671 -p 5672:5672 -p 15671:15671 -p 15672:15672 amqpstormdev
docker cp amqpstormdev:/etc/rabbitmq/ssl/ ./amqpstorm3/tests/resources/

# Wait for RabbitMQ to startup properly.
docker exec amqpstormdev wait-for-rabbitmq

# Print RabbitMQ version
echo "RabbitMQ Version: $(docker exec amqpstormdev rabbitmqctl --version)"

# Add user.
docker exec amqpstormdev rabbitmqctl add_user 'amqpstorm3' '2a55f70a841f18b'
docker exec amqpstormdev rabbitmqctl -p / set_permissions 'amqpstorm3' '.*' '.*' '.*'
docker exec amqpstormdev rabbitmqctl set_user_tags amqpstorm3 administrator

# Confirm all ports are reachable.
nc -zv rmq.eandersson.net 5671  || exit 1
nc -zv rmq.eandersson.net 5672  || exit 1
nc -zv rmq.eandersson.net 15671 || exit 1
nc -zv rmq.eandersson.net 15672 || exit 1

# Wait for a few seconds to make sure RabbitMQ has time so start properly.
sleep 3

# Run tests.
pytest --cov=./amqpstorm3 --durations=5
flake8 --ignore=F821 amqpstorm3/