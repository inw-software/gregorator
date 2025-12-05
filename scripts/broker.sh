#!/bin/sh
docker run -d --name rabbit \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:4-management

docker run -d --name mosquitto \
  -p 1883:1883 \
  -p 9001:9001 \
  eclipse-mosquitto
