#!/bin/sh

while ! nc -z mongo 27017; do
    echo "Waiting for MongoDB..."
    sleep 3
done

export RABBITMQ_HOST=rabbitmq &&
export MONGO_HOST=mongo &&
python3 main.py >> output.log 2>&1