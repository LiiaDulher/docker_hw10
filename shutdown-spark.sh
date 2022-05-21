#!/bin/bash

docker stop spark-master
docker stop spark-worker
docker rm spark-master
docker rm spark-worker
docker network rm dulher-spark-network
