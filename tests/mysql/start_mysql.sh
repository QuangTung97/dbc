#!/bin/bash
docker run \
  --name mysql_dbc_test \
  -e MYSQL_ROOT_PASSWORD=pass \
  -e MYSQL_DATABASE=testdb \
  -p 3306:3306 \
  -d mysql:8.0-bookworm
