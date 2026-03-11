#!/bin/bash
docker run \
  --name postgres_dbc_test \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=pass \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  -d postgres:16.11
