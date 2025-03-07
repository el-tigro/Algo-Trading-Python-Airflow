#!/bin/sh
echo "Starting Entrypoint"

airflow db init

airflow scheduler \
  & exec airflow webserver --pid /tmp/airflow.pid
