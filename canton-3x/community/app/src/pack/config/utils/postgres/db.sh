#!/usr/bin/env bash

# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

function check_file() {
  local -r file="$1"
  if [[ ! -e $file ]]; then
    echo "Please run this script from the directory containing the $file file."
    exit 1
  fi
}

function do_usage() {
  echo "Usage: $0 <setup|reset|drop|create-user|start|stop>"
  echo "  setup: create databases"
  echo "  reset: drop and recreate databases"
  echo "  drop:  drop databases"
  echo "  create-user: create user"
  echo "  start [durable]: start docker db. Without durable, it will remove the container after exit"
  echo "  resume: resume durable docker db"
  echo "  stop: stop docker db"
}

function do_setup() {
  for db in $(cat "databases")
  do
    echo "creating db ${db}"
    echo "create database ${DBPREFIX}${db}; grant all on database ${DBPREFIX}${db} to current_user;" | \
      PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT $POSTGRES_DB $POSTGRES_USER
  done
}

function do_drop() {
  for db in $(cat "databases")
  do
    echo "dropping db ${db}"
    echo "drop database if exists ${DBPREFIX}${db};" | \
    PGPASSWORD=$POSTGRES_PASSWORD psql -h $POSTGRES_HOST -p $POSTGRES_PORT $POSTGRES_DB $POSTGRES_USER
  done
}

function do_create_user() {
  echo "Creating user ${POSTGRES_USER} (assumes your default user can do that on ${POSTGRES_DB})..."
  echo "CREATE ROLE \"${POSTGRES_USER}\" LOGIN PASSWORD '${POSTGRES_PASSWORD}';ALTER USER \"${POSTGRES_USER}\" createdb;" | \
    psql -h $POSTGRES_HOST -p $POSTGRES_PORT ${POSTGRES_DB}
}

function do_start_docker_db() {
  if [ "$1" == "durable" ]; then
    removeDockerAfterExit=""
    echo "starting durable docker based postgres"
  else
    echo "starting non-durable docker based postgres"
    removeDockerAfterExit="--rm"
  fi
  docker run -d ${removeDockerAfterExit} --name canton-postgres \
    --shm-size 1024mb \
    --publish ${POSTGRES_PORT}:5432 \
    -e POSTGRES_USER=$POSTGRES_USER \
    -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
    -e POSTGRES_DB=$POSTGRES_DB \
    -v "$PWD/postgres.conf":/etc/postgresql/postgresql.conf \
    postgres:12 \
    -c 'config_file=/etc/postgresql/postgresql.conf'
}

function do_resume_docker_db() {
  echo "resuming docker based postgres"
  docker start canton-postgres
}

function do_stop_docker_db() {
  echo "stopping docker based postgres"
  docker stop canton-postgres
}

function check_env {
  if [[ -z "$POSTGRES_USER" || -z "$POSTGRES_HOST" || -z "$POSTGRES_DB" || -z "$POSTGRES_PASSWORD" || -z "$POSTGRES_PORT" ]]; then
    echo 1
  else
    echo 0
  fi
}

check_file "databases"
if [[ $(check_env) -ne 0 ]]; then
  echo "Looking for db.env as environment variables are not set: POSTGRES_USER, POSTGRES_HOST, POSTGRES_DB, POSTGRES_PASSWORD, POSTGRES_PORT."
  echo $(env | grep -v POSTGRES_PASSWORD | grep POSTGRES)
  check_file "db.env"
  source "db.env"
  echo $(env | grep -v POSTGRES_PASSWORD | grep POSTGRES)
  if [[ $(check_env) -ne 0 ]]; then
    echo "POSTGRES_ environment is not properly set in db.env"
    exit 1
  fi
else
  echo "Using host=${POSTGRES_HOST}, port=${POSTGRES_PORT} user=${POSTGRES_USER}, db=${POSTGRES_DB} from environment"
fi


case "$1" in
  setup)
    do_setup
    ;;
  reset)
    do_drop
    do_setup
    ;;
  drop)
    do_drop
    ;;
  create-user)
    do_create_user
    ;;
  start)
    do_start_docker_db $2
    ;;
  resume)
    do_resume_docker_db
    ;;
  stop)
    do_stop_docker_db
    ;;
  *)
    do_usage
    ;;
esac
