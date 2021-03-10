#!/usr/bin/env bash

docker-compose down -v 

rm -rf graphite/conf/*
rm -rf graphite/statsd_conf/*

docker-compose up -d
