#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


docker-compose down -v

rm -rf graphite/conf/*
rm -rf graphite/statsd_conf/*

docker-compose up -d
