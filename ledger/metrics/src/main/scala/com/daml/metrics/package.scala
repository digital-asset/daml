// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import io.prometheus.client

package object metrics {

  def gauge(name: String): client.Gauge = client.Gauge
    .build()
    .name(name)
    .help(name)
    .register()
  private[metrics] def counter(name: String): client.Counter = client.Counter
    .build()
    .name(name)
    .help(name)
    .register()

  def summary(name: String): client.Summary = client.Summary
    .build()
    .name(name)
    .help(name)
    .quantile(0.5, 0.01)
    .quantile(0.75, 0.01)
    .quantile(0.95, 0.001)
    .quantile(0.98, 0.001)
    .quantile(0.99, 0.001)
    .quantile(0.999, 0.0001)
    .maxAgeSeconds(10 * 60)
    .ageBuckets(10)
    .register()

  def summaryWithLabel(name: String): client.Summary = client.Summary
    .build()
    .name(name)
    .labelNames("application_id")
    .help(name)
    .quantile(0.5, 0.01)
    .quantile(0.75, 0.01)
    .quantile(0.95, 0.001)
    .quantile(0.98, 0.001)
    .quantile(0.99, 0.001)
    .quantile(0.999, 0.0001)
    .maxAgeSeconds(10 * 60)
    .ageBuckets(10)
    .register()

  def histogram(name: String): client.Histogram = client.Histogram
    .build()
    .name(name)
    .help(name)
    .register()

}
