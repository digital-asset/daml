// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.Slf4jReporter.LoggingLevel
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricRegistry, Slf4jReporter}

/** Manages metrics and reporters. Creates and starts a JmxReporter and creates an Slf4jReporter as well, which dumps
  * its metrics when this object is closed
  * <br/><br/>
  * Note that metrics are in general light-weight and add negligible overhead. They are not visible to everyday
  * users so they can be safely enabled all the time. */
final class MetricsReporting(metrics: MetricRegistry, jmxDomain: String) extends AutoCloseable {

  private val jmxReporter = JmxReporter
    .forRegistry(metrics)
    .inDomain(jmxDomain)
    .build
  jmxReporter.start()

  private val slf4jReporter = Slf4jReporter
    .forRegistry(metrics)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .withLoggingLevel(LoggingLevel.DEBUG)
    .build()

  override def close(): Unit = {
    slf4jReporter.report()
    jmxReporter.close()
  }
}
