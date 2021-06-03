// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import com.daml.metrics.{MetricName, Metrics}

final class JsonApiMetrics(override val registry: MetricRegistry) extends Metrics(registry) {

  object daml {
    private val Prefix: MetricName = MetricName.DAML

    object http_json_api {
      private val Prefix: MetricName = daml.Prefix :+ "http_json_api"

      val httpRequest: Timer = registry.timer(Prefix :+ "http_request")
      val websocketRequest: Counter = registry.counter(Prefix :+ "websocket_request")
    }
  }
}
