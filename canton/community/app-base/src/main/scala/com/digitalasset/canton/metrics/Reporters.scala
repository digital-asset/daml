// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.Reporter
import io.prometheus.client.exporter.HTTPServer

object Reporters {

  class Prometheus(hostname: String, port: Int) extends Reporter {
    val server: HTTPServer = new HTTPServer.Builder().withHostname(hostname).withPort(port).build();
    override def close(): Unit = server.close()
  }

}
