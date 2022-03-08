// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.NotUsed
import akka.stream.scaladsl.Flow
import io.prometheus.client.Gauge

object StreamMetrics {
  def countElements[Out](counter: Gauge): Flow[Out, Out, NotUsed] =
    Flow[Out].map { item =>
      counter.inc()
      item
    }
}
