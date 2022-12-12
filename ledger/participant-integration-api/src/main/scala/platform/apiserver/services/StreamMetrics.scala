// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.daml.metrics.api.MetricHandle.Counter

object StreamMetrics {
  def countElements[Out](counter: Counter): Flow[Out, Out, NotUsed] =
    Flow[Out].map { item =>
      counter.inc()
      item
    }
}
