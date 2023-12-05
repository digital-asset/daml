// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.metrics.api.MetricHandle.Counter
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

object StreamMetrics {
  def countElements[Out](counter: Counter): Flow[Out, Out, NotUsed] =
    Flow[Out].map { item =>
      counter.inc()
      item
    }
}
