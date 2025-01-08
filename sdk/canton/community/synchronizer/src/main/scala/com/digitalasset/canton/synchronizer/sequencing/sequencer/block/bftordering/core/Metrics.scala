// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core

import com.daml.metrics.api.MetricHandle.Timer
import com.daml.metrics.api.MetricsContext

import scala.concurrent.Future

object Metrics {

  def maybeTimed[X](future: Future[X], timer: Option[Timer])(implicit
      mc: MetricsContext
  ): Future[X] =
    timer.map(_.timeFuture(future)).getOrElse(future)
}
