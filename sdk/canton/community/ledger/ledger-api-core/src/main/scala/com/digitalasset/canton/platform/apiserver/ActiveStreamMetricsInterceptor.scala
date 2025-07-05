// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.ActiveStreamCounterInterceptor

final class ActiveStreamMetricsInterceptor(
    metrics: LedgerApiServerMetrics
) extends ActiveStreamCounterInterceptor {

  private val activeStreamsGauge = metrics.lapi.streams.active

  override protected def established(methodName: String): Unit =
    activeStreamsGauge.updateValue(_ + 1)

  override protected def finished(methodName: String): Unit = activeStreamsGauge.updateValue(_ - 1)

}
