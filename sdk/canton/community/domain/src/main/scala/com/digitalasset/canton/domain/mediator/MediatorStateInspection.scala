// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.tracing.TraceContext

private[mediator] class MediatorStateInspection(state: MediatorState) {
  def finalizedResponseCount()(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[Long] =
    state.finalizedResponseStore.count()

  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    state.locatePruningTimestamp(skip)

  def reportMaxResponseAgeMetric(oldestResponseTimestamp: Option[CantonTimestamp]): Unit =
    state.reportMaxResponseAgeMetric(oldestResponseTimestamp)
}
