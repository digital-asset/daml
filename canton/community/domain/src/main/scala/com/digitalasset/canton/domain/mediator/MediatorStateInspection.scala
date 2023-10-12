// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

private[mediator] class MediatorStateInspection(state: MediatorState) {
  def finalizedResponseCount()(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Long] =
    state.finalizedResponseStore.count()

  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Option[CantonTimestamp]] =
    state.locatePruningTimestamp(skip)

  def reportMaxResponseAgeMetric(oldestResponseTimestamp: Option[CantonTimestamp]): Unit =
    state.reportMaxResponseAgeMetric(oldestResponseTimestamp)
}
