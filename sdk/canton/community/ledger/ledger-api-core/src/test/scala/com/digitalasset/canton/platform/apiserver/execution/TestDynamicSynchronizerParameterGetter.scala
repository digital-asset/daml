// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class TestDynamicSynchronizerParameterGetter(
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
)(implicit
    ec: ExecutionContext
) extends DynamicSynchronizerParameterGetter {
  override def getLedgerTimeRecordTimeTolerance(synchronizerIdO: Option[PhysicalSynchronizerId])(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonNegativeFiniteDuration] =
    EitherT.pure[FutureUnlessShutdown, String](ledgerTimeRecordTimeTolerance)
}
