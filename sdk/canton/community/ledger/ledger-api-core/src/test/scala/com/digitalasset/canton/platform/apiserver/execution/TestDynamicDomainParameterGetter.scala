// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class TestDynamicDomainParameterGetter(
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
)(implicit
    ec: ExecutionContext
) extends DynamicDomainParameterGetter {
  override def getLedgerTimeRecordTimeTolerance(synchronizerIdO: Option[SynchronizerId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonNegativeFiniteDuration] =
    EitherT.pure[FutureUnlessShutdown, String](ledgerTimeRecordTimeTolerance)
}
