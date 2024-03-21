// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class TestDynamicDomainParameterGetter(
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration
)(implicit
    ec: ExecutionContext
) extends DynamicDomainParameterGetter {
  override def getLedgerTimeRecordTimeTolerance(domainIdO: Option[DomainId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, NonNegativeFiniteDuration] =
    EitherT.pure[Future, String](ledgerTimeRecordTimeTolerance)
}
