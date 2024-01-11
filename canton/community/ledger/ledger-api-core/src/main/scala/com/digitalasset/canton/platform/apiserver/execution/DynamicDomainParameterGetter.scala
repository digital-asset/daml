// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

/** Class for retrieving dynamic domain parameters.
  *
  * Because of the current organisation of code between the ledger API and Canton, the ledger API
  * code does not have direct access to Canton concepts such as dynamic domain parameters (it only sees
  * the `CantonSyncService` as an instance of a [[com.digitalasset.canton.ledger.participant.state.v2.ReadService]]
  * and [[com.digitalasset.canton.ledger.participant.state.v2.WriteService]]).
  *
  * An instance of this trait is therefore provided as a "hook" to the ledger API to retrieve dynamic domain parameters.
  */
trait DynamicDomainParameterGetter {
  def getLedgerTimeRecordTimeTolerance(domainIdO: Option[DomainId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, NonNegativeFiniteDuration]
}
