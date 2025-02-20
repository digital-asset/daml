// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

/** Class for retrieving dynamic synchronizer parameters.
  *
  * Because of the current organisation of code between the ledger API and Canton, the ledger API
  * code does not have direct access to Canton concepts such as dynamic synchronizer parameters (it
  * only sees the `CantonSyncService` as an instance of
  * [[com.digitalasset.canton.ledger.participant.state.SyncService]]).
  *
  * An instance of this trait is therefore provided as a "hook" to the ledger API to retrieve
  * dynamic synchronizer parameters.
  */
trait DynamicSynchronizerParameterGetter {
  def getLedgerTimeRecordTimeTolerance(synchronizerIdO: Option[SynchronizerId])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonNegativeFiniteDuration]
}
