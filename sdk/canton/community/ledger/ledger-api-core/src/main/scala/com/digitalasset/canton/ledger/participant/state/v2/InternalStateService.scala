// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.v2

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference

trait InternalStateService {
  def activeContracts(
      partyIds: Set[LfPartyId],
      validAt: Option[Offset],
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed]
}

trait InternalStateServiceProvider {
  def internalStateService: Option[InternalStateService]
  def registerInternalStateService(internalStateService: InternalStateService): Unit
  def unregisterInternalStateService(): Unit
}

trait InternalStateServiceProviderImpl extends InternalStateServiceProvider {
  private val internalStateServiceRef: AtomicReference[Option[InternalStateService]] =
    new AtomicReference(None)

  override def internalStateService: Option[InternalStateService] =
    internalStateServiceRef.get()

  override def registerInternalStateService(internalStateService: InternalStateService): Unit =
    internalStateServiceRef.set(Some(internalStateService))

  override def unregisterInternalStateService(): Unit =
    internalStateServiceRef.set(None)
}
