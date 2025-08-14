// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicReference

trait InternalIndexService {
  def activeContracts(
      partyIds: Set[LfPartyId],
      validAt: Option[Offset],
  )(implicit traceContext: TraceContext): Source[GetActiveContractsResponse, NotUsed]

  def topologyTransactions(
      partyId: LfPartyId,
      fromExclusive: Offset,
  )(implicit traceContext: TraceContext): Source[TopologyTransaction, NotUsed]
}

trait InternalIndexServiceProvider {
  def internalIndexService: Option[InternalIndexService]
  def registerInternalIndexService(internalIndexService: InternalIndexService): Unit
  def unregisterInternalIndexService(): Unit
}

trait InternalIndexServiceProviderImpl extends InternalIndexServiceProvider {
  private val internalIndexServiceRef: AtomicReference[Option[InternalIndexService]] =
    new AtomicReference(None)

  override def internalIndexService: Option[InternalIndexService] =
    internalIndexServiceRef.get()

  override def registerInternalIndexService(internalIndexService: InternalIndexService): Unit =
    internalIndexServiceRef.set(Some(internalIndexService))

  override def unregisterInternalIndexService(): Unit =
    internalIndexServiceRef.set(None)
}
