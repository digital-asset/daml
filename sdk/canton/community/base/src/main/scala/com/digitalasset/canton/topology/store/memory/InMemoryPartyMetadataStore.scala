// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.store.{PartyMetadata, PartyMetadataStore}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap

class InMemoryPartyMetadataStore extends PartyMetadataStore {

  private val store = TrieMap[PartyId, PartyMetadata]()

  override def insertOrUpdatePartyMetadata(
      partiesMetadata: Seq[PartyMetadata]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    partiesMetadata.foreach(metadata => store.put(metadata.partyId, metadata).discard)
    FutureUnlessShutdown.unit
  }

  override def metadataForParties(partyIds: Seq[PartyId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[Option[PartyMetadata]]] =
    FutureUnlessShutdown.pure(partyIds.map(store.get))

  override def markNotified(
      effectiveAt: CantonTimestamp,
      partyIds: Seq[PartyId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    partyIds.foreach { partyId =>
      store.get(partyId) match {
        case Some(cur) if cur.effectiveTimestamp == effectiveAt =>
          store
            .put(
              partyId,
              cur.copy()(
                effectiveTimestamp = cur.effectiveTimestamp,
                submissionId = cur.submissionId,
                notified = true,
              ),
            )
            .discard
        case _ => ()
      }
    }
    FutureUnlessShutdown.unit
  }

  override def fetchNotNotified()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[PartyMetadata]] =
    FutureUnlessShutdown.pure(store.values.filterNot(_.notified).toSeq)

  override def close(): Unit = ()
}
