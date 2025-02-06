// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.store.{PartyMetadata, PartyMetadataStore}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class InMemoryPartyMetadataStore extends PartyMetadataStore {

  private val store = TrieMap[PartyId, PartyMetadata]()

  override def insertOrUpdatePartyMetadata(
      partiesMetadata: Seq[PartyMetadata]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    partiesMetadata.foreach(metadata => store.put(metadata.partyId, metadata).discard)
    Future.unit
  }

  override def metadataForParties(partyIds: Seq[PartyId])(implicit
      traceContext: TraceContext
  ): Future[Seq[Option[PartyMetadata]]] =
    Future.successful(partyIds.map(store.get))

  override def markNotified(
      effectiveAt: CantonTimestamp,
      partyIds: Seq[PartyId],
  )(implicit traceContext: TraceContext): Future[Unit] = {
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
    Future.unit
  }

  override def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]] =
    Future.successful(store.values.filterNot(_.notified).toSeq)

  override def close(): Unit = ()
}
