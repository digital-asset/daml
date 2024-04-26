// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.store.{PartyMetadata, PartyMetadataStore}
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class InMemoryPartyMetadataStore extends PartyMetadataStore {

  private val store = TrieMap[PartyId, PartyMetadata]()

  override def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store
      .put(
        partyId,
        PartyMetadata(partyId, displayName, participantId)(
          effectiveTimestamp = effectiveTimestamp,
          submissionId = submissionId,
        ),
      )
      .discard
    Future.unit

  }

  override def metadataForParty(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Future[Option[PartyMetadata]] =
    Future.successful(store.get(partyId))

  override def markNotified(
      metadata: PartyMetadata
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store.get(metadata.partyId) match {
      case Some(cur) if cur.effectiveTimestamp == metadata.effectiveTimestamp =>
        store
          .put(
            metadata.partyId,
            metadata.copy()(
              effectiveTimestamp = metadata.effectiveTimestamp,
              submissionId = metadata.submissionId,
              notified = true,
            ),
          )
          .discard
      case _ => ()
    }
    Future.unit
  }

  override def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]] =
    Future.successful(store.values.filterNot(_.notified).toSeq)

  override def close(): Unit = ()
}
