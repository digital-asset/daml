// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

// TODO(#15161): Rename file to InMemoryPartyMetadataStore
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

// TODO(#15161) collapse into InMemoryTopologyStoreX
trait InMemoryTopologyStoreCommon[+StoreId <: TopologyStoreId] extends NamedLogging {
  this: TopologyStoreX[StoreId] =>

  private val watermark = new AtomicReference[Option[CantonTimestamp]](None)

  @nowarn("cat=unused")
  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(watermark.get())

  override def updateDispatchingWatermark(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    watermark.getAndSet(Some(timestamp)) match {
      case Some(old) if old > timestamp =>
        logger.error(
          s"Topology dispatching watermark is running backwards! new=$timestamp, old=${old}"
        )
      case _ => ()
    }
    Future.unit
  }

}
