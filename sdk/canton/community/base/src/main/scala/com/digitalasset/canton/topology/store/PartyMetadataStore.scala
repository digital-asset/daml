// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.db.DbPartyMetadataStore
import com.digitalasset.canton.topology.store.memory.InMemoryPartyMetadataStore
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

/** the party metadata used to inform the ledger api server
  *
  * the first class parameters correspond to the relevant information, whereas the
  * second class parameters are synchronisation information used during crash recovery.
  * we don't want these in an equality comparison.
  */
final case class PartyMetadata(
    partyId: PartyId,
    displayName: Option[DisplayName],
    participantId: Option[ParticipantId],
)(
    val effectiveTimestamp: CantonTimestamp,
    val submissionId: String255,
    val notified: Boolean = false,
)

trait PartyMetadataStore extends AutoCloseable {

  def metadataForParty(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Future[Option[PartyMetadata]]

  final def insertOrUpdatePartyMetadata(metadata: PartyMetadata)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    insertOrUpdatePartyMetadata(
      partyId = metadata.partyId,
      participantId = metadata.participantId,
      displayName = metadata.displayName,
      effectiveTimestamp = metadata.effectiveTimestamp,
      submissionId = metadata.submissionId,
    )
  }

  def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** mark the given metadata as having been successfully forwarded to the domain */
  def markNotified(metadata: PartyMetadata)(implicit traceContext: TraceContext): Future[Unit]

  /** fetch the current set of party data which still needs to be notified */
  def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]]

}

object PartyMetadataStore {

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): PartyMetadataStore =
    storage match {
      case _: MemoryStorage => new InMemoryPartyMetadataStore()
      case jdbc: DbStorage => new DbPartyMetadataStore(jdbc, timeouts, loggerFactory)
    }

}
