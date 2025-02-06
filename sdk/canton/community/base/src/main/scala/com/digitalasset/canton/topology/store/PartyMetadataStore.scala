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

/** Store to manage batches of party metadata prior to indexing parties for the ledger API
  */
trait PartyMetadataStore extends AutoCloseable {

  /** Fetch the metadata for the given party IDs. The order of the response corresponds
    * to the input order. None is returned on behalf of currently unknown parties.
    */
  def metadataForParties(partyIds: Seq[PartyId])(implicit
      traceContext: TraceContext
  ): Future[Seq[Option[PartyMetadata]]]

  /** Reflect the specified batch of party metadata in the store. */
  def insertOrUpdatePartyMetadata(partiesMetadata: Seq[PartyMetadata])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Mark the given parties as having been successfully forwarded to the ledger API server
    * as of the specified effectiveAt timestamp.
    */
  def markNotified(effectiveAt: CantonTimestamp, partyIds: Seq[PartyId])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Fetch the current set of party metadata that still needs to be notified. */
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
