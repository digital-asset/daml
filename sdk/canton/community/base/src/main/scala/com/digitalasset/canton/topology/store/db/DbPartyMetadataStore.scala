// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String300}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.store.{PartyMetadata, PartyMetadataStore}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class DbPartyMetadataStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PartyMetadataStore
    with DbStore {

  import DbStorage.Implicits.BuilderChain.*
  import storage.api.*

  override def metadataForParty(
      partyId: PartyId
  )(implicit traceContext: TraceContext): Future[Option[PartyMetadata]] = {
    storage
      .query(
        metadataForPartyQuery(sql"party_id = $partyId #${storage.limit(1)}"),
        functionFullName,
      )
      .map(_.headOption)
  }

  private def metadataForPartyQuery(
      where: SQLActionBuilderChain
  ): DbAction.ReadOnly[Seq[PartyMetadata]] = {

    val query =
      sql"select party_id, display_name, participant_id, submission_id, effective_at, notified from common_party_metadata where " ++ where

    for {
      data <- query
        .as[(PartyId, Option[String], Option[String], String255, CantonTimestamp, Boolean)]
    } yield {
      data.map {
        case (partyId, displayNameS, participantIdS, submissionId, effectiveAt, notified) =>
          val participantId =
            participantIdS
              .flatMap(x => UniqueIdentifier.fromProtoPrimitive_(x).toOption)
              .map(ParticipantId(_))
          val displayName = displayNameS.flatMap(String255.create(_).toOption)
          PartyMetadata(
            partyId,
            displayName,
            participantId = participantId,
          )(
            effectiveTimestamp = effectiveAt,
            submissionId = submissionId,
            notified = notified,
          )
      }
    }
  }

  override def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val participantS = dbValue(participantId)
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into common_party_metadata (party_id, display_name, participant_id, submission_id, effective_at)
                    VALUES ($partyId, $displayName, $participantS, $submissionId, $effectiveTimestamp)
                 on conflict (party_id) do update
                  set
                    display_name = $displayName,
                    participant_id = $participantS,
                    submission_id = $submissionId,
                    effective_at = $effectiveTimestamp,
                    notified = false
                 """
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Oracle =>
        sqlu"""merge into common_party_metadata
                  using dual
                  on (party_id = $partyId)
                  when matched then
                    update set
                      display_name = $displayName,
                      participant_id = $participantS,
                      submission_id = $submissionId,
                      effective_at = $effectiveTimestamp,
                      notified = ${false}
                  when not matched then
                    insert (party_id, display_name, participant_id, submission_id, effective_at)
                    values ($partyId, $displayName, $participantS, $submissionId, $effectiveTimestamp)
                 """
    }
    storage.update_(query, functionFullName)
  }

  private def dbValue(participantId: Option[ParticipantId]): Option[String300] =
    participantId.map(_.uid.toLengthLimitedString.asString300)

  /** mark the given metadata has having been successfully forwarded to the domain */
  override def markNotified(
      metadata: PartyMetadata
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val partyId = metadata.partyId
    val effectiveAt = metadata.effectiveTimestamp
    val query =
      sqlu"UPDATE common_party_metadata SET notified = ${true} WHERE party_id = $partyId and effective_at = $effectiveAt"
    storage.update_(query, functionFullName)
  }

  /** fetch the current set of party data which still needs to be notified */
  override def fetchNotNotified()(implicit
      traceContext: TraceContext
  ): Future[Seq[PartyMetadata]] = {
    storage
      .query(
        metadataForPartyQuery(sql"notified = ${false}"),
        functionFullName,
      )
  }
}
