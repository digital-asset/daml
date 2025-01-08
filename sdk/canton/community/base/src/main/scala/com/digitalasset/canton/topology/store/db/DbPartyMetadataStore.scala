// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
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

  override def metadataForParties(
      partyIds: Seq[PartyId]
  )(implicit traceContext: TraceContext): Future[Seq[Option[PartyMetadata]]] =
    NonEmpty
      .from(partyIds)
      .fold(Future.successful(Seq.empty[Option[PartyMetadata]]))(nonEmptyPartyIds =>
        for {
          storedParties <- storage
            .query(
              metadataForPartyQuery(DbStorage.toInClause("party_id", nonEmptyPartyIds)),
              functionFullName,
            )
            .map(_.map(metadata => metadata.partyId -> metadata).toMap)
        } yield partyIds.map(storedParties.get)
      )

  private def metadataForPartyQuery(
      where: SQLActionBuilderChain
  ): DbAction.ReadOnly[Seq[PartyMetadata]] = {

    val query =
      sql"select party_id, participant_id, submission_id, effective_at, notified from common_party_metadata where " ++ where

    for {
      data <- query
        .as[(PartyId, Option[String], String255, CantonTimestamp, Boolean)]
    } yield {
      data.map { case (partyId, participantIdS, submissionId, effectiveAt, notified) =>
        val participantId =
          participantIdS
            .flatMap(x => UniqueIdentifier.fromProtoPrimitive_(x).toOption)
            .map(ParticipantId(_))
        PartyMetadata(
          partyId,
          participantId = participantId,
        )(
          effectiveTimestamp = effectiveAt,
          submissionId = submissionId,
          notified = notified,
        )
      }
    }
  }

  def insertOrUpdatePartyMetadata(
      partiesMetadata: Seq[PartyMetadata]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        val insertQuery =
          """insert into common_party_metadata (party_id, participant_id, submission_id, effective_at)
                    VALUES (?, ?, ?, ?)
                 on conflict (party_id) do update
                  set
                    participant_id = ?,
                    submission_id = ?,
                    effective_at = ?,
                    notified = false
                 """
        DbStorage
          .bulkOperation_(insertQuery, partiesMetadata, storage.profile) { pp => metadata =>
            val participantS = dbValue(metadata.participantId)
            pp >> metadata.partyId
            pp >> participantS
            pp >> metadata.submissionId
            pp >> metadata.effectiveTimestamp
            pp >> participantS
            pp >> metadata.submissionId
            pp >> metadata.effectiveTimestamp
          }
          .transactionally
      case _: DbStorage.Profile.H2 =>
        val mergeQuery =
          """merge into common_party_metadata
                  using dual
                  on (party_id = ?)
                  when matched then
                    update set
                      participant_id = ?,
                      submission_id = ?,
                      effective_at = ?,
                      notified = ?
                  when not matched then
                    insert (party_id, participant_id, submission_id, effective_at)
                    values (?, ?, ?, ?)
                 """
        DbStorage
          .bulkOperation_(mergeQuery, partiesMetadata, storage.profile) { pp => metadata =>
            val participantS = dbValue(metadata.participantId)
            pp >> metadata.partyId
            pp >> participantS
            pp >> metadata.submissionId
            pp >> metadata.effectiveTimestamp
            pp >> false
            pp >> metadata.partyId
            pp >> participantS
            pp >> metadata.submissionId
            pp >> metadata.effectiveTimestamp
          }
          .transactionally
    }
    storage.queryAndUpdate(query, functionFullName)
  }

  private def dbValue(participantId: Option[ParticipantId]): Option[String300] =
    participantId.map(_.uid.toLengthLimitedString.asString300)

  /** mark the given metadata has having been successfully forwarded to the synchronizer */
  override def markNotified(
      effectiveAt: CantonTimestamp,
      partyIds: Seq[PartyId],
  )(implicit traceContext: TraceContext): Future[Unit] =
    NonEmpty.from(partyIds).fold(Future.unit) { nonEmptyPartyIds =>
      val query =
        (sql"UPDATE common_party_metadata SET notified = ${true} WHERE effective_at = $effectiveAt and " ++ DbStorage
          .toInClause("party_id", nonEmptyPartyIds)).asUpdate
      storage.update_(query, functionFullName)
    }

  /** fetch the current set of party data which still needs to be notified */
  override def fetchNotNotified()(implicit
      traceContext: TraceContext
  ): Future[Seq[PartyMetadata]] =
    storage
      .query(
        metadataForPartyQuery(sql"notified = ${false}"),
        functionFullName,
      )
}
