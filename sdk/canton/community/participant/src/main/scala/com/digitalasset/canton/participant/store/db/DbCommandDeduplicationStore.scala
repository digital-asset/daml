// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.OptionT
import cats.syntax.option.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.v2.ChangeId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.submission.ChangeIdHash
import com.digitalasset.canton.participant.store.CommandDeduplicationStore.OffsetAndPublicationTime
import com.digitalasset.canton.participant.store.{
  CommandDeduplicationData,
  CommandDeduplicationStore,
  DefiniteAnswerEvent,
}
import com.digitalasset.canton.protocol.StoredParties
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbSerializationException
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.digitalasset.canton.{ApplicationId, CommandId}
import slick.jdbc.SetParameter

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class DbCommandDeduplicationStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends CommandDeduplicationStore
    with DbStore {
  import storage.api.*
  import storage.converters.*

  private val cachedLastPruning =
    new AtomicReference[Option[Option[OffsetAndPublicationTime]]](None)

  private implicit val setParameterStoredParties: SetParameter[StoredParties] =
    StoredParties.getVersionedSetParameter(releaseProtocolVersion.v)
  private implicit val setParameterTraceContext: SetParameter[SerializableTraceContext] =
    SerializableTraceContext.getVersionedSetParameter(releaseProtocolVersion.v)
  private implicit val setParameterTraceContextO: SetParameter[Option[SerializableTraceContext]] =
    SerializableTraceContext.getVersionedSetParameterO(releaseProtocolVersion.v)

  override def lookup(
      changeIdHash: ChangeIdHash
  )(implicit traceContext: TraceContext): OptionT[Future, CommandDeduplicationData] = {
    val query =
      sql"""
        select application_id, command_id, act_as,
          offset_definite_answer, publication_time_definite_answer, submission_id_definite_answer, trace_context_definite_answer,
          offset_acceptance, publication_time_acceptance, submission_id_acceptance, trace_context_acceptance
        from par_command_deduplication
        where change_id_hash = $changeIdHash
        """.as[CommandDeduplicationData].headOption
    storage.querySingle(query, functionFullName)
  }

  override def storeDefiniteAnswers(
      answers: Seq[(ChangeId, DefiniteAnswerEvent, Boolean)]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val update = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        """
          insert into par_command_deduplication(
            change_id_hash,
            application_id, command_id, act_as,
            offset_definite_answer, publication_time_definite_answer, submission_id_definite_answer, trace_context_definite_answer,
            offset_acceptance, publication_time_acceptance, submission_id_acceptance, trace_context_acceptance
          )
          values (
            ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?
          )
          on conflict (change_id_hash) do
            update set
              offset_definite_answer = excluded.offset_definite_answer,
              publication_time_definite_answer = excluded.publication_time_definite_answer,
              submission_id_definite_answer = excluded.submission_id_definite_answer,
              trace_context_definite_answer = excluded.trace_context_definite_answer,
              offset_acceptance = (case when ? = '1' then excluded.offset_acceptance else par_command_deduplication.offset_acceptance end),
              publication_time_acceptance = (case when ? = '1' then excluded.publication_time_acceptance else par_command_deduplication.publication_time_acceptance end),
              submission_id_acceptance = (case when ? = '1' then excluded.submission_id_acceptance else par_command_deduplication.submission_id_acceptance end),
              trace_context_acceptance = (case when ? = '1' then excluded.trace_context_acceptance else par_command_deduplication.trace_context_acceptance end)
            where par_command_deduplication.offset_definite_answer < excluded.offset_definite_answer
            """
      case _: DbStorage.Profile.Oracle =>
        """
          merge into par_command_deduplication using
             (select
                ? as change_id_hash,
                ? as application_id,
                ? as command_id,
                ? as act_as,
                ? as offset_definite_answer,
                ? as publication_time_definite_answer,
                ? as submission_id_definite_answer,
                ? as trace_context_definite_answer,
                ? as offset_acceptance,
                ? as publication_time_acceptance,
                cast(? as nvarchar2(300)) as submission_id_acceptance,
                to_blob(?) as trace_context_acceptance
              from dual) excluded
            on (par_command_deduplication.change_id_hash = excluded.change_id_hash)
            when matched then
              update set
                offset_definite_answer = excluded.offset_definite_answer,
                publication_time_definite_answer = excluded.publication_time_definite_answer,
                submission_id_definite_answer = excluded.submission_id_definite_answer,
                trace_context_definite_answer = excluded.trace_context_definite_answer,
                offset_acceptance = (case when ? = '1' then excluded.offset_acceptance else par_command_deduplication.offset_acceptance end),
                publication_time_acceptance = (case when ? = '1' then excluded.publication_time_acceptance else par_command_deduplication.publication_time_acceptance end),
                submission_id_acceptance = (case when ? = '1' then excluded.submission_id_acceptance else par_command_deduplication.submission_id_acceptance end),
                trace_context_acceptance = (case when ? = '1' then excluded.trace_context_acceptance else par_command_deduplication.trace_context_acceptance end)
              where par_command_deduplication.offset_definite_answer < excluded.offset_definite_answer
            when not matched then
              insert (
                change_id_hash,
                application_id, command_id, act_as,
                offset_definite_answer, publication_time_definite_answer,
                submission_id_definite_answer, trace_context_definite_answer,
                offset_acceptance, publication_time_acceptance,
                submission_id_acceptance, trace_context_acceptance
              )
              values (
                excluded.change_id_hash,
                excluded.application_id, excluded.command_id, excluded.act_as,
                excluded.offset_definite_answer, excluded.publication_time_definite_answer,
                excluded.submission_id_definite_answer, excluded.trace_context_definite_answer,
                excluded.offset_acceptance, excluded.publication_time_acceptance,
                excluded.submission_id_acceptance, excluded.trace_context_acceptance
              )
            """
      case _: DbStorage.Profile.H2 =>
        """
          merge into par_command_deduplication using
             (select
                cast(? as varchar(300)) as change_id_hash,
                cast(? as varchar(300)) as application_id,
                cast(? as varchar(300)) as command_id,
                cast(? as bytea) as act_as,
                cast(? as bigint) as offset_definite_answer,
                cast(? as bigint) as publication_time_definite_answer,
                cast(? as varchar(300)) as submission_id_definite_answer,
                cast(? as bytea) as trace_context_definite_answer,
                cast(? as bigint) as offset_acceptance,
                cast(? as bigint) as publication_time_acceptance,
                cast(? as varchar(300)) as submission_id_acceptance,
                cast(? as bytea) as trace_context_acceptance
              from dual) as excluded
            on (par_command_deduplication.change_id_hash = excluded.change_id_hash)
            when matched and par_command_deduplication.offset_definite_answer < excluded.offset_definite_answer then
              update set
                offset_definite_answer = excluded.offset_definite_answer,
                publication_time_definite_answer = excluded.publication_time_definite_answer,
                submission_id_definite_answer = excluded.submission_id_definite_answer,
                trace_context_definite_answer = excluded.trace_context_definite_answer,
                offset_acceptance = (case when ? = '1' then excluded.offset_acceptance else par_command_deduplication.offset_acceptance end),
                publication_time_acceptance = (case when ? = '1' then excluded.publication_time_acceptance else par_command_deduplication.publication_time_acceptance end),
                submission_id_acceptance = (case when ? = '1' then excluded.submission_id_acceptance else par_command_deduplication.submission_id_acceptance end),
                trace_context_acceptance = (case when ? = '1' then excluded.trace_context_acceptance else par_command_deduplication.trace_context_acceptance end)
            when not matched then
              insert (
                change_id_hash,
                application_id, command_id, act_as,
                offset_definite_answer, publication_time_definite_answer,
                submission_id_definite_answer, trace_context_definite_answer,
                offset_acceptance, publication_time_acceptance,
                submission_id_acceptance, trace_context_acceptance
              )
              values (
                excluded.change_id_hash,
                excluded.application_id, excluded.command_id, excluded.act_as,
                excluded.offset_definite_answer, excluded.publication_time_definite_answer,
                excluded.submission_id_definite_answer, excluded.trace_context_definite_answer,
                excluded.offset_acceptance, excluded.publication_time_acceptance,
                excluded.submission_id_acceptance, excluded.trace_context_acceptance
              )
            """
    }
    val bulkUpdate =
      DbStorage.bulkOperation(
        update,
        answers,
        storage.profile,
      ) { pp => update =>
        val (changeId, definiteAnswerEvent, accepted) = update
        val acceptance = if (accepted) definiteAnswerEvent.some else None
        pp >> ChangeIdHash(changeId)
        pp >> ApplicationId(changeId.applicationId)
        pp >> CommandId(changeId.commandId)
        pp >> StoredParties.fromIterable(changeId.actAs)
        pp >> definiteAnswerEvent.offset
        pp >> definiteAnswerEvent.publicationTime
        pp >> definiteAnswerEvent.serializableSubmissionId
        pp >> SerializableTraceContext(definiteAnswerEvent.traceContext)
        pp >> acceptance.map(_.offset)
        pp >> acceptance.map(_.publicationTime)
        pp >> acceptance.flatMap(_.serializableSubmissionId)
        pp >> acceptance.map(accept => SerializableTraceContext(accept.traceContext))

        @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
        def setAcceptFlag(): Unit = {
          val acceptedFlag = if (accepted) "1" else "0"
          pp >> acceptedFlag
          pp >> acceptedFlag
          pp >> acceptedFlag
          pp >> acceptedFlag
        }
        setAcceptFlag()
      }
    // No need for synchronous commit across DB replicas, because this method is driven from the
    // published events in the multi-domain event log, which itself uses synchronous commits and
    // therefore ensures synchronization. After a crash, crash recovery will sync the
    // command deduplication data with the MultiDomainEventLog.
    storage.queryAndUpdate(bulkUpdate, functionFullName).flatMap { rowCounts =>
      MonadUtil.sequentialTraverse_(rowCounts.iterator.zip(answers.tails)) {
        case (rowCount, currentAndLaterAnswers) =>
          val (changeId, definiteAnswerEvent, accepted) =
            currentAndLaterAnswers.headOption.getOrElse(
              throw new RuntimeException(
                s"Received a row count $rowCount for an non-existent definite answer"
              )
            )
          val laterAnswers = currentAndLaterAnswers.drop(1)
          checkRowCount(rowCount, changeId, definiteAnswerEvent, accepted, laterAnswers)
      }
    }
  }

  private def checkRowCount(
      rowCount: Int,
      changeId: ChangeId,
      definiteAnswerEvent: DefiniteAnswerEvent,
      accepted: Boolean,
      laterAnswersInBatch: Seq[(ChangeId, DefiniteAnswerEvent, Boolean)],
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = definiteAnswerEvent.traceContext
    if (rowCount == 1) {
      val acceptance = if (accepted) definiteAnswerEvent.some else None
      val data = CommandDeduplicationData.tryCreate(changeId, definiteAnswerEvent, acceptance)
      logger.debug(s"Updated command deduplication data for ${changeId.hash} to $data")
      Future.unit
    } else if (rowCount == 0) {
      val changeIdHash = ChangeIdHash(changeId)
      // Check what's in the DB
      lookup(changeIdHash).fold[Unit](
        ErrorUtil.internalError(
          new DbSerializationException(s"Failed to update command deduplication for $changeIdHash")
        )
      ) { data =>
        if (
          data.latestDefiniteAnswer == definiteAnswerEvent && (!accepted || data.latestAcceptance == definiteAnswerEvent.some)
        ) {
          if (data.latestDefiniteAnswer.traceContext == definiteAnswerEvent.traceContext) {
            logger.debug(
              s"Looked and found expected command deduplication data for ${changeId.hash}."
            )
          } else {
            // this happened after 2 years for the first time with a client, but we don't know why
            // adding therefore this warning so maybe we can get at some point a flake to reproduce
            // and tackle it.
            logger.warn(
              s"Looked and found expected command deduplication data for ${changeId.hash} but with different trace contexts ${data.latestDefiniteAnswer.traceContext} vs ${definiteAnswerEvent.traceContext}. Not a problem but unexpected."
            )
          }
        } else {
          def error(): Unit = {
            ErrorUtil.internalError(
              new IllegalArgumentException(
                s"Cannot update command deduplication data for $changeIdHash from offset ${data.latestDefiniteAnswer.offset} to offset ${definiteAnswerEvent.offset}\n Found data: $data\nDefinite answer update: $definiteAnswerEvent"
              )
            )
          }

          def laterOverwrite(): Unit = {
            logger.debug(
              s"Command deduplication data for ${changeId.hash} is being overwritten by later completion."
            )
          }

          /* If the bulk insertion query is retried, we may find a later definite answer than the current one.
           * So we check for this possibility before we complain.
           */
          val laterDefiniteAnswerOverwrites = laterAnswersInBatch.exists {
            case (otherChangeId, _answer, _accepted) => changeId == otherChangeId
          }
          if (laterDefiniteAnswerOverwrites) {
            if (accepted) {
              val laterAcceptanceOverwrites = laterAnswersInBatch.exists {
                case (otherChangeId, _answer, otherAccepted) =>
                  changeId == otherChangeId && otherAccepted
              }
              if (laterAcceptanceOverwrites) laterOverwrite()
              else if (data.latestAcceptance == definiteAnswerEvent.some) {
                logger.debug(
                  s"Looked and found expected command deduplication acceptance for ${changeId.hash}"
                )
              } else error()
            } else laterOverwrite()
          } else error()
        }
      }
    } else {
      ErrorUtil.internalErrorAsync(
        new DbSerializationException(
          s"Updating the command deduplication for ${changeId.hash} updated $rowCount rows"
        )
      )
    }
  }

  override def prune(upToInclusive: GlobalOffset, prunedPublicationTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    cachedLastPruning.updateAndGet {
      case Some(Some(OffsetAndPublicationTime(currentOffset, currentPublicationTime))) =>
        Some(
          Some(
            OffsetAndPublicationTime(
              currentOffset.max(upToInclusive),
              currentPublicationTime.max(prunedPublicationTime),
            )
          )
        )
      case _ =>
        Some(Some(OffsetAndPublicationTime(upToInclusive, prunedPublicationTime)))
    }

    {
      val updatePruneOffset = storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          sqlu"""
          insert into par_command_deduplication_pruning(client, pruning_offset, publication_time)
          values (0, $upToInclusive, $prunedPublicationTime)
          on conflict (client) do
            update set
              pruning_offset = (case when par_command_deduplication_pruning.pruning_offset < excluded.pruning_offset then excluded.pruning_offset else par_command_deduplication_pruning.pruning_offset end),
              publication_time = (case when par_command_deduplication_pruning.publication_time < excluded.publication_time then excluded.publication_time else par_command_deduplication_pruning.publication_time end)
            """
        case _: DbStorage.Profile.Oracle | _: DbStorage.Profile.H2 =>
          sqlu"""
            merge into par_command_deduplication_pruning using dual
              on (client = 0)
              when matched then
                update set
                  pruning_offset = (case when par_command_deduplication_pruning.pruning_offset < $upToInclusive then $upToInclusive else par_command_deduplication_pruning.pruning_offset end),
                  publication_time = (case when par_command_deduplication_pruning.publication_time < $prunedPublicationTime then $prunedPublicationTime else par_command_deduplication_pruning.publication_time end)
              when not matched then
                insert (client, pruning_offset, publication_time)
                values (0, $upToInclusive, $prunedPublicationTime)
            """
      }
      val doPrune =
        sqlu"""delete from par_command_deduplication where offset_definite_answer <= $upToInclusive"""
      storage.update_(updatePruneOffset.andThen(doPrune), functionFullName)
    }
  }

  override def latestPruning()(implicit
      traceContext: TraceContext
  ): OptionT[Future, CommandDeduplicationStore.OffsetAndPublicationTime] = {
    cachedLastPruning.get() match {
      case None =>
        {
          val query =
            sql"""
          select pruning_offset, publication_time
          from par_command_deduplication_pruning
             """.as[OffsetAndPublicationTime].headOption
          storage.querySingle(query, functionFullName)
        }
          .transform { offset =>
            // only replace if we haven't raced with another thread
            cachedLastPruning.compareAndSet(None, Some(offset))
            offset
          }
      case Some(value) => OptionT.fromOption[Future](value)
    }
  }
}
