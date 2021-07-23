// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.util.UUID
import java.util.concurrent.Executors

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.concurrent.{ExecutionContext, Future}
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
import com.daml.ledger.on.sql.queries.Queries
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.`export`.LedgerDataExporter
import com.daml.ledger.participant.state.kvutils.api.{
  CommitMetadata,
  LedgerReader,
  LedgerRecord,
  LedgerWriter,
}
import com.daml.ledger.participant.state.kvutils.{KeyValueCommitting, OffsetBuilder, Raw}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.validator._
import com.daml.ledger.validator.caching.{CachingStateReader, ImmutablesOnlyCacheUpdatePolicy}
import com.daml.ledger.validator.preexecution._
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.common.MismatchException
import com.daml.telemetry.TelemetryContext

import scala.util.{Failure, Success}
import scala.{concurrent => sc}

final class SqlLedgerReaderWriter(
    override val ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: Ref.ParticipantId,
    metrics: Metrics,
    database: Database,
    dispatcher: Dispatcher[Index],
    committer: Committer,
    stateAccess: SqlLedgerStateAccess,
    timeProvider: TimeProvider,
    committerExecutionContext: sc.ExecutionContext,
) extends LedgerWriter
    with LedgerReader {

  override def currentHealth(): HealthStatus = Healthy

  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        OffsetBuilder.highestIndex(startExclusive.getOrElse(StartOffset)),
        RangeSource((startExclusive, endInclusive) =>
          Source
            .future(
              Timed
                .value(
                  metrics.daml.ledger.log.read,
                  database.inReadTransaction("read_log") { queries =>
                    Future.fromTry(queries.selectFromLog(startExclusive, endInclusive))
                  },
                )
                .removeExecutionContext
            )
            .mapConcat(identity)
            .mapMaterializedValue(_ => NotUsed)
        ),
      )
      .map { case (_, entry) => entry }

  override def commit(
      correlationId: String,
      envelope: Raw.Envelope,
      metadata: CommitMetadata,
  )(implicit telemetryContext: TelemetryContext): sc.Future[SubmissionResult] =
    committer
      .commit(
        participantId,
        correlationId,
        envelope,
        exportRecordTime = timeProvider.getCurrentTime,
        ledgerStateAccess = stateAccess,
      )(committerExecutionContext)
      .andThen { case Success((SubmissionResult.Acknowledged, writtenIndex)) =>
        dispatcher.signalNewHead(writtenIndex)
      }(committerExecutionContext)
      .map(_._1)(committerExecutionContext)
}

object SqlLedgerReaderWriter {
  private[sql] type Committer = PreExecutingValidatingCommitter[
    Option[DamlStateValue],
    RawPreExecutingCommitStrategy.ReadSet,
    RawKeyValuePairsWithLogEntry,
  ]

  private[sql] type StateValueCache = Cache[DamlStateKey, DamlStateValue]

  private val StartOffset: Offset = OffsetBuilder.fromLong(StartIndex)

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  final class Owner(
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      metrics: Metrics,
      engine: Engine,
      jdbcUrl: String,
      resetOnStartup: Boolean,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: StateValueCache = Cache.none,
  )(implicit loggingContext: LoggingContext)
      extends ResourceOwner[SqlLedgerReaderWriter] {
    override def acquire()(implicit context: ResourceContext): Resource[SqlLedgerReaderWriter] = {
      implicit val migratorExecutionContext: ExecutionContext[Database.Migrator] =
        ExecutionContext(context.executionContext)
      for {
        uninitializedDatabase <- Database.owner(jdbcUrl, metrics).acquire()
        database <- Resource.fromFuture(
          if (resetOnStartup) uninitializedDatabase.migrateAndReset().removeExecutionContext
          else sc.Future.successful(uninitializedDatabase.migrate())
        )
        ledgerId <- Resource.fromFuture(
          updateOrRetrieveLedgerId(ledgerId, database).removeExecutionContext
        )
        dispatcher <- new DispatcherOwner(database).acquire()
        sqlLedgerStateAccess = new SqlLedgerStateAccess(database, metrics)
        keyValueCommitting = new KeyValueCommitting(
          engine,
          metrics,
          inStaticTimeMode = timeProvider != TimeProvider.UTC,
        )
        keySerializationStrategy = StateKeySerializationStrategy.createDefault()
        ledgerDataExporter <- LedgerDataExporter.Owner.acquire()
        committer = new Committer(
          transformStateReader = ledgerStateReader =>
            CachingStateReader(
              stateValueCache,
              ImmutablesOnlyCacheUpdatePolicy,
              DamlLedgerStateReader
                .from(ledgerStateReader, keySerializationStrategy),
            ),
          validator = new PreExecutingSubmissionValidator(
            keyValueCommitting,
            new RawPreExecutingCommitStrategy(keySerializationStrategy),
            metrics = metrics,
          ),
          postExecutionConflictDetector = new EqualityBasedPostExecutionConflictDetector,
          postExecutionWriteSetSelector =
            new TimeBasedWriteSetSelector(() => timeProvider.getCurrentTime),
          postExecutionWriter = new RawPostExecutionWriter,
          ledgerDataExporter = ledgerDataExporter,
        )
        committerExecutionContext <- ResourceOwner
          .forExecutorService(() =>
            sc.ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())
          )
          .acquire()
      } yield new SqlLedgerReaderWriter(
        ledgerId,
        participantId,
        metrics,
        database,
        dispatcher,
        committer,
        sqlLedgerStateAccess,
        timeProvider,
        committerExecutionContext,
      )
    }
  }

  private def updateOrRetrieveLedgerId(
      providedLedgerId: LedgerId,
      database: Database,
  ): Future[Database.Writer, LedgerId] =
    database.inWriteTransaction("retrieve_ledger_id") { queries =>
      Future.fromTry(
        queries
          .updateOrRetrieveLedgerId(providedLedgerId)
          .flatMap { ledgerId =>
            if (providedLedgerId != ledgerId) {
              Failure(
                new MismatchException.LedgerId(
                  domain.LedgerId(ledgerId),
                  domain.LedgerId(providedLedgerId),
                )
              )
            } else {
              Success(ledgerId)
            }
          }
      )
    }

  private final class DispatcherOwner(database: Database) extends ResourceOwner[Dispatcher[Index]] {
    override def acquire()(implicit context: ResourceContext): Resource[Dispatcher[Index]] =
      for {
        head <- Resource.fromFuture(
          database
            .inReadTransaction("read_head") { queries =>
              Future.fromTry(
                queries.selectLatestLogEntryId().map(_.map(_ + 1).getOrElse(StartIndex))
              )
            }
            .removeExecutionContext
        )
        dispatcher <- Dispatcher
          .owner(
            name = "sql-participant-state",
            zeroIndex = StartIndex,
            headAtInitialization = head,
          )
          .acquire()
      } yield dispatcher
  }

  private final class SqlLedgerStateAccess(database: Database, metrics: Metrics)
      extends LedgerStateAccess[Index] {
    override def inTransaction[T](body: LedgerStateOperations[Index] => sc.Future[T])(implicit
        executionContext: sc.ExecutionContext
    ): sc.Future[T] =
      database
        .inWriteTransaction("commit") { queries =>
          body(new TimedLedgerStateOperations(new SqlLedgerStateOperations(queries), metrics))
        }
        .removeExecutionContext
  }

  private final class SqlLedgerStateOperations(queries: Queries)
      extends BatchingLedgerStateOperations[Index] {
    override def readState(
        keys: Iterable[Raw.StateKey]
    )(implicit executionContext: sc.ExecutionContext): sc.Future[Seq[Option[Raw.Envelope]]] =
      Future.fromTry(queries.selectStateValuesByKeys(keys)).removeExecutionContext

    override def writeState(
        keyValuePairs: Iterable[Raw.StateEntry]
    )(implicit executionContext: sc.ExecutionContext): sc.Future[Unit] =
      Future.fromTry(queries.updateState(keyValuePairs)).removeExecutionContext

    override def appendToLog(
        key: Raw.LogEntryId,
        value: Raw.Envelope,
    )(implicit executionContext: sc.ExecutionContext): sc.Future[Index] =
      Future.fromTry(queries.insertRecordIntoLog(key, value)).removeExecutionContext
  }

}
