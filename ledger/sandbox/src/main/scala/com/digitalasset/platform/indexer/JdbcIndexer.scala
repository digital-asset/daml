// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink}
import akka.{Done, NotUsed}
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Ref.LedgerString.ordering
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.dec.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.EventIdFormatter
import com.digitalasset.platform.sandbox.metrics.timedFuture
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlLedger.defaultNumberOfShortLivedConnections
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{
  DbType,
  JdbcLedgerDao,
  LedgerDao,
  PersistenceEntry
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.migration.FlywayMigrations
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import com.digitalasset.platform.sandbox.stores.ledger.{
  LedgerEntry,
  PartyLedgerEntry,
  PackageLedgerEntry
}
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

sealed trait InitStatus
final abstract class Initialized extends InitStatus
final abstract class Uninitialized extends InitStatus

object JdbcIndexerFactory {
  def apply(
      metrics: MetricRegistry,
      loggerFactory: NamedLoggerFactory): JdbcIndexerFactory[Uninitialized] =
    new JdbcIndexerFactory[Uninitialized](metrics, loggerFactory)
}

class JdbcIndexerFactory[Status <: InitStatus] private (
    metrics: MetricRegistry,
    loggerFactory: NamedLoggerFactory) {
  private val logger = loggerFactory.getLogger(classOf[JdbcIndexer])
  private[indexer] val asyncTolerance = 30.seconds

  def validateSchema(jdbcUrl: String)(
      implicit x: Status =:= Uninitialized): JdbcIndexerFactory[Initialized] = {
    FlywayMigrations(jdbcUrl, loggerFactory).validate()
    this.asInstanceOf[JdbcIndexerFactory[Initialized]]
  }

  def migrateSchema(jdbcUrl: String)(
      implicit x: Status =:= Uninitialized): JdbcIndexerFactory[Initialized] = {
    FlywayMigrations(jdbcUrl, loggerFactory).migrate()
    this.asInstanceOf[JdbcIndexerFactory[Initialized]]
  }

  def create(
      participantId: ParticipantId,
      actorSystem: ActorSystem,
      readService: ReadService,
      jdbcUrl: String)(implicit x: Status =:= Initialized): Future[JdbcIndexer] = {
    val materializer: Materializer = Materializer(actorSystem)

    implicit val ec: ExecutionContext = DEC

    val ledgerInit = readService
      .getLedgerInitialConditions()
      .runWith(Sink.head)(materializer)

    val ledgerDao: LedgerDao = initializeDao(jdbcUrl, metrics, actorSystem.dispatcher)
    for {
      LedgerInitialConditions(ledgerIdString, _, _) <- ledgerInit
      ledgerId = domain.LedgerId(ledgerIdString)
      _ <- initializeLedger(ledgerId, ledgerDao)
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      externalOffset <- ledgerDao.lookupExternalLedgerEnd()
    } yield {
      new JdbcIndexer(ledgerEnd, externalOffset, participantId, ledgerDao, metrics)(materializer) {
        override def close(): Unit = {
          super.close()
          materializer.shutdown()
          Await.result(actorSystem.terminate(), asyncTolerance)
          ()
        }
      }
    }
  }

  private def initializeDao(
      jdbcUrl: String,
      metrics: MetricRegistry,
      executionContext: ExecutionContext) = {
    val dbType = DbType.jdbcType(jdbcUrl)
    val dbDispatcher =
      DbDispatcher.start(
        jdbcUrl,
        if (dbType.supportsParallelWrites) defaultNumberOfShortLivedConnections else 1,
        loggerFactory,
        metrics,
      )
    LedgerDao.metered(
      JdbcLedgerDao(
        dbDispatcher,
        ContractSerializer,
        TransactionSerializer,
        ValueSerializer,
        KeyHasher,
        dbType,
        loggerFactory,
        executionContext,
      ),
      metrics,
    )
  }

  private def ledgerFound(foundLedgerId: LedgerId) = {
    logger.info(s"Found existing ledger with id: ${foundLedgerId.unwrap}")
    Future.successful(foundLedgerId)
  }

  private def initializeLedger(ledgerId: domain.LedgerId, ledgerDao: LedgerDao) = {
    ledgerDao
      .lookupLedgerId()
      .flatMap {
        case Some(foundLedgerId) if foundLedgerId == ledgerId =>
          ledgerFound(foundLedgerId)

        case Some(foundLedgerId) =>
          val errorMsg =
            s"Ledger id mismatch. Ledger id given ('$ledgerId') is not equal to the existing one ('$foundLedgerId')!"
          logger.error(errorMsg)
          Future.failed(new IllegalArgumentException(errorMsg))

        case None =>
          logger.info(s"Initializing ledger with id: ${ledgerId.unwrap}")
          ledgerDao.initializeLedger(ledgerId, 0).map(_ => ledgerId)(DEC)
      }(DEC)
  }
}

/**
  * @param initialInternalOffset The last offset internal to the indexer stored in the database.
  * @param beginAfterExternalOffset The last offset received from the read service.
  *                                 This offset has inclusive semantics,
  */
class JdbcIndexer private[indexer] (
    initialInternalOffset: Long,
    beginAfterExternalOffset: Option[LedgerString],
    participantId: ParticipantId,
    ledgerDao: LedgerDao,
    metrics: MetricRegistry)(implicit mat: Materializer)
    extends Indexer
    with AutoCloseable {

  @volatile
  private var headRef = initialInternalOffset

  @volatile
  private var lastReceivedRecordTime = Instant.now()

  @volatile
  private var lastReceivedOffset: LedgerString = _

  object Metrics {
    val stateUpdateProcessingTimer =
      metrics.timer("JdbcIndexer.processedStateUpdates")

    private[JdbcIndexer] def setup(): Unit = {
      val lastReceivedRecordTimeName = "JdbcIndexer.lastReceivedRecordTime"
      val lastReceivedOffsetName = "JdbcIndexer.lastReceivedOffset"
      val currentRecordTimeLagName = "JdbcIndexer.currentRecordTimeLag"

      metrics.remove(lastReceivedRecordTimeName)
      metrics.remove(lastReceivedOffsetName)
      metrics.remove(currentRecordTimeLagName)

      metrics.gauge(
        lastReceivedRecordTimeName,
        () =>
          new Gauge[Long] {
            override def getValue: Long = lastReceivedRecordTime.toEpochMilli
        })

      metrics.gauge(
        lastReceivedOffsetName,
        () =>
          new Gauge[LedgerString] {
            override def getValue: LedgerString = lastReceivedOffset
        })

      metrics.gauge(
        currentRecordTimeLagName,
        () =>
          new Gauge[Long] {
            override def getValue: Long =
              Instant.now().toEpochMilli - lastReceivedRecordTime.toEpochMilli
        })
      ()
    }
  }

  /**
    * Subscribes to an instance of ReadService.
    *
    * @param readService the ReadService to subscribe to
    * @return a handle of IndexFeedHandle or a failed Future
    */
  override def subscribe(readService: ReadService): Future[IndexFeedHandle] = {
    Metrics.setup()

    val (killSwitch, completionFuture) = readService
      .stateUpdates(beginAfterExternalOffset.map(Offset.assertFromString))
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .mapAsync(1)({
        case (offset, update) =>
          timedFuture(Metrics.stateUpdateProcessingTimer, handleStateUpdate(offset, update))
      })
      .toMat(Sink.ignore)(Keep.both)
      .run()

    Future.successful(indexHandleFromKillSwitch(killSwitch, completionFuture))
  }

  private def handleStateUpdate(offset: Offset, update: Update): Future[Unit] = {
    lastReceivedOffset = offset.toLedgerString
    lastReceivedRecordTime = update.recordTime.toInstant

    val externalOffset = Some(offset.toLedgerString)
    update match {
      case Heartbeat(recordTime) =>
        ledgerDao
          .storeLedgerEntry(
            headRef,
            headRef + 1,
            externalOffset,
            PersistenceEntry.Checkpoint(LedgerEntry.Checkpoint(recordTime.toInstant)))
          .map(_ => headRef = headRef + 1)(DEC)

      case PartyAddedToParticipant(
          party,
          displayName,
          hostingParticipantId,
          recordTime,
          submissionId) =>
        ledgerDao
          .storePartyEntry(
            headRef,
            headRef + 1,
            externalOffset,
            PartyLedgerEntry.AllocationAccepted(
              submissionId,
              hostingParticipantId,
              recordTime.toInstant,
              PartyDetails(party, Some(displayName), this.participantId == hostingParticipantId))
          )
          .map(_ => headRef = headRef + 1)(DEC)

      case PartyAllocationRejected(
          submissionId,
          hostingParticipantId,
          recordTime,
          rejectionReason) =>
        ledgerDao
          .storePartyEntry(
            headRef,
            headRef + 1,
            externalOffset,
            PartyLedgerEntry.AllocationRejected(
              submissionId,
              hostingParticipantId,
              recordTime.toInstant,
              rejectionReason
            )
          )
          .map(_ => headRef = headRef + 1)(DEC)

      case PublicPackageUpload(archives, optSourceDescription, recordTime, optSubmissionId) =>
        val recordTimeInstant = recordTime.toInstant
        val packages: List[(DamlLf.Archive, v2.PackageDetails)] = archives.map(
          archive =>
            archive -> v2.PackageDetails(
              size = archive.getPayload.size.toLong,
              knownSince = recordTimeInstant,
              sourceDescription = optSourceDescription))
        val optEntry: Option[PackageLedgerEntry] =
          optSubmissionId.map(submissionId =>
            PackageLedgerEntry.PackageUploadAccepted(submissionId, recordTimeInstant))
        ledgerDao
          .storePackageEntry(
            headRef,
            headRef + 1,
            externalOffset,
            packages,
            optEntry
          )
          .map(_ => headRef = headRef + 1)(DEC)

      case PublicPackageUploadRejected(submissionId, recordTime, rejectionReason) =>
        val entry: PackageLedgerEntry =
          PackageLedgerEntry.PackageUploadRejected(
            submissionId,
            recordTime.toInstant,
            rejectionReason
          )
        ledgerDao
          .storePackageEntry(
            headRef,
            headRef + 1,
            externalOffset,
            List.empty,
            Some(entry)
          )
          .map(_ => headRef = headRef + 1)(DEC)

      case TransactionAccepted(
          optSubmitterInfo,
          transactionMeta,
          transaction,
          transactionId,
          recordTime,
          divulgedContracts) =>
        val toAbsCoid: ContractId => AbsoluteContractId =
          EventIdFormatter.makeAbsCoid(transactionId)

        val blindingInfo = Blinding.blind(transaction.mapContractId(cid => cid: ContractId))

        val mappedDisclosure = blindingInfo.disclosure.map {
          case (nodeId, parties) =>
            EventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
        }

        val mappedLocalDivulgence = blindingInfo.localDivulgence.map {
          case (nodeId, parties) =>
            EventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
        }

        assert(blindingInfo.localDivulgence.isEmpty)

        val pt = PersistenceEntry.Transaction(
          LedgerEntry.Transaction(
            optSubmitterInfo.map(_.commandId),
            transactionId,
            optSubmitterInfo.map(_.applicationId),
            optSubmitterInfo.map(_.submitter),
            transactionMeta.workflowId,
            transactionMeta.ledgerEffectiveTime.toInstant,
            recordTime.toInstant,
            transaction
              .mapContractIdAndValue(toAbsCoid, _.mapContractId(toAbsCoid))
              .mapNodeId(EventIdFormatter.fromTransactionId(transactionId, _)),
            mappedDisclosure
          ),
          mappedLocalDivulgence,
          blindingInfo.globalDivulgence,
          divulgedContracts.map(c => c.contractId -> c.contractInst)
        )
        ledgerDao
          .storeLedgerEntry(headRef, headRef + 1, externalOffset, pt)
          .map(_ => headRef = headRef + 1)(DEC)

      case config: ConfigurationChanged =>
        ledgerDao
          .storeConfigurationEntry(
            headRef,
            headRef + 1,
            externalOffset,
            config.recordTime.toInstant,
            config.submissionId,
            config.participantId,
            config.newConfiguration,
            None
          )
          .map(_ => headRef = headRef + 1)(DEC)

      case configRejection: ConfigurationChangeRejected =>
        ledgerDao
          .storeConfigurationEntry(
            headRef,
            headRef + 1,
            externalOffset,
            configRejection.recordTime.toInstant,
            configRejection.submissionId,
            configRejection.participantId,
            configRejection.proposedConfiguration,
            Some(configRejection.rejectionReason)
          )
          .map(_ => headRef = headRef + 1)(DEC)

      case CommandRejected(recordTime, submitterInfo, reason) =>
        val rejection = PersistenceEntry.Rejection(
          LedgerEntry.Rejection(
            recordTime.toInstant,
            submitterInfo.commandId,
            submitterInfo.applicationId,
            submitterInfo.submitter,
            toDomainRejection(submitterInfo, reason)
          )
        )
        ledgerDao
          .storeLedgerEntry(headRef, headRef + 1, externalOffset, rejection)
          .map(_ => ())(DEC)
          .map(_ => headRef = headRef + 1)(DEC)
    }
  }

  private def toDomainRejection(
      submitterInfo: SubmitterInfo,
      state: RejectionReason): domain.RejectionReason = state match {
    case RejectionReason.Inconsistent =>
      domain.RejectionReason.Inconsistent(RejectionReason.Inconsistent.description)
    case RejectionReason.Disputed(reason @ _) => domain.RejectionReason.Disputed(state.description)
    case RejectionReason.MaximumRecordTimeExceeded =>
      domain.RejectionReason.TimedOut(state.description)
    case RejectionReason.ResourcesExhausted => domain.RejectionReason.OutOfQuota(state.description)
    case RejectionReason.PartyNotKnownOnLedger =>
      domain.RejectionReason.PartyNotKnownOnLedger(state.description)
    case RejectionReason.SubmitterCannotActViaParticipant(details) =>
      domain.RejectionReason.SubmitterCannotActViaParticipant(state.description)
  }

  private def indexHandleFromKillSwitch(
      ks: KillSwitch,
      completionFuture: Future[Done]): IndexFeedHandle = new IndexFeedHandle {
    override def stop(): Future[akka.Done] = {
      ks.shutdown()
      completionFuture
    }

    override def completed(): Future[akka.Done] = {
      completionFuture
    }
  }

  override def close(): Unit = {
    ledgerDao.close()
  }
}
