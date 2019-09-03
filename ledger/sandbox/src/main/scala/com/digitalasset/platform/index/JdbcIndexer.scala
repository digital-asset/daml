// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink}
import akka.{Done, NotUsed}
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlLedger.{
  noOfShortLivedConnections,
  noOfStreamingConnections
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{
  LedgerDao,
  PersistenceEntry,
  JdbcLedgerDao
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object JdbcIndexer {
  private val logger = LoggerFactory.getLogger(classOf[JdbcIndexer])
  private[index] val asyncTolerance = 30.seconds

  def create(
      actorSystem: ActorSystem,
      readService: ReadService,
      jdbcUrl: String): Future[JdbcIndexer] = {
    val materializer: ActorMaterializer = ActorMaterializer()(actorSystem)
    val metricsManager = MetricsManager(false)

    implicit val ec: ExecutionContext = DEC

    val ledgerInit = readService
      .getLedgerInitialConditions()
      .runWith(Sink.head)(materializer)

    val ledgerDao: LedgerDao = initializeDao(jdbcUrl, metricsManager)
    for {
      LedgerInitialConditions(ledgerIdString, _, _) <- ledgerInit
      ledgerId = domain.LedgerId(ledgerIdString)
      _ <- initializeLedger(ledgerId, ledgerDao)
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      externalOffset <- ledgerDao.lookupExternalLedgerEnd()
    } yield {
      new JdbcIndexer(ledgerEnd, externalOffset, ledgerDao)(materializer) {
        override def close(): Unit = {
          super.close()
          materializer.shutdown()
          Await.result(actorSystem.terminate(), asyncTolerance)
          metricsManager.close()
        }
      }
    }
  }

  private def initializeDao(jdbcUrl: String, mm: MetricsManager) = {
    val dbType = JdbcLedgerDao.jdbcType(jdbcUrl)
    val dbDispatcher =
      DbDispatcher(
        jdbcUrl,
        dbType,
        if (dbType.supportsParallelWrites) noOfShortLivedConnections else 1,
        noOfStreamingConnections)
    val ledgerDao = LedgerDao.metered(
      JdbcLedgerDao(
        dbDispatcher,
        ContractSerializer,
        TransactionSerializer,
        ValueSerializer,
        KeyHasher,
        dbType))(mm)
    ledgerDao
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
class JdbcIndexer private (
    initialInternalOffset: Long,
    beginAfterExternalOffset: Option[LedgerString],
    ledgerDao: LedgerDao)(implicit mat: Materializer)
    extends Indexer
    with AutoCloseable {

  @volatile
  private var headRef = initialInternalOffset

  /**
    * Subscribes to an instance of ReadService.
    *
    * @param readService the ReadService to subscribe to
    * @return a handle of IndexFeedHandle or a failed Future
    */
  override def subscribe(readService: ReadService): Future[IndexFeedHandle] = {
    val (killSwitch, completionFuture) = readService
      .stateUpdates(beginAfterExternalOffset.map(Offset.assertFromString))
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .mapAsync(1)((handleStateUpdate _).tupled)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    Future.successful(indexHandleFromKillSwitch(killSwitch, completionFuture))
  }

  private def handleStateUpdate(offset: Offset, update: Update): Future[Unit] = {
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

      case PartyAddedToParticipant(party, displayName, _, _) =>
        ledgerDao.storeParty(party, Some(displayName), externalOffset).map(_ => ())(DEC)

      case PublicPackageUploaded(archive, sourceDescription, _, _) =>
        val uploadId = UUID.randomUUID().toString
        val uploadInstant = Instant.now() // TODO: use PublicPackageUploaded.recordTime for multi-ledgers (#2635)
        val packages: List[(DamlLf.Archive, v2.PackageDetails)] = List(
          archive -> v2.PackageDetails(
            size = archive.getPayload.size.toLong,
            knownSince = uploadInstant,
            sourceDescription = sourceDescription
          )
        )
        ledgerDao.uploadLfPackages(uploadId, packages, externalOffset).map(_ => ())(DEC)

      case TransactionAccepted(
          optSubmitterInfo,
          transactionMeta,
          transaction,
          transactionId,
          recordTime,
          divulgedContracts) =>
        val toAbsCoid: ContractId => AbsoluteContractId =
          SandboxEventIdFormatter.makeAbsCoid(transactionId)

        val blindingInfo = Blinding.blind(transaction.mapContractId(cid => cid: ContractId))

        val mappedDisclosure = blindingInfo.explicitDisclosure.map {
          case (nodeId, parties) =>
            SandboxEventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
        }

        val mappedLocalImplicitDisclosure = blindingInfo.localImplicitDisclosure.map {
          case (nodeId, parties) =>
            SandboxEventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
        }

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
              .mapNodeId(SandboxEventIdFormatter.fromTransactionId(transactionId, _)),
            mappedDisclosure
          ),
          mappedLocalImplicitDisclosure,
          blindingInfo.globalImplicitDisclosure,
          referencedContracts
        )
        ledgerDao
          .storeLedgerEntry(headRef, headRef + 1, externalOffset, pt)
          .map(_ => headRef = headRef + 1)(DEC)

      case _: ConfigurationChanged =>
        // TODO (GS) implement configuration changes
        Future.successful(())

      case _: ConfigurationChangeRejected =>
        // TODO(JM) implement configuration rejections
        Future.successful(())

      case CommandRejected(submitterInfo, reason) =>
        val rejection = PersistenceEntry.Rejection(
          LedgerEntry.Rejection(
            Instant.now(), // TODO should we get this from the backend?
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
    case RejectionReason.DuplicateCommand =>
      domain.RejectionReason.DuplicateCommandId(state.description)
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
