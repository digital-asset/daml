// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink}
import akka.{Done, NotUsed}
import com.daml.ledger.participant.state.v2.Update._
import com.daml.ledger.participant.state.v2._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
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
  PostgresLedgerDao
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
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object PostgresIndexer {
  private val logger = LoggerFactory.getLogger(classOf[PostgresIndexer])
  private[index] val asyncTolerance = 30.seconds

  def create(readService: ReadService, jdbcUrl: String): Future[PostgresIndexer] = {
    val actorSystem = ActorSystem("postgres-indexer")
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
      new PostgresIndexer(ledgerEnd, externalOffset, ledgerDao)(materializer) {
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
    val dbDispatcher = DbDispatcher(jdbcUrl, noOfShortLivedConnections, noOfStreamingConnections)
    val ledgerDao = LedgerDao.metered(
      PostgresLedgerDao(
        dbDispatcher,
        ContractSerializer,
        TransactionSerializer,
        ValueSerializer,
        KeyHasher))(mm)
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

class PostgresIndexer(initialOffset: Long, beginAfter: Option[LedgerString], ledgerDao: LedgerDao)(
    implicit mat: Materializer)
    extends Indexer
    with AutoCloseable {

  @volatile
  private var headRef = initialOffset

  /**
    * Subscribes to an instance of ReadService.
    *
    * @param readService the ReadService to subscribe to
    * @param onError     callback to signal error during feed processing
    * @param onComplete  callback fired only once at normal feed termination.
    * @return a handle of IndexFeedHandle or a failed Future
    */
  override def subscribe(
      readService: ReadService,
      onError: Throwable => Unit,
      onComplete: () => Unit): Future[IndexFeedHandle] = {
    val (killSwitch, completionFuture) = readService
      .stateUpdates(beginAfter.map(Offset.assertFromString))
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .mapAsync(1)((handleStateUpdate _).tupled)
      .toMat(Sink.ignore)(Keep.both)
      .run()
    completionFuture.onComplete {
      case Failure(NonFatal(t)) => onError(t)
      case Success(_) => onComplete()
    }(DEC)

    Future.successful(indexHandleFromKillSwitch(killSwitch, completionFuture))
  }

  private def handleStateUpdate(offset: Offset, update: Update): Future[Unit] = {
    update match {
      case Heartbeat(recordTime) =>
        ledgerDao
          .storeLedgerEntry(
            headRef,
            headRef + 1,
            Some(offset.toLedgerString),
            PersistenceEntry.Checkpoint(LedgerEntry.Checkpoint(recordTime)))
          .map(_ => headRef = headRef + 1)(DEC)

      case PartyAddedToParticipant(party, displayName, _, _) =>
        ledgerDao.storeParty(party, Some(displayName)).map(_ => ())(DEC)

      case PublicPackagesUploaded(_, _, _, _) =>
        // TODO (GS) implement once https://github.com/digital-asset/daml/pull/1610 lands
        Future.successful(())

      case TransactionAccepted(
          optSubmitterInfo,
          transactionMeta,
          transaction,
          transactionId,
          recordTime,
          referencedContracts) =>
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
            transactionMeta.ledgerEffectiveTime,
            recordTime,
            transaction
              .mapContractIdAndValue(toAbsCoid, _.mapContractId(toAbsCoid))
              .mapNodeId(SandboxEventIdFormatter.fromTransactionId(transactionId, _)),
            mappedDisclosure
          ),
          mappedLocalImplicitDisclosure,
          blindingInfo.globalImplicitDisclosure
        )
        ledgerDao
          .storeLedgerEntry(headRef, headRef + 1, Some(offset.toLedgerString), pt)
          .map(_ => headRef = headRef + 1)(DEC)

      case ConfigurationChanged(newConfiguration) =>
        // TODO (GS) implement configuration changes
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
          .storeLedgerEntry(headRef, headRef + 1, Some(offset.toLedgerString), rejection)
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
  }

  override def close(): Unit = {
    ledgerDao.close()
  }
}
