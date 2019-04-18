// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.impl.reference

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.RejectionReason._
import com.daml.ledger.participant.state.v1.Update._
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.participant.state.v1.impl.reference.Transaction.{TxDelta, _}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.lfpackage.{Ast, Decode}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.akkastreams.SignalDispatcher
import com.digitalasset.platform.server.services.command.time.TimeModelValidator
import com.digitalasset.platform.services.time.TimeModel
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/** TODO (SM): update/complete comments on this example as part of
  * https://github.com/digital-asset/daml/issues/388
  *
  * This is an example (simple) in-memory ledger, which comprises most of the ledger api backend.
  * The ledger here is essentially just a list of LedgerSyncEvents.
  * We also maintain an ephemeral cache of active contracts, to efficiently support queries
  * from the backend relating to activeness.
  *
  * @param packages
  * @param timeModel
  * @param timeProvider
  * @param mat
  */
class Ledger(timeModel: TimeModel, timeProvider: TimeProvider)(implicit mat: ActorMaterializer)
    extends ReadService
    with WriteService {

  object StateController {
    private val ledgerState = new AtomicReference(emptyLedgerState)
    private val stateChangeDispatcher = SignalDispatcher()

    def updateState(f: LedgerState => LedgerState): Unit = {
      ledgerState.getAndUpdate(f(_))
      stateChangeDispatcher.signal()
    }

    def subscribe(): Source[LedgerState, NotUsed] =
      stateChangeDispatcher
        .subscribe(signalOnSubscribe = true)
        .map(_signal => getState)

    def getState(): LedgerState = ledgerState.get

    def close(): Unit =
      stateChangeDispatcher.close()
  }

  private val ec = mat.system.dispatcher
  private val validator = TimeModelValidator(timeModel)
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val initialRecordTime = timeProvider.getCurrentTime

  /**
    * Task to send out transient heartbeat events to subscribers.
    */
  private val heartbeatTask = mat.system.scheduler
    .schedule(
      FiniteDuration(5, TimeUnit.SECONDS),
      FiniteDuration(5, TimeUnit.SECONDS),
      () => submitHeartbeat()
    )(ec)

  def getCurrentLedgerEnd: Offset =
    mkOffset(StateController.getState().ledger.size)

  /**
    * Submit the supplied transaction to the ledger and notify all ledger event subscriptions of the
    * resultant event.
    * NOTE: Care must be taken to make sure we can always send out ledger sync events in the same
    * order, therefore when broadcasting new events, we use a separate task to enqueue events directly
    * from the ledger and only signal that task here.
    */
  override def submitTransaction(
      submitterInfo: v1.SubmitterInfo,
      transactionMeta: v1.TransactionMeta,
      transaction: SubmittedTransaction): Unit = {
    // FIXME (SM): using a Future for the `submitTransaction` method would
    // allow to do some potentially heavy computation incl. pushing the data
    // out over the wire. Might be easier to implement than requiring to do a
    // hand-over between multiple threads.
    submit(submitterInfo, transactionMeta, transaction)
  }

  /**
    * Submit the supplied transaction to the ledger.
    * This will result in a LedgerSyncEvent being written to the ledger.
    * If the submission fails, then a RejectedCommand event is written, otherwise
    * an AcceptedTransaction event is written and the transaction is assumed "committed".
    * Note that this happens in two phases:
    * 1) We read the current state of the ledger then perform consistency checks
    *    and full validation on the transaction. The full validation is relatively
    *    expensive and we don't want to perform it in phase 2 below.
    * 2) We then re-read the ledger state using an optimistic lock, perform the
    *    consistency checks again and then update the ledger state. This phase must
    *    be side-effect free, as it may get repeated if the optimistic locking fails.
    */
  private def submit(
      submitterInfo: v1.SubmitterInfo,
      transactionMeta: v1.TransactionMeta,
      transaction: SubmittedTransaction): Unit = {

    logger.debug(s"Submitting transaction with commandId=${submitterInfo.commandId}")
    val recordedAt = timeProvider.getCurrentTime

    // determine inputs and outputs
    val txDelta = Transaction.computeTxDelta(transaction)

    // perform checks upfront and full validation
    val validationResult = validateSubmission(
      submitterInfo,
      transactionMeta,
      transaction,
      txDelta,
      recordedAt,
      StateController.getState)

    StateController.updateState { prevState =>
      // advance ledger
      val newOffset = prevState.ledger.size + 1

      // determine the final event, re-checking consistency now we have the (optimistic) lock
      val event = (for (_ <- validationResult;
        _ <- checkSubmission(submitterInfo, transactionMeta, txDelta, recordedAt, prevState))
        yield ()) match {
        case Left(event) => event
        case Right(()) => {
          val inputContractIds = txDelta.inputs ++ txDelta.inputs_nc
          val inputContracts = prevState.activeContracts
            .filterKeys(inputContractIds.contains(_))
            .toList
          mkAcceptedTransaction(
            submitterInfo,
            transactionMeta,
            transaction,
            mkOffset(newOffset),
            recordedAt,
            inputContracts)
        }
      }

      logger.debug(s"committing event $event")

      // update the ledger
      val ledger = prevState.ledger :+ event

      // assign any new outputs absolute contract ids
      val outputs = mkContractOutputs(transaction, txDelta, mkOffset(newOffset))

      // prune active contract cache of any consumed inputs and add the outputs
      val activeContracts = (prevState.activeContracts -- txDelta.inputs) ++ outputs

      // record in duplication check cache
      //
      // TODO (SM): make duplication check also include the submitter
      // https://github.com/digital-asset/daml/issues/387
      //
      val duplicationCheck = prevState.duplicationCheck + (
        (
          submitterInfo.applicationId,
          submitterInfo.commandId))

      LedgerState(prevState.ledgerId, ledger, prevState.packages, activeContracts, duplicationCheck)
    }
  }

  /**
    * This is the relatively expensive transaction validation that is performed off the critical lock-holding path.
    * Note that we also perform the consistency checks prior to validation, in order to fail-fast if there is a consistency issue.
    * However these checks will need to be performed again once the lock is held.
    */
  private def validateSubmission(
      submitterInfo: v1.SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      txDelta: TxDelta,
      recordedAt: Instant,
      ledgerState: LedgerState): Either[v1.Update, Unit] = {

    checkSubmission(submitterInfo, transactionMeta, txDelta, recordedAt, ledgerState)
      .flatMap(_ =>
        runPreCommitValidation(submitterInfo, transactionMeta, transaction, ledgerState))
  }

  private def runPreCommitValidation(
      submitterInfo: v1.SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      ledgerState: LedgerState): Either[v1.Update, Unit] = {
    Validation
      .validate(
        submitterInfo,
        transactionMeta,
        transaction,
        ledgerState.activeContracts,
        ledgerState.packages) match {
      case Validation.Failure(msg) =>
        Left(mkRejectedCommand(Disputed(msg), submitterInfo))
      case Validation.Success =>
        Right(())
    }
  }

  /**
    * Check submission for duplicates, time-outs wrt maxRecordTime and consistency
    * NOTE: this is performed pre-commit prior to validation and also
    * during commit while the ledger state lock is held.
    */
  private def checkSubmission(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      txDelta: TxDelta,
      recordedAt: Instant,
      ledgerState: LedgerState): Either[Update, Unit] = {
    checkDuplicates(submitterInfo, ledgerState)
      .flatMap(_ => checkLet(submitterInfo, transactionMeta, ledgerState))
      .flatMap(_ => checkActiveness(submitterInfo, txDelta, ledgerState))
  }

  // check for and ignore duplicates
  private def checkDuplicates(
      submitterInfo: SubmitterInfo,
      ledgerState: LedgerState): Either[Update, Unit] = {

    Either.cond(
      !ledgerState.duplicationCheck.contains(
        submitterInfo.applicationId -> submitterInfo.commandId),
      (),
      mkRejectedCommand(DuplicateCommand, submitterInfo)
    )
  }

  // validate LET time
  private def checkLet(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      ledgerState: LedgerState): Either[Update, Unit] = {
    validator
      .checkLet(
        // FIXME (SM): this is racy. We need to reflect current time into the
        // ledger-state and compare it there!
        //
        // I wonder why the validator needs all these params! We'd probably be
        // better off inlining the checks here.
        //
        // Also note that this needs to go hand-in-hand with adding the
        // TimeConfiguration as per https://github.com/digital-asset/daml/issues/385
        //
        timeProvider.getCurrentTime,
        transactionMeta.ledgerEffectiveTime.toInstant,
        submitterInfo.maxRecordTime.toInstant,
        submitterInfo.commandId,
        submitterInfo.applicationId
      )
      .fold(t => Left(mkRejectedCommand(MaximumRecordTimeExceeded, submitterInfo)), _ => Right(()))
  }

  // check for consistency
  // NOTE: we do this by checking the activeness of all input contracts, both
  // consuming and non-consuming.
  private def checkActiveness(
      submitterInfo: SubmitterInfo,
      txDelta: TxDelta,
      ledgerState: LedgerState): Either[Update, Unit] = {

    Either.cond(
      txDelta.inputs.subsetOf(ledgerState.activeContracts.keySet) &&
        txDelta.inputs_nc.subsetOf(ledgerState.activeContracts.keySet),
      (),
      mkRejectedCommand(Inconsistent, submitterInfo)
    )
  }

  /**
    * Subscribe to the stream of ledger updates
    */
  def stateUpdates(offset: Option[Offset]): Source[(Offset, v1.Update), NotUsed] = {
    StateController
      .subscribe()
      .statefulMapConcat { () =>
        var currentOffset: Long = offset.flatMap(_.components.headOption).getOrElse(0)
        state =>
          val newEvents = state.ledger
            .drop(currentOffset.toInt)
            .zipWithIndex
            .map { case (u, i) => (mkOffset(i), u) }
          currentOffset += newEvents.size

          logger.trace(
            s"stateUpdates: Emitting ${newEvents.size} new events from offset ${currentOffset}")
          newEvents
      }
  }

  def uploadArchive(archive: Archive): Unit = {
    val (pkgId: PackageId, pkg: Ast.Package) = Decode.decodeArchive(archive)

    StateController.updateState { state =>
      state.copy(
        ledger = state.ledger :+ PublicPackageUploaded(archive),
        packages = state.packages + (pkgId -> ((pkg, archive)))
      )
    }
  }

  /**
    * Shutdown this ledger implementation, stopping the heartbeat tasks and closing
    * all subscription streams.
    */
  def shutdownTasks(): Unit = {
    heartbeatTask.cancel()
    StateController.close()
  }

  private def submitHeartbeat(): Unit = {
    val heartbeat = Heartbeat(Timestamp.assertFromInstant(timeProvider.getCurrentTime))
    // atomically read and update the ledger state
    StateController.updateState { prevState =>
      prevState.copy(
        ledger = prevState.ledger :+ heartbeat,
      )
    }
  }

  private def mkContractOutputs(
      transaction: SubmittedTransaction,
      txDelta: TxDelta,
      offset: Offset): Map[AbsoluteContractId, AbsoluteContractInst] = {
    val txId = offset.toString

    txDelta.outputs.toList.map {
      case (contractId, contract) =>
        (
          mkAbsContractId(txId)(contractId),
          contract.contract.mapValue(_.mapContractId(mkAbsContractId(txId))))
    }.toMap
  }

  private def mkAcceptedTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      offset: Offset,
      recordTime: Instant,
      inputContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)]) = {
    val txId = offset.toString // for this ledger, offset is also the transaction id
    TransactionAccepted(
      Some(submitterInfo),
      transactionMeta,
      toAbsTx(txId, transaction),
      txId,
      Timestamp.assertFromInstant(recordTime),
      inputContracts
    )
  }

  private def mkRejectedCommand(rejectionReason: RejectionReason, submitterInfo: SubmitterInfo) =
    CommandRejected(Some(submitterInfo), rejectionReason)

  private def mkOffset(offset: Int): Offset =
    Offset(Array(offset.toLong))

  override def getLedgerInitialConditions(): Future[LedgerInitialConditions] =
    Future.successful(
      LedgerInitialConditions(
        ledgerId = StateController.getState.ledgerId,
        recordTimeEpoch = initialRecordTime
      )
    )

  private def emptyLedgerState = {
    LedgerState(
      Ref.SimpleString.assertFromString(UUID.randomUUID().toString),
      List(),
      Map.empty,
      Map.empty[AbsoluteContractId, AbsoluteContractInst],
      Set.empty[(ApplicationId, CommandId)]
    )
  }

  /**
    * The state of the ledger, which must be updated atomically.
    */
  case class LedgerState(
      ledgerId: LedgerId,
      ledger: List[v1.Update],
      packages: Map[Ref.PackageId, (Ast.Package, Archive)],
      activeContracts: Map[AbsoluteContractId, AbsoluteContractInst],
      duplicationCheck: Set[(ApplicationId, CommandId)])
}
