// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.example

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Source, SourceQueueWithComplete}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.{
  AcceptedTransaction,
  Heartbeat,
  RejectedCommand
}
import com.digitalasset.ledger.backend.api.v1.RejectionReason.{
  Disputed,
  DuplicateCommandId,
  Inconsistent,
  TimedOut
}
import com.digitalasset.ledger.backend.api.v1._
import com.digitalasset.ledger.example.Transaction.TxDelta
import com.digitalasset.ledger.example.Transaction._
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.server.services.command.time.TimeModelValidator
import com.digitalasset.platform.services.time.TimeModel

import scala.concurrent.duration.FiniteDuration
import akka.actor.{Actor, Props}

/**
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
class Ledger(
    packages: DamlPackageContainer,
    timeModel: TimeModel,
    timeProvider: TimeProvider,
    mat: ActorMaterializer) {

  private val ledgerState = new AtomicReference[LedgerState](emptyLedgerState)

  private val ec = mat.system.dispatcher
  private val validator = TimeModelValidator(timeModel)

  /**
    * Task to dispatch ledger sync events to subscribers
    * in a repeatable ledger-defined order.
    */
  private val dispatcherTask = mat.system.actorOf(Props(new Actor {
    var offset: Int = 0 // the next offset to send
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    override def receive: Receive = {
      case () =>
        publishEvent(ledgerState.get().ledger(offset))
        offset = offset + 1
    }
  }))

  /**
    * Task to send out transient heartbeat events to subscribers.
    */
  private val heartbeatTask = mat.system.scheduler
    .schedule(
      FiniteDuration(5, TimeUnit.SECONDS),
      FiniteDuration(5, TimeUnit.SECONDS),
      () => publishHeartbeat()
    )(ec)

  def getCurrentLedgerEnd: LedgerSyncOffset =
    ledgerState.get().offset.toString

  def lookupActiveContract(contractId: AbsoluteContractId): Option[ContractEntry] =
    ledgerState.get().activeContracts.get(contractId)

  def activeContractSetSnapshot(): (LedgerSyncOffset, Map[AbsoluteContractId, ContractEntry]) = {
    val state = ledgerState.get()
    (state.offset.toString, state.activeContracts)
  }

  /**
    * Submit the supplied transaction to the ledger and notify all ledger event subscriptions of the
    * resultant event.
    * NOTE: Care must be taken to make sure we can always send out ledger sync events in the same
    * order, therefore when broadcasting new events, we use a separate task to enqueue events directly
    * from the ledger and only signal that task here.
    */
  def submitAndNotify(submission: TransactionSubmission): Unit = {
    submitToLedger(submission)
    notifyDispatcher()
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
  def submitToLedger(submission: TransactionSubmission): Unit = {

    val recordedAt = timeProvider.getCurrentTime

    // determine inputs and outputs
    val txDelta = Transaction.computeTxDelta(submission)

    // perform checks upfront and full validation
    val validationResult = validateSubmission(submission, txDelta, recordedAt, ledgerState.get())

    // atomically read and update the ledger state
    ledgerState.updateAndGet { prevState =>
      // advance ledger
      val newOffset = prevState.offset + 1

      // determine the final event, re-checking consistency now we have the (optimistic) lock
      val event = (for (_ <- validationResult;
        _ <- checkSubmission(submission, txDelta, recordedAt, prevState)) yield (())) match {
        case Left(mkEvent) => mkEvent(newOffset.toString)
        case Right(()) => mkAcceptedTransaction(newOffset.toString, submission, recordedAt)
      }

      // update the ledger
      val ledger = prevState.ledger :+ event

      // assign any new outputs absolute contract ids
      val outputs = mkContractOutputs(submission, txDelta, newOffset.toString)

      // prune active contract cache of any consumed inputs and add the outputs
      val activeContracts = (prevState.activeContracts -- txDelta.inputs) ++ outputs

      // record in duplication check cache
      val duplicationCheck = prevState.duplicationCheck + (
        (
          submission.applicationId,
          submission.commandId))

      LedgerState(
        ledger,
        newOffset,
        activeContracts,
        duplicationCheck,
        prevState.subscriptionQueues)
    }
    ()
  }

  /**
    * This is the relatively expensive transaction validation that is performed off the critical lock-holding path.
    * Note that we also perform the consistency checks prior to validation, in order to fail-fast if there is a consistency issue.
    * However these checks will need to be performed again once the lock is held.
    */
  private def validateSubmission(
      submission: TransactionSubmission,
      txDelta: TxDelta,
      recordedAt: Instant,
      ledgerState: LedgerState): Either[LedgerSyncOffset => LedgerSyncEvent, Unit] = {

    checkSubmission(submission, txDelta, recordedAt, ledgerState) match {
      case rejection @ Left(_) => rejection
      case Right(()) =>
        // validate transaction
        Validation
          .validate(submission, ledgerState.activeContracts.mapValues(_.contract), packages) match {
          case Validation.Failure(msg) =>
            Left(offset => mkRejectedCommand(Disputed(msg), offset, submission, recordedAt))
          case Validation.Success =>
            Right(())
        }
    }
  }

  /**
    * Check submission for duplicates, time-outs and consistency
    * NOTE: this is performed pre-commit prior to validation and also
    * during commit while the ledger state lock is held.
    */
  private def checkSubmission(
      submission: TransactionSubmission,
      txDelta: TxDelta,
      recordedAt: Instant,
      ledgerState: LedgerState): Either[LedgerSyncOffset => LedgerSyncEvent, Unit] = {

    // check for and ignore duplicates
    if (ledgerState.duplicationCheck.contains((submission.applicationId, submission.commandId))) {
      return Left(
        offset =>
          mkRejectedCommand(
            DuplicateCommandId("duplicate submission detected"),
            offset,
            submission,
            recordedAt))
    }

    // time validation
    validator
      .checkLet(
        timeProvider.getCurrentTime,
        submission.ledgerEffectiveTime,
        submission.maximumRecordTime,
        submission.commandId,
        submission.applicationId)
      .fold(
        t =>
          return Left(
            offset => mkRejectedCommand(TimedOut(t.getMessage), offset, submission, recordedAt)),
        _ => ())

    // check for consistency
    // NOTE: we do this by checking the activeness of all input contracts, both
    // consuming and non-consuming.
    if (!txDelta.inputs.subsetOf(ledgerState.activeContracts.keySet) ||
      !txDelta.inputs_nc.subsetOf(ledgerState.activeContracts.keySet)) {
      return Left(
        offset =>
          mkRejectedCommand(
            Inconsistent("one or more of the inputs has been consumed"),
            offset,
            submission,
            recordedAt))
    }
    Right(())
  }

  /**
    * Subscribe to the stream of ledger sync events
    */
  def ledgerSyncEvents(offset: Option[LedgerSyncOffset]): Source[LedgerSyncEvent, NotUsed] = {
    val (queue, source) =
      Source.queue[LedgerSyncEvent](Int.MaxValue, OverflowStrategy.fail).preMaterialize()(mat)
    val state = ledgerState.updateAndGet { state =>
      LedgerState(
        state.ledger,
        state.offset,
        state.activeContracts,
        state.duplicationCheck,
        state.subscriptionQueues :+ queue)
    }
    val snapshot = getEventsSnapshot(state, offset)
    Source(snapshot).concat(source)
  }

  /**
    * Shutdown this ledger implementation, stopping the heartbeat tasks and closing
    * all subscription streams.
    */
  def shutdownTasks(): Unit = {
    heartbeatTask.cancel()
    publishHeartbeat()
    for (q <- ledgerState.get().subscriptionQueues) {
      q.complete
    }
  }

  private def notifyDispatcher(): Unit = {
    dispatcherTask ! (())
  }

  private def publishHeartbeat(): Unit =
    publishEvent(Heartbeat(timeProvider.getCurrentTime, getCurrentLedgerEnd))

  private def publishEvent(event: LedgerSyncEvent): Unit = {
    for (q <- ledgerState.get().subscriptionQueues) {
      q.offer(event)
    }
  }

  private def getEventsSnapshot(
      ledgerState: LedgerState,
      offset: Option[LedgerSyncOffset]): List[LedgerSyncEvent] = {
    val index: Int = offset match {
      case Some(s) => Integer.parseInt(s)
      case None => 0
    }
    ledgerState.ledger.splitAt(index)._2
  }

  private def mkContractOutputs(
      submission: TransactionSubmission,
      txDelta: TxDelta,
      offset: LedgerSyncOffset): Map[AbsoluteContractId, ContractEntry] =
    txDelta.outputs.toList.map {
      case (contractId, contract) =>
        (
          mkAbsContractId(offset)(contractId),
          ContractEntry(
            contract.contract.mapValue(_.mapContractId(mkAbsContractId(offset))),
            submission.ledgerEffectiveTime,
            offset, // offset used as the transaction id
            submission.workflowId,
            contract.witnesses
          ))
    }.toMap

  private def mkAcceptedTransaction(
      offset: LedgerSyncOffset,
      submission: TransactionSubmission,
      recordedAt: Instant) = {
    val txId = offset // for this ledger, offset is also the transaction id
    AcceptedTransaction(
      toAbsTx(txId, submission.transaction),
      txId,
      Some(submission.submitter),
      submission.ledgerEffectiveTime,
      recordedAt,
      offset,
      submission.workflowId,
      submission.blindingInfo.explicitDisclosure.map {
        case (nid, parties) => (toAbsNodeId(txId, nid), parties)
      },
      Some(submission.applicationId),
      Some(submission.commandId)
    )
  }

  private def mkRejectedCommand(
      rejectionReason: RejectionReason,
      offset: LedgerSyncOffset,
      submission: TransactionSubmission,
      recordedAt: Instant) =
    RejectedCommand(
      recordedAt,
      submission.commandId,
      submission.submitter,
      rejectionReason,
      offset,
      Some(submission.applicationId))

  private def emptyLedgerState =
    LedgerState(
      List.empty[LedgerSyncEvent],
      0,
      Map.empty[AbsoluteContractId, ContractEntry],
      Set.empty[(String /*ApplicationId*/, CommandId)],
      List.empty[SourceQueueWithComplete[LedgerSyncEvent]]
    )

  /**
    * The state of the ledger, which must be updated atomically.
    */
  case class LedgerState(
      ledger: List[LedgerSyncEvent],
      offset: Int,
      activeContracts: Map[AbsoluteContractId, ContractEntry],
      duplicationCheck: Set[(String /*ApplicationId*/, CommandId)],
      subscriptionQueues: List[SourceQueueWithComplete[LedgerSyncEvent]])

}

/**
  * These are the entries in the Active Contract Set.
  */
case class ContractEntry(
    contract: Value.ContractInst[VersionedValue[AbsoluteContractId]],
    let: Instant,
    transactionId: String,
    workflowId: String,
    witnesses: Set[Ref.Party])
