// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import com.daml.ledger.participant
import com.daml.ledger.participant.state.index.v1._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.platform.akkastreams.dispatcher.SignalDispatcher
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

final case class ReferenceIndexService(
    participantReadService: participant.state.v1.ReadService,
    initialConditions: LedgerInitialConditions)(implicit val mat: Materializer)
    extends participant.state.index.v1.IndexService
    with AutoCloseable {
  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val ec: ExecutionContext = mat.executionContext

  object StateController {
    private val stateChangeDispatcher = SignalDispatcher()
    private val currentState: AtomicReference[IndexState] = new AtomicReference(
      IndexState.initialState(initialConditions)
    )

    def updateState(f: IndexState => IndexState): Unit = {
      currentState.getAndUpdate(f(_))
      stateChangeDispatcher.signal()
    }

    def getState: IndexState = currentState.get

    // Subscribe to stream of new states. Does not emit a state until it
    // has been initialized (e.g. first update has been processed).
    def subscribe(): Source[IndexState, NotUsed] =
      stateChangeDispatcher
        .subscribe(signalOnSubscribe = true)
        .flatMapConcat { _signal =>
          val s = getState
          if (s.initialized)
            Source.single(s)
          else
            Source.empty
        }

    def close(): Unit =
      stateChangeDispatcher.close()
  }

  // Sink for updating the index state and forwarding the update and the resulting
  // new state to subscribers.
  private val updateStateSink = Sink.foreach[Seq[(Offset, Update)]] { batch =>
    // Process the state update batch
    StateController.updateState { state =>
      batch.foldLeft(state) {
        case (s, (uId, u)) =>
          logger.info(s"Applying update $uId: ${u.description}")
          s.tryApply(uId, u)
            .fold(err => sys.error(s"Invariant violation: $err"), identity)
      }
    }
  }

  // Flow to receive state updates, annotated with a kill switch to tear it down.
  private val stateUpdateKillSwitch =
    participantReadService
      .stateUpdates(beginAfter = None)
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      /* group updates to batches to limit the amount of state change signalling and
       * contention on the state sync variable. */
      .groupedWithin(50 /* batch size */, FiniteDuration(10, TimeUnit.MILLISECONDS))
      .to(updateStateSink)
      .run()

  // Return a result using a given handler function
  // Waits for state to be initialized (e.g. first
  // update to arrive in order to established ledger beginning and current end) before calling the handler.
  private def futureWithState[T](handler: IndexState => Future[T]): Future[T] = {
    val s = StateController.getState
    if (s.initialized) {
      handler(s)
    } else {
      // Wait until state is initialized.
      StateController
        .subscribe()
        .runWith(Sink.head)
        .flatMap(handler)
    }
  }

  override def close(): Unit = {
    StateController.close()
    stateUpdateKillSwitch.shutdown()
  }

  override def listPackages(): Future[List[PackageId]] =
    futureWithState { state =>
      Future.successful(state.packages.keys.toList)
    }

  override def isPackageRegistered(packageId: PackageId): Future[Boolean] =
    futureWithState { state =>
      Future.successful(state.packages.contains(packageId))
    }

  override def getPackage(packageId: PackageId): Future[Option[DamlLf.Archive]] =
    futureWithState { state =>
      Future.successful(
        state.packages.get(packageId)
      )
    }

  override def getLedgerConfiguration(): Future[Configuration] =
    futureWithState { state =>
      Future.successful(state.configuration)
    }

  override def getLedgerId(): Future[LedgerId] =
    Future.successful(StateController.getState.ledgerId)

  override def getLedgerBeginning(): Future[Offset] =
    futureWithState { s =>
      Future.successful(s.getBeginning)
    }

  override def getLedgerEnd(): Future[Offset] =
    futureWithState { s =>
      Future.successful(s.getUpdateId)
    }

  private def nodeIdToEventId(txId: TransactionId, nodeId: NodeId): EventId =
    Ref.PackageId.assertFromString(s"$txId/${nodeId.index}")

  private def transactionToAcsUpdateEvents(
      filter: TransactionFiltering,
      acceptedTx: TransactionAccepted) = {
    filter
      .visibleNodes(acceptedTx.transaction)
      .toList
      .flatMap {
        case (nodeId: NodeId, witnesses: Set[Party]) =>
          acceptedTx.transaction.nodes(nodeId) match {
            case create: NodeCreate[
                  Value.AbsoluteContractId,
                  Value.VersionedValue[Value.AbsoluteContractId]] =>
              List(
                acceptedTx.transactionMeta.workflowId ->
                  AcsUpdateEvent.Create(
                    nodeIdToEventId(acceptedTx.transactionId, nodeId),
                    create.coid,
                    create.coinst.template,
                    create.coinst.arg,
                    witnesses.toList
                  )
              )
            case exe: NodeExercises[
                  NodeId,
                  Value.AbsoluteContractId,
                  Value.VersionedValue[Value.AbsoluteContractId]] =>
              List(
                acceptedTx.transactionMeta.workflowId ->
                  AcsUpdateEvent.Archive(
                    nodeIdToEventId(acceptedTx.transactionId, nodeId),
                    exe.targetCoid,
                    exe.templateId,
                    witnesses.toList
                  )
              )
            case _ =>
              List.empty
          }
      }
  }

  override def getActiveContractSetSnapshot(
      filter: TransactionFilter): Future[ActiveContractSetSnapshot] =
    futureWithState { state =>
      val filtering = TransactionFiltering(filter)
      val events =
        Source.fromIterator(
          () =>
            state.txs.values
              .flatMap {
                case (acceptedTx, _) =>
                  transactionToAcsUpdateEvents(filtering, acceptedTx)
              }
              .collect {
                case (workflowId, create: AcsUpdateEvent.Create)
                    if state.activeContracts.contains(create.contractId) =>
                  (workflowId, create)
              }
              .toIterator)
      Future.successful(ActiveContractSetSnapshot(state.getUpdateId, events))
    }

  override def getActiveContractSetUpdates(
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter): Source[AcsUpdate, NotUsed] = {
    logger.trace(
      s"getActiveContractSetUpdates: beginAfter=$beginAfter, endAt=$endAt, filter=$filter")
    val filtering = TransactionFiltering(filter)

    getTransactionStream(beginAfter, endAt)
      .map {
        case (offset, (acceptedTx, blindingInfo)) =>
          val events =
            transactionToAcsUpdateEvents(filtering, acceptedTx)
              .map(_._2) /* ignore workflow id */
          // FIXME(JM): skip if events empty?
          AcsUpdate(
            optSubmitterInfo = acceptedTx.optSubmitterInfo,
            offset = offset,
            transactionMeta = acceptedTx.transactionMeta,
            transactionId = acceptedTx.transactionId,
            events = events
          )
      }
  }

  override def getAcceptedTransactions(
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter): Source[TransactionUpdate, NotUsed] =
    getTransactionStream(beginAfter, endAt)

  private def getTransactionStream(
      beginAfter: Option[Offset],
      endAt: Option[Offset]): Source[TransactionUpdate, NotUsed] = {

    StateController
      .subscribe()
      .statefulMapConcat { () =>
        var currentOffset: Option[Offset] = beginAfter
        state =>
          val txs =
            currentOffset
              .fold(state.txs) { offset =>
                state.txs.from(offset).dropWhile(getOffset(_) == offset)
              }
              .take(100) // produce in chunks of 100
          currentOffset = txs.lastOption.map(getOffset).orElse(currentOffset)
          txs
      }
      // Complete the stream once end (if given) has been reached.
      .takeWhile { t =>
        endAt.fold(true)(_ <= getOffset(t))
      }
      // Add two stream validator stages
      .via(MonotonicallyIncreasingOffsetValidation(getOffset))
      .via(BoundedOffsetValidation(getOffset, beginAfter, endAt))

  }

  private def getCompletionsFromState(
      state: IndexState,
      currentOffset: Option[Offset],
      applicationId: ApplicationId): List[CompletionEvent] = {
    val accepted =
      currentOffset
        .fold(state.txs) { offset =>
          state.txs.from(offset).dropWhile(getOffset(_) == offset)
        }
        .flatMap {
          case (offset, (acceptedTx, _blindingInfo)) =>
            acceptedTx.optSubmitterInfo.flatMap { sinfo =>
              if (sinfo.applicationId == applicationId) {
                Some(
                  CompletionEvent
                    .CommandAccepted(offset, sinfo.commandId, acceptedTx.transactionId))
              } else {
                None
              }
            }.toList
        }
        .toList
    val rejected =
      currentOffset
        .fold(state.rejections) { offset =>
          state.rejections.from(offset).dropWhile(_._1 == offset)
        }
        .flatMap {
          case (offset, rejectedCmd) =>
            if (rejectedCmd.submitterInfo.applicationId == applicationId) {
              List(
                CompletionEvent
                  .CommandRejected(offset, rejectedCmd.submitterInfo.commandId, rejectedCmd.reason))
            } else {
              List.empty
            }
        }
        .toList

    (CompletionEvent.Checkpoint(state.getUpdateId, state.recordTime)
      +: (accepted ++ rejected)).sortBy(_.offset)
  }

  override def getCompletions(
      beginAfter: Option[Offset],
      applicationId: ApplicationId,
      parties: List[Party]): Source[CompletionEvent, NotUsed] = {
    logger.trace(s"getCompletions: beginAfter=$beginAfter")
    StateController
      .subscribe()
      .statefulMapConcat({ () =>
        var currentOffset: Option[Offset] = beginAfter
        state =>
          val completions = getCompletionsFromState(state, currentOffset, applicationId)
          currentOffset = completions.lastOption.map(_.offset).orElse(currentOffset)
          logger.debug(s"Sending completions: ${completions}")
          completions
      })
  }

  override def lookupActiveContract(contractId: Value.AbsoluteContractId)
    : Future[Option[Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]]] =
    futureWithState { state =>
      Future.successful(state.activeContracts.get(contractId))
    }

  private def getOffset: TransactionUpdate => Offset = {
    case (offset, _) => offset
  }

  override def getLedgerRecordTimeStream(): Source[Time.Timestamp, NotUsed] =
    StateController
      .subscribe()
      .map(_.recordTime)
      // Scan over the states, only emitting a new timestamp when the record time has changed.
      .scan[Option[Time.Timestamp]](None) {
        case (Some(prevTime), currentTime) if prevTime == currentTime => None
        case (None, currentTime) => Some(currentTime)
      }
      .mapConcat(_.toList)

}
