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
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.platform.akkastreams.SignalDispatcher
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

    def subscribe(ledgerId: LedgerId): Source[IndexState, NotUsed] =
      stateChangeDispatcher
        .subscribe(signalOnSubscribe = true)
        .flatMapConcat { _signal =>
          val s = getState
          if (s.ledgerId != ledgerId) {
            Source.empty // FIXME(JM): or error?
          } else {
            Source.single(s)
          }
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

  // Return a result using a given handler function, after verifying that ledgerId matches,
  // and that the index state has been initialized. Waits for state to be initialized (e.g. first
  // update to arrive in order to established ledger beginning and current end) before calling the handler.
  private def asyncResultWithState[T](ledgerId: LedgerId)(
      handler: IndexState => Future[T]): AsyncResult[T] = {
    val s = StateController.getState
    if (s.ledgerId == ledgerId) {
      if (s.initialized) {
        handler(s).map(Right(_))
      } else {
        // Wait until state is initialized.
        StateController
          .subscribe(ledgerId)
          .dropWhile(_.initialized)
          .runWith(Sink.head)
          .flatMap(s2 => handler(s2).map(Right(_)))
      }
    } else {
      Future.successful(
        Left(
          IndexService.Err.LedgerIdMismatch(ledgerId, s.ledgerId)
        )
      )
    }
  }

  override def close(): Unit = {
    StateController.close()
    stateUpdateKillSwitch.shutdown()
  }

  override def listPackages(ledgerId: LedgerId): AsyncResult[List[PackageId]] =
    asyncResultWithState(ledgerId) { state =>
      Future.successful(state.packages.keys.toList)
    }

  override def isPackageRegistered(ledgerId: LedgerId, packageId: PackageId): AsyncResult[Boolean] =
    asyncResultWithState(ledgerId) { state =>
      Future.successful(state.packages.contains(packageId))
    }

  override def getPackage(
      ledgerId: LedgerId,
      packageId: PackageId): AsyncResult[Option[DamlLf.Archive]] =
    asyncResultWithState(ledgerId) { state =>
      Future.successful(
        state.packages.get(packageId)
      )
    }

  override def getLedgerConfiguration(ledgerId: LedgerId): AsyncResult[Configuration] =
    asyncResultWithState(ledgerId) { state =>
      Future.successful(state.configuration)
    }

  override def getLedgerId(): Future[LedgerId] =
    Future.successful(StateController.getState.ledgerId)

  override def getLedgerBeginning(ledgerId: LedgerId): AsyncResult[Offset] =
    asyncResultWithState(ledgerId) { s =>
      Future.successful(s.getBeginning)
    }

  override def getLedgerEnd(ledgerId: LedgerId): AsyncResult[Offset] =
    asyncResultWithState(ledgerId) { s =>
      Future.successful(s.getUpdateId)
    }

  private def nodeIdToEventId(txId: TransactionId, nodeId: NodeId): EventId =
    Ref.SimpleString.assertFromString(s"$txId/${nodeId.index}")

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
      ledgerId: LedgerId,
      filter: TransactionFilter): AsyncResult[ActiveContractSetSnapshot] =
    asyncResultWithState(ledgerId) { state =>
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
      ledgerId: LedgerId,
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter): AsyncResult[Source[AcsUpdate, NotUsed]] =
    asyncResultWithState(ledgerId) { _ =>
      Future {
        logger.debug(s"getActiveContractSetUpdates: $ledgerId, $beginAfter")
        val filtering = TransactionFiltering(filter)

        getTransactionStream(ledgerId, beginAfter, endAt)
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
    }

  override def getAcceptedTransactions(
      ledgerId: LedgerId,
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter): AsyncResult[Source[TransactionUpdate, NotUsed]] =
    asyncResultWithState(ledgerId) { state0 =>
      Future {
        logger.debug(s"getAcceptedTransactions: $ledgerId, $beginAfter")
        getTransactionStream(ledgerId, beginAfter, endAt)
        // FIXME(JM): Filter out non-matching transactions. Currently the service does this.
      }
    }

  private def getTransactionStream(
      ledgerId: LedgerId,
      beginAfter: Option[Offset],
      endAt: Option[Offset]): Source[TransactionUpdate, NotUsed] = {

    StateController
      .subscribe(ledgerId)
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
      beginFrom: Offset,
      applicationId: ApplicationId): List[CompletionEvent] = {
    val accepted =
      state.txs
        .from(beginFrom)
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
      state.rejections
        .from(beginFrom)
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
      ledgerId: LedgerId,
      beginAfter: Option[Offset],
      applicationId: ApplicationId,
      parties: List[Party]): AsyncResult[Source[CompletionEvent, NotUsed]] =
    asyncResultWithState(ledgerId) { state0 =>
      // FIXME(JM): Move the ledgerId check into the state subscription?
      logger.debug(s"getCompletions: $ledgerId, $beginAfter")

      Future {
        StateController
          .subscribe(ledgerId)
          .statefulMapConcat({ () =>
            var currentOffset: Offset =
              beginAfter.getOrElse(state0.getBeginning)
            state =>
              val completions = getCompletionsFromState(state, currentOffset, applicationId)
              currentOffset = completions.last.offset
              logger.debug(s"Sending completions: ${completions}")
              completions
          })
      }
    }

  override def lookupActiveContract(ledgerId: LedgerId, contractId: Value.AbsoluteContractId)
    : AsyncResult[Option[Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]]] =
    asyncResultWithState(ledgerId) { state =>
      Future {
        state.activeContracts.get(contractId)
      }
    }

  private def getOffset: TransactionUpdate => Offset = {
    case (offset, _) => offset
  }

  override def getLedgerRecordTimeStream(
      ledgerId: LedgerId): AsyncResult[Source[Time.Timestamp, NotUsed]] =
    asyncResultWithState(ledgerId) { state =>
      Future {
        StateController
          .subscribe(ledgerId)
          .map(_.recordTime)
          .scan[Option[Time.Timestamp]](Some(state.recordTime)) {
            case (Some(prevTime), currentTime) if prevTime == currentTime => None
            case (_, currentTime) => Some(currentTime)
          }
          .mapConcat(_.toList)
      }
    }
}
