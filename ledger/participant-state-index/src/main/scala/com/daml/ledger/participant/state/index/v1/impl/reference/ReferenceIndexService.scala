package com.daml.ledger.participant.state.index.v1.impl.reference

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import com.daml.ledger.participant
import com.daml.ledger.participant.state.index.v1._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.transaction.Node.{NodeCreate, NodeExercises}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.platform.akkastreams.SignalDispatcher
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, SyncVar}

final case class ReferenceIndexService(participantReadService: participant.state.v1.ReadService)(
    implicit val mat: Materializer)
    extends participant.state.index.v1.IndexService
    with AutoCloseable {
  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val ec: ExecutionContext = mat.executionContext

  object StateController {
    private val stateChangeDispatcher = SignalDispatcher()
    private val currentState: SyncVar[IndexState] = {
      val sv = new SyncVar[IndexState]()
      sv.put(IndexState.initialState)
      sv
    }

    def updateState(f: IndexState => IndexState): Unit = {
      currentState.put(
        f(currentState.take())
      )
      stateChangeDispatcher.signal()
    }

    def getState: IndexState = currentState.get

    def subscribe(indexId: IndexId): Source[IndexState, NotUsed] =
      stateChangeDispatcher
        .subscribe(signalOnSubscribe = true)
        .flatMapConcat { _signal =>
          val s = getState
          if (s.indexId != indexId) {
            Source.empty // FIXME(JM): or error?
          } else {
            Source.single(s)
          }
        }

    def close(): Unit =
      stateChangeDispatcher.close()
  }

  def waitUntilInitialized = {
    while (!StateController.getState.initialized) {
      logger.info("Waiting for ledger to be established...")
      Thread.sleep(1000)
    }
  }

  // Sink for updating the index state and forwarding the update and the resulting
  // new state to subscribers.
  private val updateStateSink = Sink.foreach[Seq[(UpdateId, Update)]] { batch =>
    // Process the state update batch
    StateController.updateState { state =>
      batch.foldLeft(state) {
        case (s, (uId, u)) =>
          logger.info(s"Applying update $uId: ${u.description}")
          s.tryApply(uId, u)
            .getOrElse(sys.error("FIXME invariant violation"))
      }
    }
  }

  // Flow to receive state updates, annotated with a kill switch to tear it down.
  private val stateUpdateKillSwitch =
    participantReadService
      .stateUpdates(beginAfter = None)
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .groupedWithin(50 /* batch size */, FiniteDuration(50, TimeUnit.MILLISECONDS))
      .to(updateStateSink)
      .run()

  private def asyncResultWithState[T](indexId: IndexId)(
      handler: IndexState => Future[T]): AsyncResult[T] = {
    val s = StateController.getState
    if (s.indexId == indexId) {
      handler(s).map(Right(_))
    } else {
      Future.successful(
        Left(
          IndexService.Err.IndexIdMismatch(indexId, s.indexId)
        )
      )
    }
  }

  override def close(): Unit = {
    StateController.close()
    stateUpdateKillSwitch.shutdown()
  }

  override def listPackages(indexId: IndexId): AsyncResult[List[PackageId]] =
    asyncResultWithState(indexId) { state =>
      Future.successful(state.packages.keys.toList)
    }

  override def isPackageRegistered(indexId: IndexId, packageId: PackageId): AsyncResult[Boolean] =
    asyncResultWithState(indexId) { state =>
      Future.successful(state.packages.contains(packageId))
    }

  override def getPackage(
      indexId: IndexId,
      packageId: PackageId): AsyncResult[Option[DamlLf.Archive]] =
    asyncResultWithState(indexId) { state =>
      Future.successful(
        state.packages
          .get(packageId)
          .map(_._2)
      )
    }

  override def getLedgerConfiguration(indexId: IndexId): AsyncResult[Configuration] =
    asyncResultWithState(indexId) { state =>
      Future.successful(state.getConfiguration)
    }

  override def getCurrentIndexId(): Future[IndexId] =
    Future.successful(StateController.getState.indexId)

  override def getCurrentStateId(): Future[StateId] =
    Future.successful(StateController.getState.getStateId)

  override def getLedgerBeginning(indexId: IndexId): AsyncResult[Offset] =
    asyncResultWithState(indexId) { state =>
      Future.successful(0)
    }

  override def getLedgerEnd(indexId: IndexId): AsyncResult[Offset] =
    asyncResultWithState(indexId) { state =>
      Future.successful(state.nextOffset)
    }

  private def nodeIdToEventId(txId: TransactionId, nodeId: NodeId): String =
    s"$txId/${nodeId.index}"

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
      indexId: IndexId,
      filter: TransactionFilter): AsyncResult[ActiveContractSetSnapshot] =
    asyncResultWithState(indexId) { state =>
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
      Future.successful(ActiveContractSetSnapshot(state.nextOffset - 1, events))
    }

  override def getActiveContractSetUpdates(
      indexId: IndexId,
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter): AsyncResult[Source[AcsUpdate, NotUsed]] =
    asyncResultWithState(indexId) { _ =>
      Future {
        logger.debug(s"getActiveContractSetUpdates: $indexId, $beginAfter")
        val filtering = TransactionFiltering(filter)

        StateController
          .subscribe(indexId)
          .statefulMapConcat { () =>
            var currentOffset: Offset = beginAfter.getOrElse(0)
            state =>
              // NOTE(JM): Include one transaction beyond the end offset, so we know
              // when to cut the stream.
              val endOffset = endAt.getOrElse(state.nextOffset)
              val acsUpdates = state.txs
                .range(currentOffset, endOffset)
                .map {
                  case (offset, (acceptedTx, _blindingInfo)) =>
                    val events =
                      transactionToAcsUpdateEvents(filtering, acceptedTx)
                        .map(_._2) /* ignore workflow id */
                    AcsUpdate(
                      optSubmitterInfo = acceptedTx.optSubmitterInfo,
                      offset = offset,
                      transactionMeta = acceptedTx.transactionMeta,
                      transactionId = acceptedTx.transactionId,
                      events = events.toList
                    )
                }
              currentOffset = state.nextOffset
            acsUpdates
          }
          // Complete the stream once end (if given) has been reached.
          .takeWhile { acsUpdate =>
            endAt.fold(true)(_ <= acsUpdate.offset)
          }
      }
    }

  override def getAcceptedTransactions(
      indexId: IndexId,
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter)
    : AsyncResult[Source[(Offset, (TransactionAccepted, BlindingInfo)), NotUsed]] =
    asyncResultWithState(indexId) { _ =>
      Future {
        logger.debug(s"getAcceptedTransactions: $indexId, $beginAfter")
        StateController
          .subscribe(indexId)
          .statefulMapConcat { () =>
            // FIXME(JM): Filtering.
            var currentOffset: Offset = beginAfter.getOrElse(0)
            state =>
              logger.debug(s"getAcceptedTransactions: got state ${state.getUpdateId}")
              // NOTE(JM): Include one transaction beyond the end offset, so we know
              // when to cut the stream.
              val endOffset = endAt.getOrElse(state.nextOffset)
              val txs = state.txs.range(currentOffset, endOffset)
              currentOffset = state.nextOffset
              txs
          }

          // Complete the stream once end (if given) has been reached.
          .takeWhile {
            case (offset, _) =>
              endAt.fold(true)(_ <= offset)
          }

        // FIXME(JM): Filter out non-matching transactions. Currently the service does this.
        /*.filter {
          case (offset, tx) =>
          }*/

      }
    }

  def getCompletionsFromState(
      state: IndexState,
      beginFrom: Offset,
      applicationId: String): List[CompletionEvent] = {
    val accepted =
      state.txs
        .from(beginFrom)
        .flatMap {
          case (offset, (acceptedTx, _blindingInfo)) =>
            acceptedTx.optSubmitterInfo.flatMap { sinfo =>
              if (sinfo.applicationId == applicationId) {
                Some(CompletionEvent.CommandAccepted(offset, sinfo.commandId))
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
            rejectedCmd.optSubmitterInfo.flatMap { sinfo =>
              if (sinfo.applicationId == applicationId) {
                Some(CompletionEvent.CommandRejected(offset, sinfo.commandId, rejectedCmd.reason))
              } else {
                None
              }
            }.toList
        }
        .toList

    // FiXME(JM): Do we need to persist "Heartbeat" events and emit those here
    // or is it fine that this is synthetic?
    (CompletionEvent.Checkpoint(state.nextOffset - 1, state.recordTime.get)
      +: (accepted ++ rejected)).sortBy(_.offset)
  }

  override def getCompletions(
      indexId: IndexId,
      beginAfter: Option[Offset],
      applicationId: String,
      parties: List[String]): AsyncResult[Source[CompletionEvent, NotUsed]] =
    asyncResultWithState(indexId) { _ =>
      // FIXME(JM): Move the indexId check into the state subscription?
      logger.debug(s"getCompletions: $indexId, $beginAfter")

      Future {
        StateController
          .subscribe(indexId)
          .statefulMapConcat({ () =>
            var currentBeginFrom: Offset = beginAfter.fold(0L)(_ + 1)
            state =>
              val completions = getCompletionsFromState(state, currentBeginFrom, applicationId)
              currentBeginFrom = state.nextOffset
              logger.debug(s"Sending completions: ${completions}")
              completions
          })
      }

    }

  override def lookupActiveContract(indexId: IndexId, contractId: Value.AbsoluteContractId)
    : AsyncResult[Option[Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]]] =
    asyncResultWithState(indexId) { state =>
      Future {
        state.activeContracts.get(contractId)
      }
    }
}
