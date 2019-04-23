package com.daml.ledger.participant.state.kvutils

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.platform.akkastreams.Dispatcher

import scala.concurrent.Future

/* Implementation of the participant-state using the key-value utilities and an in-memory map. */
class InMemoryKVParticipantState extends ReadService with WriteService {

  val stateRef = new AtomicReference(State(0, Timestamp.Epoch, Map.empty))
  case class State(
      nextEntryId: Long,
      recordTime: Timestamp,
      entries: Map[Long, (Timestamp, KeyValueUtils.KVEntryBlob)],
  )
  val dispatcher: Dispatcher[Long] = Dispatcher(
    (idx, _) => idx + 1L,
    idx => Future.successful(stateRef.get.entries(idx)),
    0L,
    0L
  )

  def idxToKVEventId(idx: Long): KVEventId =
  

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
    dispatcher
      .startingAt(beginAfter.map(_.components.head).getOrElse(0L))
      .map {
        case (idx, (recordTime, entry)) =>
          Offset(Array(idx.toInt)) -> KeyValueUtils.entryToUpdate(idx, entry, recordTime)
      }
  }

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Unit = {
    val entry = KeyValueUtils.transactionToEntry(submitterInfo, transactionMeta, transaction)
    val updatedState = stateRef.updateAndGet { state =>
      val newHead = state.nextEntryId + 1
      state.copy(
        nextEntryId = newHead,
        entries = state.entries + (state.nextEntryId -> (state.recordTime, entry))
      )
    }
    dispatcher.signalNewHead(updatedState.nextEntryId - 1)
  }
}
