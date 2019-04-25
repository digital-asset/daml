package com.daml.ledger.participant.state.kvutils
/*

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.SimpleString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.akkastreams.Dispatcher
import com.google.protobuf.ByteString

import scala.collection.immutable.TreeMap
import scala.concurrent.Future

/* Implementation of the participant-state using the key-value utilities and an in-memory map. */
class InMemoryKVParticipantState extends ReadService with WriteService {
  val ledgerId = SimpleString.assertFromString("InMemoryKVParticipantState")

  val engine = Engine()

  val stateRef = new AtomicReference(
    State(0, Timestamp.Epoch, TreeMap.empty, TreeMap.empty, Map.empty))
  case class State(
      nextIdx: Long,
      recordTime: Timestamp,
      // FIXME(JM): Add command de-duplication
      entries: TreeMap[Long, (Timestamp, KeyValueUtils.KVEntryBlob)], // Tree map, so we can have ordered iteration.
      rejections: TreeMap[Long, Update.CommandRejected],
      archives: Map[PackageId, ByteString]
  )
  private val beginning = 0L
  val dispatcher: Dispatcher[Long, Update] = Dispatcher(
    (idx, _) => idx + 1L,
    idx => Future.successful(getUpdate(idx, stateRef.get)),
    beginning,
    beginning
  )

  private def getUpdate(idx: Long, state: State): Update = {
    state.entries
      .get(idx)
      .map {
        case (recordTime, entry) =>
          KeyValueUtils.entryToUpdate(idxToKVEntryId(idx), entry, recordTime)
      }
      .getOrElse {
        state.rejections(idx)
      }
  }

  private def idxToKVEntryId(idx: Long): KeyValueUtils.KVEntryId =
    ByteString.copyFromUtf8(idx.toString)

  private def kvEntryIdToIdx(entryId: KeyValueUtils.KVEntryId): Long =
    entryId.toStringUtf8.toLong

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] = {
    dispatcher
      .startingAt(beginAfter.map(_.components.head.toLong).getOrElse(beginning))
      .map {
        case (idx, update) =>
          Offset(Array(idx)) -> update
      }
  }

  override def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Unit = {
    val entry = KeyValueUtils.transactionToEntry(submitterInfo, transactionMeta, transaction)

    // Simulate a validation of the transaction. If the validation
    // fails we inject a rejected command, otherwise we store the blob.
    val txInfo = KeyValueUtils.computeTxInfo(entry)
    val stateSnapshot = stateRef.get

    // FIXME(JM): Add some error handling
    val inputEntries = txInfo.inputs.contracts
      .map(_._1)
      .map { entryId =>
        entryId -> stateSnapshot.entries(kvEntryIdToIdx(entryId))._2
      }
      .toMap

    KeyValueUtils.validateTransaction(
      engine,
      stateSnapshot.archives, // FIXME(JM): "txInfo.inputs.packages" aren't correctly computed yet.
      /*
      txInfo.inputs.packages.map { k =>
        k -> stateSnapshot.archives(k)
      }.toMap,*/
      inputEntries,
      txInfo
    ) match {
      case Left(err) =>
        val rejection =
          Update.CommandRejected(Some(submitterInfo), RejectionReason.Disputed(err.toString))
        val updatedState = stateRef.updateAndGet { state =>
          state.copy(
            nextIdx = state.nextIdx + 1,
            rejections = state.rejections + (state.nextIdx -> rejection)
          )
        }
        dispatcher.signalNewHead(updatedState.nextIdx)

      case Right(()) =>
        val updatedState = stateRef.updateAndGet { state =>
          state.copy(
            nextIdx = state.nextIdx + 1,
            entries = state.entries + (state.nextIdx -> ((state.recordTime, entry)))
          )
        }
        dispatcher.signalNewHead(updatedState.nextIdx)
    }
  }

  // Back-channel for uploading DAML-LF archives.
  def uploadArchive(archive: Archive): Unit = {
    val entry = KeyValueUtils.archiveToEntry(archive)
    val updatedState = stateRef.updateAndGet { state =>
      state.copy(
        nextIdx = state.nextIdx + 1,
        entries = state.entries + (state.nextIdx -> ((state.recordTime, entry))),
        archives = state.archives + (SimpleString
          .assertFromString(archive.getHash) -> archive.toByteString)
      )
    }
    dispatcher.signalNewHead(updatedState.nextIdx)
  }

  /** Retrieve the static initial conditions of the ledger, containing
 * the ledger identifier and the epoch of the ledger record time.
 *
 * Returns a future since the implementation may need to first establish
 * connectivity to the underlying ledger. The implementer may assume that
 * this method is called only once, or very rarely.
 */
  override def getLedgerInitialConditions(): Future[LedgerInitialConditions] =
    Future.successful(LedgerInitialConditions(ledgerId, Timestamp.Epoch))
}
 */
