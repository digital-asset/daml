package com.daml.ledger.participant.state.kvutils

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.Update.PublicPackageUploaded
import com.daml.ledger.participant.state.v1.{
  Offset,
  RejectionReason,
  SubmittedTransaction,
  SubmitterInfo,
  TransactionMeta,
  Update
}
import com.digitalasset.daml.lf.data.Ref.SimpleString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.Transaction.PartialTransaction
import com.digitalasset.daml_lf.DamlLf
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.platform.common.util.DirectExecutionContext
import org.scalatest.AsyncWordSpec

class InMemoryKVParticipantStateIT extends AsyncWordSpec with AkkaBeforeAndAfterAll {

  val emptyTransaction: SubmittedTransaction =
    PartialTransaction.initial.finish.right.get

  def submitterInfo(rt: Timestamp) = SubmitterInfo(
    submitter = SimpleString.assertFromString("Alice"),
    applicationId = "tests",
    commandId = "X",
    maxRecordTime = rt.addMicros(100000)
  )
  def transactionMeta(let: Timestamp) = TransactionMeta(
    ledgerEffectiveTime = let,
    workflowId = "tests"
  )

  "In-memory implementation" should {

    // FIXME(JM): Setup fixture for the participant-state
    // creation & teardown!

    "return initial conditions" in {
      val ps = new InMemoryKVParticipantState
      ps.getLedgerInitialConditions.map { ic =>
        ps.close
        assert(true)
      }(DirectExecutionContext)
    }

    "provide update after uploadArchive" in {
      val ps = new InMemoryKVParticipantState

      val archive = DamlLf.Archive.newBuilder
        .setHash("asdf")
        .build
      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).runWith(Sink.head).map {
          case (offset, update) =>
            ps.close
            assert(offset == Offset(Array(0L)))
            assert(update == PublicPackageUploaded(archive))
        }

      ps.uploadArchive(archive)
      waitForUpdateFuture
    }

    "provide update after transaction submission" in {

      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).runWith(Sink.head).map {
          case (offset, update) =>
            ps.close
            assert(offset == Offset(Array(0L)))
        }

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      waitForUpdateFuture
    }

    "reject duplicate commands" in {
      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = None).take(2).runWith(Sink.seq).map { updates =>
          ps.close

          val (offset1, update1) = updates(0)
          val (offset2, update2) = updates(1)
          assert(offset1 == Offset(Array(0L)))
          assert(update1.isInstanceOf[Update.TransactionAccepted])

          assert(offset2 == Offset(Array(1L)))
          assert(update2.isInstanceOf[Update.CommandRejected])
          assert(
            update2
              .asInstanceOf[Update.CommandRejected]
              .reason == RejectionReason.DuplicateCommand)
        }

      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)

      waitForUpdateFuture
    }

    "return second update with beginAfter=1" in {
      val ps = new InMemoryKVParticipantState
      val rt = ps.getNewRecordTime

      val waitForUpdateFuture =
        ps.stateUpdates(beginAfter = Some(Offset(Array(0L)))).runWith(Sink.head).map {
          case (offset, update) =>
            ps.close
            assert(offset == Offset(Array(1L)))
            assert(update.isInstanceOf[Update.CommandRejected])
        }
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      ps.submitTransaction(submitterInfo(rt), transactionMeta(rt), emptyTransaction)
      waitForUpdateFuture
    }
  }

}
