// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import org.apache.pekko.stream.scaladsl.Sink
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.TransactionNodeStatistics
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDaoCompletionsSpec.*
import com.google.rpc.status.Status as RpcStatus
import io.grpc.Status
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{LoneElement, OptionValues}

import java.util.UUID
import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoCompletionsSpec extends OptionValues with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (completions)"

  it should "return the expected completion for an accepted transaction" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (offset, tx) <- store(singleCreate)
      to <- ledgerDao.lookupLedgerEnd()
      (_, response) <- ledgerDao.completions
        .getCommandCompletions(
          from.lastOffset,
          to.lastOffset,
          tx.applicationId.value,
          tx.actAs.toSet,
        )
        .runWith(Sink.head)
    } yield {
      offsetOf(response) shouldBe offset

      val completion = response.completion.toList.head

      completion.updateId shouldBe tx.transactionId
      completion.commandId shouldBe tx.commandId.value
      completion.status.value.code shouldBe io.grpc.Status.Code.OK.value()
    }
  }

  it should "return the expected completion for an accepted multi-party transaction" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, tx) <- store(multiPartySingleCreate)
      to <- ledgerDao.lookupLedgerEnd()
      // Response 1: querying as all submitters
      (_, response1) <- ledgerDao.completions
        .getCommandCompletions(
          from.lastOffset,
          to.lastOffset,
          tx.applicationId.value,
          tx.actAs.toSet,
        )
        .runWith(Sink.head)
      // Response 2: querying as a proper subset of all submitters
      (_, response2) <- ledgerDao.completions
        .getCommandCompletions(
          from.lastOffset,
          to.lastOffset,
          tx.applicationId.value,
          Set(tx.actAs.head),
        )
        .runWith(Sink.head)
      // Response 3: querying as a proper superset of all submitters
      (_, response3) <- ledgerDao.completions
        .getCommandCompletions(
          from.lastOffset,
          to.lastOffset,
          tx.applicationId.value,
          tx.actAs.toSet + "UNRELATED",
        )
        .runWith(Sink.head)
    } yield {
      response1.completion.toList.head.commandId shouldBe tx.commandId.value
      response2.completion.toList.head.commandId shouldBe tx.commandId.value
      response3.completion.toList.head.commandId shouldBe tx.commandId.value
    }
  }

  it should "return the expected completion for a rejection" in {
    val expectedCmdId = UUID.randomUUID.toString
    val rejection = new state.Update.CommandRejected.FinalReason(
      RpcStatus.of(Status.Code.ABORTED.value(), "Stop.", Seq.empty)
    )
    for {
      from <- ledgerDao.lookupLedgerEnd()
      offset <- storeRejection(rejection, expectedCmdId)
      to <- ledgerDao.lookupLedgerEnd()
      (_, response) <- ledgerDao.completions
        .getCommandCompletions(from.lastOffset, to.lastOffset, applicationId, parties)
        .runWith(Sink.head)
    } yield {
      offsetOf(response) shouldBe offset

      val completion = response.completion.toList.head

      completion.updateId shouldBe empty
      completion.commandId shouldBe expectedCmdId
      completion.status shouldBe Some(rejection.status)
    }
  }

  it should "return the expected completion for a multi-party rejection" in {
    val expectedCmdId = UUID.randomUUID.toString
    val rejection = new state.Update.CommandRejected.FinalReason(
      RpcStatus.of(Status.Code.ALREADY_EXISTS.value(), "No thanks.", Seq.empty)
    )
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeMultiPartyRejection(rejection, expectedCmdId)
      to <- ledgerDao.lookupLedgerEnd()
      // Response 1: querying as all submitters
      (_, response1) <- ledgerDao.completions
        .getCommandCompletions(from.lastOffset, to.lastOffset, applicationId, parties)
        .runWith(Sink.head)
      // Response 2: querying as a proper subset of all submitters
      (_, response2) <- ledgerDao.completions
        .getCommandCompletions(from.lastOffset, to.lastOffset, applicationId, Set(parties.head))
        .runWith(Sink.head)
      // Response 3: querying as a proper superset of all submitters
      (_, response3) <- ledgerDao.completions
        .getCommandCompletions(from.lastOffset, to.lastOffset, applicationId, parties + "UNRELATED")
        .runWith(Sink.head)
    } yield {
      response1.completion.toList.head.commandId shouldBe expectedCmdId
      response2.completion.toList.head.commandId shouldBe expectedCmdId
      response3.completion.toList.head.commandId shouldBe expectedCmdId
    }
  }

  it should "not return completions if the application id is wrong" in {
    val rejection = new state.Update.CommandRejected.FinalReason(
      RpcStatus.of(Status.Code.INTERNAL.value(), "Internal error.", Seq.empty)
    )
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeRejection(rejection)
      to <- ledgerDao.lookupLedgerEnd()
      response <- ledgerDao.completions
        .getCommandCompletions(
          from.lastOffset,
          to.lastOffset,
          applicationId = Ref.ApplicationId.assertFromString("WRONG"),
          parties,
        )
        .runWith(Sink.seq)
    } yield {
      response shouldBe Seq.empty
    }
  }

  it should "not return completions if the parties do not match" in {
    val rejection = new state.Update.CommandRejected.FinalReason(
      RpcStatus.of(Status.Code.OUT_OF_RANGE.value(), "Too far.", Seq.empty)
    )
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeRejection(rejection)
      to <- ledgerDao.lookupLedgerEnd()
      response1 <- ledgerDao.completions
        .getCommandCompletions(from.lastOffset, to.lastOffset, applicationId, Set("WRONG"))
        .runWith(Sink.seq)
      response2 <- ledgerDao.completions
        .getCommandCompletions(
          from.lastOffset,
          to.lastOffset,
          applicationId,
          Set("WRONG1", "WRONG2", "WRONG3"),
        )
        .runWith(Sink.seq)
    } yield {
      response1 shouldBe Seq.empty
      response2 shouldBe Seq.empty
    }
  }

  it should "not return completions if the parties do not match (multi-party submission)" in {
    val rejection = new state.Update.CommandRejected.FinalReason(
      RpcStatus.of(Status.Code.PERMISSION_DENIED.value(), "Forbidden.", Seq.empty)
    )
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeMultiPartyRejection(rejection)
      to <- ledgerDao.lookupLedgerEnd()
      response1 <- ledgerDao.completions
        .getCommandCompletions(from.lastOffset, to.lastOffset, applicationId, Set("WRONG"))
        .runWith(Sink.seq)
      response2 <- ledgerDao.completions
        .getCommandCompletions(
          from.lastOffset,
          to.lastOffset,
          applicationId,
          Set("WRONG1", "WRONG2", "WRONG3"),
        )
        .runWith(Sink.seq)
    } yield {
      response1 shouldBe Seq.empty
      response2 shouldBe Seq.empty
    }
  }

  it should "allow arbitrarily large rejection reasons" in {
    val rejection = new state.Update.CommandRejected.FinalReason(
      RpcStatus.of(Status.Code.ABORTED.value(), (0 to 10000).map(_ => " ").mkString(""), Seq.empty)
    )
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeMultiPartyRejection(rejection)
      to <- ledgerDao.lookupLedgerEnd()
      response1 <- ledgerDao.completions
        .getCommandCompletions(from.lastOffset, to.lastOffset, applicationId, Set("WRONG"))
        .runWith(Sink.seq)
    } yield {
      response1 shouldBe Seq.empty
    }
  }

  private def storeRejection(
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
      commandId: Ref.CommandId = UUID.randomUUID().toString,
      submissionId: Ref.SubmissionId = UUID.randomUUID().toString,
  ): Future[Offset] = {
    val offset = nextOffset()
    ledgerDao
      .storeRejection(
        completionInfo = Some(
          state.CompletionInfo(
            actAs = List(party1),
            applicationId = applicationId,
            commandId = commandId,
            optDeduplicationPeriod = None,
            submissionId = Some(submissionId),
            statistics = Some(statistics),
          )
        ),
        recordTime = Timestamp.now(),
        offset,
        reason = reason,
      )
      .map(_ => offset)
  }

  private def storeMultiPartyRejection(
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
      commandId: Ref.CommandId = UUID.randomUUID().toString,
      submissionId: Ref.SubmissionId = UUID.randomUUID().toString,
  ): Future[Offset] = {
    lazy val offset = nextOffset()
    ledgerDao
      .storeRejection(
        completionInfo = Some(
          state.CompletionInfo(
            actAs = List(party1, party2, party3),
            applicationId = applicationId,
            commandId = commandId,
            optDeduplicationPeriod = None,
            submissionId = Some(submissionId),
            statistics = Some(statistics),
          )
        ),
        recordTime = Timestamp.now(),
        offset,
        reason = reason,
      )
      .map(_ => offset)
  }
}

private[dao] object JdbcLedgerDaoCompletionsSpec {

  private val applicationId = Ref.ApplicationId.assertFromString("JdbcLedgerDaoCompletionsSpec")
  private val party1 = Ref.Party.assertFromString("JdbcLedgerDaoCompletionsSpec1")
  private val party2 = Ref.Party.assertFromString("JdbcLedgerDaoCompletionsSpec2")
  private val party3 = Ref.Party.assertFromString("JdbcLedgerDaoCompletionsSpec3")
  private val parties = Set(party1, party2, party3)
  private val statistics = TransactionNodeStatistics.Empty

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  private def offsetOf(response: CompletionStreamResponse): Offset =
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.value.absolute.get)

}
