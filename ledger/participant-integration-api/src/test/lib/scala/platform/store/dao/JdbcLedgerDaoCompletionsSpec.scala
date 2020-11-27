// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.{Offset, RejectionReason, SubmitterInfo}
import com.daml.lf.data.Ref.Party
import com.daml.ledger.ApplicationId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.platform.ApiOffset
import com.daml.platform.store.dao.JdbcLedgerDaoCompletionsSpec._
import com.daml.platform.store.CompletionFromTransaction
import org.scalatest.{AsyncFlatSpec, LoneElement, Matchers, OptionValues}

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
        .getCommandCompletions(from, to, tx.applicationId.get, tx.actAs.toSet)
        .runWith(Sink.head)
    } yield {
      offsetOf(response) shouldBe offset

      val completion = response.completions.loneElement

      completion.transactionId shouldBe tx.transactionId
      completion.commandId shouldBe tx.commandId.get
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
        .getCommandCompletions(from, to, tx.applicationId.get, tx.actAs.toSet)
        .runWith(Sink.head)
      // Response 2: querying as a proper subset of all submitters
      (_, response2) <- ledgerDao.completions
        .getCommandCompletions(from, to, tx.applicationId.get, Set(tx.actAs.head))
        .runWith(Sink.head)
      // Response 3: querying as a proper superset of all submitters
      (_, response3) <- ledgerDao.completions
        .getCommandCompletions(from, to, tx.applicationId.get, tx.actAs.toSet + "UNRELATED")
        .runWith(Sink.head)
    } yield {
      response1.completions.loneElement.commandId shouldBe tx.commandId.get
      response2.completions.loneElement.commandId shouldBe tx.commandId.get
      response3.completions.loneElement.commandId shouldBe tx.commandId.get
    }
  }

  it should "return the expected completion for a rejection" in {
    val expectedCmdId = UUID.randomUUID.toString
    for {
      from <- ledgerDao.lookupLedgerEnd()
      offset <- storeRejection(RejectionReason.Inconsistent(""), expectedCmdId)
      to <- ledgerDao.lookupLedgerEnd()
      (_, response) <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, parties)
        .runWith(Sink.head)
    } yield {
      offsetOf(response) shouldBe offset

      val completion = response.completions.loneElement

      completion.transactionId shouldBe empty
      completion.commandId shouldBe expectedCmdId
      completion.status.value.code shouldNot be(io.grpc.Status.Code.OK.value())
    }
  }

  it should "return the expected completion for a multi-party rejection" in {
    val expectedCmdId = UUID.randomUUID.toString
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeMultiPartyRejection(RejectionReason.Inconsistent(""), expectedCmdId)
      to <- ledgerDao.lookupLedgerEnd()
      // Response 1: querying as all submitters
      (_, response1) <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, parties)
        .runWith(Sink.head)
      // Response 2: querying as a proper subset of all submitters
      (_, response2) <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, Set(parties.head))
        .runWith(Sink.head)
      // Response 3: querying as a proper superset of all submitters
      (_, response3) <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, parties + "UNRELATED")
        .runWith(Sink.head)
    } yield {
      response1.completions.loneElement.commandId shouldBe expectedCmdId
      response2.completions.loneElement.commandId shouldBe expectedCmdId
      response3.completions.loneElement.commandId shouldBe expectedCmdId
    }
  }

  it should "not return completions if the application id is wrong" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeRejection(RejectionReason.Inconsistent(""))
      to <- ledgerDao.lookupLedgerEnd()
      response <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId = "WRONG", parties)
        .runWith(Sink.seq)
    } yield {
      response shouldBe Seq.empty
    }
  }

  it should "not return completions if the parties do not match" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeRejection(RejectionReason.Inconsistent(""))
      to <- ledgerDao.lookupLedgerEnd()
      response1 <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, Set("WRONG"))
        .runWith(Sink.seq)
      response2 <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, Set("WRONG1", "WRONG2", "WRONG3"))
        .runWith(Sink.seq)
    } yield {
      response1 shouldBe Seq.empty
      response2 shouldBe Seq.empty
    }
  }

  it should "not return completions if the parties do not match (multi-party submission)" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- storeMultiPartyRejection(RejectionReason.Inconsistent(""))
      to <- ledgerDao.lookupLedgerEnd()
      response1 <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, Set("WRONG"))
        .runWith(Sink.seq)
      response2 <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, Set("WRONG1", "WRONG2", "WRONG3"))
        .runWith(Sink.seq)
    } yield {
      response1 shouldBe Seq.empty
      response2 shouldBe Seq.empty
    }
  }

  it should "return the expected status for each rejection reason" in {
    val reasons = Seq[RejectionReason](
      RejectionReason.Disputed(""),
      RejectionReason.Inconsistent(""),
      RejectionReason.InvalidLedgerTime(""),
      RejectionReason.ResourcesExhausted(""),
      RejectionReason.PartyNotKnownOnLedger(""),
      RejectionReason.SubmitterCannotActViaParticipant(""),
      RejectionReason.InvalidLedgerTime(""),
    )

    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- Future.sequence(reasons.map(reason => storeRejection(reason)))
      to <- ledgerDao.lookupLedgerEnd()
      responses <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, parties)
        .map(_._2)
        .runWith(Sink.seq)
    } yield {
      responses should have length reasons.length.toLong
      val returnedCodes = responses.flatMap(_.completions.map(_.status.get.code))
      for ((reason, code) <- reasons.zip(returnedCodes)) {
        code shouldBe CompletionFromTransaction.toErrorCode(reason).value()
      }
      succeed
    }
  }

  private def storeRejection(
      reason: RejectionReason,
      commandId: String = UUID.randomUUID().toString,
  ): Future[Offset] = {
    lazy val offset = nextOffset()
    ledgerDao
      .storeRejection(
        submitterInfo = Some(SubmitterInfo(List(party1), applicationId, commandId, Instant.EPOCH)),
        recordTime = Instant.now,
        offset = offset,
        reason = reason,
      )
      .map(_ => offset)
  }

  private def storeMultiPartyRejection(
      reason: RejectionReason,
      commandId: String = UUID.randomUUID().toString,
  ): Future[Offset] = {
    lazy val offset = nextOffset()
    ledgerDao
      .storeRejection(
        submitterInfo = Some(
          SubmitterInfo(List(party1, party2, party3), applicationId, commandId, Instant.EPOCH)),
        recordTime = Instant.now,
        offset = offset,
        reason = reason,
      )
      .map(_ => offset)
  }
}

private[dao] object JdbcLedgerDaoCompletionsSpec {

  private val applicationId: ApplicationId =
    "JdbcLedgerDaoCompletionsSpec".asInstanceOf[ApplicationId]
  private val party1: Party = "JdbcLedgerDaoCompletionsSpec1".asInstanceOf[Party]
  private val party2: Party = "JdbcLedgerDaoCompletionsSpec2".asInstanceOf[Party]
  private val party3: Party = "JdbcLedgerDaoCompletionsSpec3".asInstanceOf[Party]
  private val parties: Set[Party] = Set(party1, party2, party3)

  private def offsetOf(response: CompletionStreamResponse): Offset =
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.value.absolute.get)

}
