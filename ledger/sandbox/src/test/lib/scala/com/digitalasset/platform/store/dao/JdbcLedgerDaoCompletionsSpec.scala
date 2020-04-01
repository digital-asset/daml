// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.Party
import com.daml.ledger.{ApplicationId, CommandId}
import com.daml.ledger.api.domain.RejectionReason
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.platform.ApiOffset
import com.daml.platform.store.dao.JdbcLedgerDaoCompletionsSpec._
import com.daml.platform.store.entries.LedgerEntry
import com.daml.platform.store.{CompletionFromTransaction, PersistenceEntry}
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
        .getCommandCompletions(from, to, tx.applicationId.get, Set(tx.submittingParty.get))
        .runWith(Sink.head)
    } yield {
      offsetOf(response) shouldBe offset

      val completion = response.completions.loneElement

      completion.transactionId shouldBe tx.transactionId
      completion.commandId shouldBe tx.commandId.get
      completion.status.value.code shouldBe io.grpc.Status.Code.OK.value()
    }
  }

  it should "return the expected completion for a rejection" in {
    val offset = nextOffset()
    val rejection = rejectWith(RejectionReason.Inconsistent(""))
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(offset, rejection)
      to <- ledgerDao.lookupLedgerEnd()
      (_, response) <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, parties)
        .runWith(Sink.head)
    } yield {
      offsetOf(response) shouldBe offset

      val completion = response.completions.loneElement

      completion.transactionId shouldBe empty
      completion.commandId shouldBe rejection.entry.commandId
      completion.status.value.code shouldNot be(io.grpc.Status.Code.OK.value())
    }
  }

  it should "not return completions if the application id is wrong" in {
    val offset = nextOffset()
    val rejection = rejectWith(RejectionReason.Inconsistent(""))
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(offset, rejection)
      to <- ledgerDao.lookupLedgerEnd()
      response <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId = "WRONG", parties)
        .runWith(Sink.seq)
    } yield {
      response should have size 0 // `shouldBe empty` upsets WartRemover
    }
  }

  it should "not return completions if the parties do not match" in {
    val offset = nextOffset()
    val rejection = rejectWith(RejectionReason.Inconsistent(""))
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- ledgerDao.storeLedgerEntry(offset, rejection)
      to <- ledgerDao.lookupLedgerEnd()
      response <- ledgerDao.completions
        .getCommandCompletions(from, to, applicationId, Set("WRONG"))
        .runWith(Sink.seq)
    } yield {
      response should have size 0 // `shouldBe empty` upsets WartRemover
    }
  }

  it should "return the expected status for each rejection reason" in {
    val reasons = Seq[RejectionReason](
      RejectionReason.Disputed(""),
      RejectionReason.Inconsistent(""),
      RejectionReason.InvalidLedgerTime(""),
      RejectionReason.OutOfQuota(""),
      RejectionReason.PartyNotKnownOnLedger(""),
      RejectionReason.SubmitterCannotActViaParticipant(""),
      RejectionReason.InvalidLedgerTime(""),
    )

    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- Future.sequence(
        reasons.map(reason => ledgerDao.storeLedgerEntry(nextOffset(), rejectWith(reason))))
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

}

private[dao] object JdbcLedgerDaoCompletionsSpec {

  private val applicationId: ApplicationId =
    "JdbcLedgerDaoCompletionsSpec".asInstanceOf[ApplicationId]
  private val party: Party = "JdbcLedgerDaoCompletionsSpec".asInstanceOf[Party]
  private val parties: Set[Party] = Set(party)

  private def offsetOf(response: CompletionStreamResponse): Offset =
    ApiOffset.assertFromString(response.checkpoint.get.offset.get.value.absolute.get)

  private def rejectWith(reason: RejectionReason): PersistenceEntry.Rejection =
    PersistenceEntry.Rejection(
      LedgerEntry.Rejection(
        recordTime = Instant.now,
        commandId = UUID.randomUUID().toString.asInstanceOf[CommandId],
        applicationId = applicationId,
        submitter = party,
        rejectionReason = reason,
      )
    )

}
