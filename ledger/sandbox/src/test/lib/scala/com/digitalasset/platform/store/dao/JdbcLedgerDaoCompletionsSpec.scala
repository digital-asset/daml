// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.time.Instant
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.ApplicationId
import com.digitalasset.ledger.api.domain.RejectionReason
import com.digitalasset.platform.store.{CompletionFromTransaction, PersistenceEntry}
import com.digitalasset.platform.store.entries.LedgerEntry
import org.scalatest.{AsyncFlatSpec, Matchers, OptionValues}

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoCompletionsSpec extends OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (completions)"

  private val applicationId: ApplicationId = "JdbcLedgerDaoCompletionsSpec"
  private val party: Party = "JdbcLedgerDaoCompletionsSpec"
  private val parties: Set[Party] = Set(party)

  private def rejectWith(reason: RejectionReason): PersistenceEntry.Rejection =
    PersistenceEntry.Rejection(
      LedgerEntry.Rejection(
        recordTime = Instant.now,
        commandId = UUID.randomUUID().toString,
        applicationId = applicationId,
        submitter = party,
        rejectionReason = reason,
      )
    )

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
      val offset = response.checkpoint.value.offset.value.value.absolute.value.toLong
      offset should be > from.toLong
      offset should be <= to.toLong

      response.completions should have length 1
      val completion = response.completions.head

      completion.transactionId shouldBe empty
      completion.commandId shouldBe rejection.entry.commandId
      completion.status.value.code shouldNot be(io.grpc.Status.Code.OK)
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
      response should have length 0
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
      response should have length 0
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
      RejectionReason.TimedOut(""),
    )

    for {
      from <- ledgerDao.lookupLedgerEnd()
      pr <- Future.sequence(
        for (reason <- reasons; offset = nextOffset())
          yield ledgerDao.storeLedgerEntry(offset, rejectWith(reason)))
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
