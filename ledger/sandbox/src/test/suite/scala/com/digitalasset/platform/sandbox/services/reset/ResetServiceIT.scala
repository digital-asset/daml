// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.reset

import java.io.File
import java.util.UUID

import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.{
  IsStatusException,
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest
}
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
import com.digitalasset.ledger.api.v1.transaction_filter.TransactionFilter
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.{SandboxFixture, TestCommands}
import com.digitalasset.platform.testing.{StreamConsumer, WaitForCompletionsObserver}
import com.digitalasset.timer.RetryStrategy
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, DurationInt, DurationLong}

final class ResetServiceIT
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Matchers
    with ScalaFutures
    with SandboxFixture
    with SuiteResourceManagementAroundEach
    with TestCommands {

  private val eventually = RetryStrategy.exponentialBackoff(10, 10.millis)

  override def timeLimit: Span = scaled(30.seconds)

  override protected val config: SandboxConfig =
    super.config.copy(ledgerIdMode = LedgerIdMode.Dynamic())

  override protected def darFile: File = new File(rlocation("ledger/test-common/Test-stable.dar"))

  private def getLedgerId(): Future[String] =
    LedgerIdentityServiceGrpc
      .stub(channel)
      .getLedgerIdentity(GetLedgerIdentityRequest())
      .map(_.ledgerId)

  // Resets and waits for a new ledger identity to be available
  private def reset(ledgerId: String): Future[String] =
    for {
      _ <- ResetServiceGrpc.stub(channel).reset(ResetRequest(ledgerId))
      newLedgerId <- eventually { (_, _) =>
        getLedgerId()
      }
    } yield newLedgerId

  private def timedReset(ledgerId: String): Future[(String, Duration)] = {
    val start = System.nanoTime()
    reset(ledgerId).zip(Future.successful((System.nanoTime() - start).nanos))
  }

  private def submitAndWait(req: SubmitAndWaitRequest): Future[Empty] =
    CommandServiceGrpc.stub(channel).submitAndWait(req)

  private def activeContracts(ledgerId: String, f: TransactionFilter): Future[Set[CreatedEvent]] =
    new StreamConsumer[GetActiveContractsResponse](
      ActiveContractsServiceGrpc
        .stub(channel)
        .getActiveContracts(GetActiveContractsRequest(ledgerId, Some(f)), _))
      .all()
      .map(_.flatMap(_.activeContracts)(collection.breakOut))

  private def submitAndExpectCompletions(ledgerId: String, commands: Int): Future[Unit] =
    for {
      _ <- Future.sequence(
        Vector.fill(commands)(
          CommandSubmissionServiceGrpc
            .stub(channel)
            .submit(dummyCommands(LedgerId(ledgerId), UUID.randomUUID.toString))))
      unit <- WaitForCompletionsObserver(commands)(
        CommandCompletionServiceGrpc
          .stub(channel)
          .completionStream(
            CompletionStreamRequest(
              ledgerId = ledgerId,
              applicationId = M.applicationId,
              parties = Seq(M.party),
              offset = Some(M.ledgerBegin)
            ),
            _))
    } yield unit

  "ResetService" when {

    "state is reset" should {

      "return a new ledger ID" in {
        for {
          lid1 <- getLedgerId()
          lid2 <- reset(lid1)
          throwable <- reset(lid1).failed
        } yield {
          lid1 should not equal lid2
          IsStatusException(Status.Code.NOT_FOUND)(throwable)
        }
      }

      "return new ledger ID - 20 resets" in {
        Future
          .sequence(Iterator.iterate(getLedgerId())(_.flatMap(reset)).take(20).toVector)
          .map(ids => ids.distinct should have size 20L)
      }

      // 5 attempts with 5 transactions each seem to strike the right balance
      // to complete before the 30 seconds test timeout in normal conditions while
      // still causing the test to fail if something goes wrong
      //
      // the 10 seconds timeout built into the context's ledger reset will
      // be hit if something goes horribly wrong, causing an exception to report
      // waitForNewLedger: out of retries
      "consistently complete within 5 seconds" in {
        val numberOfCommands = 5
        val numberOfAttempts = 5
        Future
          .sequence(
            Iterator
              .iterate(getLedgerId()) { ledgerIdF =>
                for {
                  ledgerId <- ledgerIdF
                  _ <- submitAndExpectCompletions(ledgerId, numberOfCommands)
                  (newLedgerId, timing) <- timedReset(ledgerId) if timing <= 5.seconds
                } yield newLedgerId
              }
              .take(numberOfAttempts)
          )
          .map(_ => succeed)
      }

      "remove contracts from ACS after reset" in {
        for {
          lid <- getLedgerId()
          req = dummyCommands(LedgerId(lid), "commandId1")
          _ <- submitAndWait(SubmitAndWaitRequest(commands = req.commands))
          events <- activeContracts(lid, M.transactionFilter)
          _ = events.size shouldBe 3
          newLid <- reset(lid)
          newEvents <- activeContracts(newLid, M.transactionFilter)
        } yield {
          newEvents.size shouldBe 0
        }
      }

    }
  }
}
