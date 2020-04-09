// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.reset

import java.io.File
import java.time.Instant
import java.util.UUID

import com.daml.api.util.TimestampConversion
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{
  IsStatusException,
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse
}
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  PartyManagementServiceGrpc
}
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest
}
import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.daml.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
import com.daml.ledger.api.v1.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest,
  TimeServiceGrpc
}
import com.daml.ledger.api.v1.transaction_filter.TransactionFilter
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.AbstractSandboxFixture
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.testing.{StreamConsumer, WaitForCompletionsObserver}
import com.daml.timer.RetryStrategy
import com.google.protobuf.empty.Empty
import io.grpc.Status
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

abstract class ResetServiceITBase
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Matchers
    with ScalaFutures
    with AbstractSandboxFixture
    with SuiteResourceManagementAroundEach
    with TestCommands {

  override def timeLimit: Span = scaled(30.seconds)

  override protected def config: SandboxConfig =
    super.config.copy(ledgerIdMode = LedgerIdMode.Dynamic)

  protected val eventually: RetryStrategy = RetryStrategy.exponentialBackoff(10, scaled(10.millis))

  override protected def darFile: File = new File(rlocation("ledger/test-common/Test-stable.dar"))

  protected def timeIsStatic: Boolean =
    config.timeProviderType.getOrElse(SandboxConfig.DefaultTimeProviderType) == TimeProviderType.Static

  protected def fetchLedgerId(): Future[String] =
    LedgerIdentityServiceGrpc
      .stub(channel)
      .getLedgerIdentity(GetLedgerIdentityRequest())
      .map(_.ledgerId)

  // Resets and waits for a new ledger identity to be available
  protected def reset(ledgerId: String): Future[String] =
    for {
      _ <- ResetServiceGrpc.stub(channel).reset(ResetRequest(ledgerId))
      newLedgerId <- eventually { (_, _) =>
        fetchLedgerId()
      }
    } yield newLedgerId

  protected def timedReset(ledgerId: String): Future[(String, FiniteDuration)] = {
    val start = System.nanoTime()
    reset(ledgerId).map(_ -> (System.nanoTime() - start).nanos)
  }

  protected def allocateParty(hint: String): Future[String] =
    PartyManagementServiceGrpc
      .stub(channel)
      .allocateParty(AllocatePartyRequest(hint))
      .map(_.partyDetails.get.party)

  protected def submitAndWait(req: SubmitAndWaitRequest): Future[Empty] =
    CommandServiceGrpc.stub(channel).submitAndWait(req)

  protected def activeContracts(ledgerId: String, f: TransactionFilter): Future[Set[CreatedEvent]] =
    new StreamConsumer[GetActiveContractsResponse](
      ActiveContractsServiceGrpc
        .stub(channel)
        .getActiveContracts(GetActiveContractsRequest(ledgerId, Some(f)), _))
      .all()
      .map(_.flatMap(_.activeContracts)(collection.breakOut))

  protected def submitAndExpectCompletions(
      ledgerId: String,
      commands: Int,
      party: String,
  ): Future[Unit] =
    for {
      _ <- Future.sequence(
        Vector.fill(commands)(
          CommandSubmissionServiceGrpc
            .stub(channel)
            .submit(dummyCommands(LedgerId(ledgerId), UUID.randomUUID.toString, party))))
      unit <- WaitForCompletionsObserver(commands)(
        CommandCompletionServiceGrpc
          .stub(channel)
          .completionStream(
            CompletionStreamRequest(
              ledgerId = ledgerId,
              applicationId = M.applicationId,
              parties = Seq(party),
              offset = Some(M.ledgerBegin)
            ),
            _))
    } yield unit

  protected def getTime(ledgerId: String): Future[Instant] =
    new StreamConsumer[GetTimeResponse](
      TimeServiceGrpc.stub(channel).getTime(GetTimeRequest(ledgerId), _))
      .first()
      .map(_.flatMap(_.currentTime).map(TimestampConversion.toInstant).get)

  protected def setTime(ledgerId: String, currentTime: Instant, newTime: Instant): Future[Unit] =
    TimeServiceGrpc
      .stub(channel)
      .setTime(
        SetTimeRequest(
          ledgerId,
          Some(TimestampConversion.fromInstant(currentTime)),
          Some(TimestampConversion.fromInstant(newTime)),
        ))
      .map(_ => ())

  "ResetService" when {
    "state is reset" should {
      "return a new ledger ID" in {
        for {
          lid1 <- fetchLedgerId()
          lid2 <- reset(lid1)
          throwable <- reset(lid1).failed
        } yield {
          lid1 should not equal lid2
          IsStatusException(Status.Code.NOT_FOUND)(throwable)
        }
      }

      "return new ledger ID - 20 resets" in {
        Future
          .sequence(Iterator.iterate(fetchLedgerId())(_.flatMap(reset)).take(20).toVector)
          .map(ids => ids.distinct should have size 20L)
      }

      // 5 attempts with 5 transactions each seem to strike the right balance to complete before the
      // 30 seconds test timeout in normal conditions while still causing the test to fail if
      // something goes wrong.
      //
      // The 10 seconds timeout built into the context's ledger reset will be hit if something goes
      // horribly wrong, causing an exception to report "waitForNewLedger: out of retries".
      val expectedResetCompletionTime = Span.convertSpanToDuration(scaled(5.seconds))
      s"consistently complete within $expectedResetCompletionTime" in {
        val numberOfCommands = 5
        val numberOfAttempts = 5
        Future
          .sequence(
            Iterator
              .iterate(fetchLedgerId()) { ledgerIdF =>
                for {
                  ledgerId <- ledgerIdF
                  party <- allocateParty(M.party)
                  _ <- submitAndExpectCompletions(ledgerId, numberOfCommands, party)
                  (newLedgerId, completionTime) <- timedReset(ledgerId)
                  _ = completionTime should be <= expectedResetCompletionTime
                } yield newLedgerId
              }
              .take(numberOfAttempts)
          )
          .map(_ => succeed)
      }

      "remove contracts from ACS after reset" in {
        for {
          ledgerId <- fetchLedgerId()
          party <- allocateParty(M.party)
          request = dummyCommands(LedgerId(ledgerId), "commandId1", party)
          _ <- submitAndWait(SubmitAndWaitRequest(commands = request.commands))
          events <- activeContracts(ledgerId, M.transactionFilter)
          _ = events should have size 3
          newLid <- reset(ledgerId)
          newEvents <- activeContracts(newLid, M.transactionFilter)
        } yield {
          newEvents should have size 0
        }
      }

      if (timeIsStatic) {
        "reset the time to the epoch" in {
          for {
            ledgerId <- fetchLedgerId()
            epoch <- getTime(ledgerId)

            now = Instant.now()
            _ <- setTime(ledgerId, epoch, now)
            newTime <- getTime(ledgerId)
            _ = newTime should not be epoch

            newLedgerId <- reset(ledgerId)
            resetTime <- getTime(newLedgerId)
          } yield {
            resetTime should be(epoch)
          }
        }
      }
    }
  }
}
