// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.scaladsl.Sink
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  IsStatusException,
  SuiteResourceManagementAroundEach,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_completion_service.{
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.services.TestCommands
import com.digitalasset.platform.sandbox.utils.InfiniteRetries
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.scalatest.concurrent.{AsyncTimeLimitedTests, ScalaFutures}
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, Suite}

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}

class ResetServiceIT
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Suite
    with InfiniteRetries
    with Matchers
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture // TODO: this suite should not be using LedgerContext, as it is smart and hides too much of the reset mechanism
    with ScalaFutures
    with TestCommands
    with SuiteResourceManagementAroundEach {

  override def timeLimit: Span = scaled(30.seconds)

  override protected val config: Config =
    Config.default.withLedgerIdMode(LedgerIdMode.Dynamic())

  override protected def darFile: File = new File(rlocation("ledger/test-common/Test-stable.dar"))

  private val allTemplatesForParty = M.transactionFilter

  private def extractEvents(response: GetActiveContractsResponse): Set[CreatedEvent] =
    response.activeContracts.toSet

  "ResetService" when {
    "state is reset" should {

      "return a new ledger ID" in allFixtures { ctx1 =>
        val lid1 = ctx1.ledgerId
        for {
          ctx2 <- ctx1.reset()
          lid2 = ctx2.ledgerId
          throwable <- ctx1.reset().failed
        } yield {
          lid1 should not equal lid2
          IsStatusException(Status.Code.NOT_FOUND)(throwable)
        }
      }

      "return new ledger ID - multiple resets" in allFixtures { initialCtx =>
        case class Acc(ctx: LedgerContext, lids: List[LedgerId])

        val resets = (1 to 20).foldLeft(Future.successful(Acc(initialCtx, List.empty))) {
          (eventualAcc, _) =>
            for {
              acc <- eventualAcc
              nCtx <- acc.ctx.reset()
              lid = nCtx.ledgerId
              lids = acc.lids :+ lid
            } yield Acc(nCtx, lids)
        }

        resets.flatMap { acc =>
          acc.lids.toSet should have size acc.lids.size.toLong
        }
      }

      // 5 attempts with 5 transactions each seem to strike the right balance
      // to complete before the 30 seconds test timeout in normal conditions while
      // still causing the test to fail if something goes wrong
      //
      // the 10 seconds timeout built into the context's ledger reset will
      // be hit if something goes horribly wrong, causing an exception to report
      // waitForNewLedger: out of retries
      "consistently complete within 5 seconds" in allFixtures { initialCtx =>
        case class Acc(ctx: LedgerContext, times: List[Long])

        val operator = "party"
        val numberOfCommands = 5

        final class WaitForNCompletions(threshold: Int)
            extends StreamObserver[CompletionStreamResponse] {
          private[this] val promise = Promise[Unit]
          val result: Future[Unit] = promise.future

          var counter = new AtomicInteger(0)

          override def onNext(v: CompletionStreamResponse): Unit = {
            val total = counter.addAndGet(v.completions.size)
            if (total >= threshold) {
              val _ = promise.trySuccess(())
            }
          }

          override def onError(throwable: Throwable): Unit = {
            val _ = promise.tryFailure(throwable)
          }

          override def onCompleted(): Unit = {
            val _ = promise.tryFailure(new RuntimeException("no more completions"))
          }
        }

        val resets = (1 to 5).foldLeft(Future.successful(Acc(initialCtx, List.empty))) {
          (eventualAcc, _) =>
            for {
              acc <- eventualAcc
              waitForCompletions = new WaitForNCompletions(numberOfCommands)
              _ = acc.ctx.commandCompletionService.completionStream(
                CompletionStreamRequest(
                  scalaz.Tag.unwrap(acc.ctx.ledgerId),
                  M.applicationId,
                  Seq(operator),
                  Some(M.ledgerBegin)
                ),
                waitForCompletions)
              reqs = Vector.fill(numberOfCommands)(
                dummyCommands(acc.ctx.ledgerId, UUID.randomUUID.toString))
              _ <- Future.sequence(reqs.map(acc.ctx.commandSubmissionService.submit))
              _ <- waitForCompletions.result
              start = System.nanoTime()
              nCtx <- acc.ctx.reset()
              end = System.nanoTime()
              times = acc.times :+ Duration.fromNanos(end - start).toMillis
            } yield Acc(nCtx, times)
        }

        resets.flatMap { acc =>
          every(acc.times) should be <= 5000L
        }
      }

      "remove contracts from ACS after reset" in allFixtures { ctx =>
        val req = dummyCommands(ctx.ledgerId, "commandId1")
        for {
          _ <- ctx.commandService.submitAndWait(SubmitAndWaitRequest(commands = req.commands))
          snapshot <- ctx.acsClient.getActiveContracts(allTemplatesForParty).runWith(Sink.seq)
          _ = {
            val responses = snapshot.init // last response is just ledger offset
            val events = responses.flatMap(extractEvents)
            events.size shouldBe 3
          }
          newContext <- ctx.reset()
          newSnapshot <- newContext.acsClient
            .getActiveContracts(allTemplatesForParty)
            .runWith(Sink.seq)
        } yield {
          newSnapshot.size shouldBe 1
          val newEvents = newSnapshot.flatMap(extractEvents)
          newEvents.size shouldBe 0
        }
      }

    }
  }
}
