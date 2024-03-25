// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.WorkflowId
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  HasCloseContext,
  PromiseUnlessShutdownFactory,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PingServiceTest.RequestResponse
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.ledger.api.client.CommandResult
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.{DefaultTestIdentities, DomainId, PartyId}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, Spanning, TraceContext}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import com.google.rpc.status.Status
import io.opentelemetry.api.trace.Tracer
import org.scalatest.Assertion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.chaining.scalaUtilChainingOps

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class PingServiceTest extends BaseTestWordSpec with HasExecutionContext {

  private lazy val aliceId = PartyId.tryFromProtoPrimitive("alice::default")
  private lazy val bobId = PartyId.tryFromProtoPrimitive("bob::default")
  private lazy val charlieId = PartyId.tryFromProtoPrimitive("charlie::default")
  private lazy val workflowId = WorkflowId("workflowId")

  private class TestService(override val clock: SimClock)
      extends PingService.Impl
      with AdminWorkflowService
      with NamedLogging
      with FlagCloseable
      with PromiseUnlessShutdownFactory
      with HasCloseContext
      with Spanning {

    val commandResult = new AtomicReference[RequestResponse](RequestResponse(Promise(), Promise()))

    def getIdCheckActionAndRespondSubmission(
        expectedAction: String,
        result: CommandResult,
    ): Future[String] = {
      val current = commandResult.getAndSet(RequestResponse(Promise(), Promise()))
      current.response.success(result)
      current.requestIdAndAction.future.map { case (id, action) =>
        action shouldBe expectedAction
        id
      }
    }

    override protected def adminPartyId: PartyId = aliceId

    override protected def maxLevelSupported: NonNegativeInt = NonNegativeInt.tryCreate(10)

    override protected def pingDeduplicationDuration: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.tryOfMinutes(5)

    override protected implicit def ec: ExecutionContext = parallelExecutionContext

    override protected def isActive: Boolean = true

    override protected def futureSupervisor: FutureSupervisor =
      PingServiceTest.this.futureSupervisor

    override protected implicit def tracer: Tracer = NoReportingTracerProvider.tracer

    override protected def submitRetryingOnErrors(
        id: PingId,
        action: String,
        cmds: Seq[Command],
        domainId: Option[DomainId],
        workflowId: Option[WorkflowId],
        deduplicationDuration: NonNegativeFiniteDuration,
        timeout: NonNegativeFiniteDuration,
    )(implicit traceContext: TraceContext): Future[CommandResult] = {
      val command = commandResult.get()
      command.requestIdAndAction.success((id, action))
      command.response.future
    }
    override protected def loggerFactory: NamedLoggerFactory = PingServiceTest.this.loggerFactory
    override protected def timeouts: ProcessingTimeout = PingServiceTest.this.timeouts

    override protected def maxBongDuration: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.tryOfMinutes(10)

    override protected def pingResponseTimeout: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.tryOfMinutes(10)
  }

  private class TestFixture() {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val service = new TestService(clock)
    val pingTimeout = NonNegativeFiniteDuration.tryOfMillis(5000)

    def archived(cid: String): Unit = {
      service.processArchivedEvents(Seq(cid))
    }

    def pingCreated(cid: String, id: String, initiator: PartyId, responder: PartyId): Unit = {
      logger.debug(s"Responding to ping ${id}")
      val contract = new M.ping.Ping.Contract(
        new M.ping.Ping.ContractId(cid),
        new M.ping.Ping(
          id,
          initiator.toProtoPrimitive,
          responder.toProtoPrimitive,
        ),
        Set.empty[String].asJava,
        Set.empty[String].asJava,
      )
      service.processCreatedContracts(
        Seq(
          service.pingCreated(
            PingService
              .TxContext(DefaultTestIdentities.domainId, workflowId, effectiveAt = clock.now),
            contract,
          )
        )
      )
    }

    def bongCreated(cid: String, id: String, initiator: PartyId, responder: PartyId): Unit = {
      logger.debug(s"Responding with bong ${id}")
      val contract = new M.bong.Bong.Contract(
        new M.bong.Bong.ContractId(cid),
        new M.bong.Bong(
          id,
          initiator.toProtoPrimitive,
          List().asJava,
          responder.toProtoPrimitive,
          List().asJava,
          clock.now.plusSeconds(15).toInstant,
        ),
        Set.empty[String].asJava,
        Set.empty[String].asJava,
      )
      service.processCreatedContracts(
        Seq(
          service.bongCreated(
            PingService
              .TxContext(DefaultTestIdentities.domainId, workflowId, effectiveAt = clock.now),
            contract,
          )
        )
      )
    }

  }

  private def runTest(test: TestFixture => Future[Assertion])(
      expectedWarningMessages: Seq[String]
  ): Assertion = {
    val fixture = new TestFixture()
    val result = test(fixture)
    result.futureValue.tap { _ =>
      fixture.service.close()
    }
  }

  "PingServiceTest" should {

    "initiates a ping correctly and reports it as completed if responded" in {
      runTest { fixture =>
        import fixture.*
        val resultF = service.ping(targetParties = Set(bobId), validators = Set(), pingTimeout)
        for {
          pingId <- service.getIdCheckActionAndRespondSubmission(
            "ping",
            CommandResult.Success("tx1"),
          )
          _ = {
            pingCreated("123", pingId, aliceId, bobId)
            archived("123")
          }
          result <- resultF
        } yield {
          result shouldBe a[PingService.Success]
        }
      }(Seq.empty)
    }
  }
  "responds properly to a ping initiated by someone else" in {
    runTest { fixture =>
      import fixture.*
      val id = "abc"
      pingCreated("123", id, bobId, aliceId)
      for {
        id2 <- service.getIdCheckActionAndRespondSubmission(
          "ping-response",
          CommandResult.Success("tx1"),
        )
      } yield {
        archived("123")
        id shouldBe id2
      }
    }(Seq.empty)
  }

  "initiates a bong correctly" in {
    runTest { fixture =>
      import fixture.*
      val resultF = {
        // use two target parties to trigger the initiation of a bong
        service.ping(targetParties = Set(bobId, charlieId), validators = Set(), pingTimeout)
      }
      for {
        id <- service.getIdCheckActionAndRespondSubmission(
          "bong-proposal",
          CommandResult.Success("tx1"),
        )
        _ = {
          bongCreated("123", id, aliceId, bobId)
          archived("123")
        }
        result <- resultF
      } yield {
        result shouldBe a[PingService.Success]
      }
    }(Seq.empty)
  }

  "fails ping if submission fails" in {
    runTest { fixture =>
      import fixture.*
      val resultF = service.ping(targetParties = Set(bobId), validators = Set(), pingTimeout)
      for {
        _ <- service.getIdCheckActionAndRespondSubmission(
          "ping",
          CommandResult.TimeoutReached("123", Status()),
        )
        result <- resultF
      } yield {
        result shouldBe a[PingService.Failure]
      }
    }(Seq.empty)
  }

  "timeout ping if we don't observe the contract in time" in {
    runTest { fixture =>
      import fixture.*
      val resultF = service.ping(targetParties = Set(bobId), validators = Set(), pingTimeout)
      for {
        _ <- service.getIdCheckActionAndRespondSubmission(
          "ping",
          CommandResult.Success("tx1"),
        )
        _ = {
          clock.advance(pingTimeout.duration.plusSeconds(3))
        }
        result <- resultF
      } yield {
        result match {
          case PingService.Success(_, _) => fail("should not succeed")
          case PingService.Failure(reason) =>
            reason should include("Timeout: We were unable to create the ping contract")
        }
      }
    }(Seq.empty)
  }

  "timeout ping if we don't receive a response" in {
    runTest { fixture =>
      import fixture.*
      val resultF = service.ping(targetParties = Set(bobId), validators = Set(), pingTimeout)
      for {
        pingId <- service.getIdCheckActionAndRespondSubmission(
          "ping",
          CommandResult.Success("tx1"),
        )
        _ = {
          pingCreated("123", pingId, aliceId, bobId)
          clock.advance(pingTimeout.duration.plusSeconds(3))
        }
        result <- resultF
      } yield {
        archived("123") // late archival doesn't matter
        result match {
          case PingService.Success(_, _) => fail("should not succeed")
          case PingService.Failure(reason) =>
            reason should include(
              "Timeout: We were able to create the ping contract, but responder did not respond in time"
            )
        }
      }
    }(Seq.empty)
  }

}

object PingServiceTest {

  private final case class RequestResponse(
      requestIdAndAction: Promise[(String, String)],
      response: Promise[CommandResult],
  )

}
