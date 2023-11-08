// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.ledger.api.refinements.ApiTypes.WorkflowId
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.participant.admin.workflows.java.pingpong as M
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.*

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
class PingServiceTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach {

  var scheduler: ScheduledExecutorService = _
  var service: PingService = _

  override def afterEach(): Unit = {
    Option(service).foreach(_.close())
    Option(scheduler).foreach(_.shutdown())
  }

  "PingServiceTest" should {

    val clock = new SimClock(loggerFactory = loggerFactory)
    val alicePartId = ParticipantId("alice")
    val aliceId = alicePartId.adminParty
    val alice = aliceId.toParty
    val bobId = "bob::default"
    val charlieId = "Charlie::default"

    def setupTest(recipients: Set[String], pingTimeoutMs: Long, gracePeriod: Long, tag: Int) = {
      val submitter = new MockLedgerAcs(logger, alice)
      scheduler = Threading.singleThreadScheduledExecutor("ping-service-tests", noTracingLogger)
      service = new PingService(
        submitter,
        aliceId,
        maxLevelSupported = 10,
        DefaultProcessingTimeouts.testing,
        pingDeduplicationTime = NonNegativeFiniteDuration.tryOfMinutes(5),
        isActive = true,
        None,
        futureSupervisor,
        loggerFactory.append("tag", tag.toString),
        clock,
      )(parallelExecutionContext, scheduler)
      val res = service.ping(recipients, Set(), pingTimeoutMs, gracePeriod)
      val cmd = Await.result(submitter.lastCommand, 10.seconds)
      val id = for {
        createCmd <- cmd.command.create
        record <- createCmd.getCreateArguments.fields.find(_.label == "id")
        value <- record.value
      } yield value.getText
      (service, id.value, res)
    }

    def verifySuccess(pingRes: Future[PingService.Result]) = {
      val pinged = Await.result(pingRes, 10.seconds)
      pinged match {
        case PingService.Success(_, _) => ()
        case PingService.Failure => fail("ping test failed")
      }
    }

    def verifyFailure(pingRes: Future[PingService.Result]) = {
      val pinged = Await.result(pingRes, 10.seconds)
      pinged match {
        case PingService.Success(_, _) => fail("ping test should have failed but didnt")
        case PingService.Failure => ()
      }
    }

    def respond(id: String, responder: String, service: PingService, observers: Set[String]) =
      service.processPongs(
        Seq(
          new M.Pong.Contract(
            new M.Pong.ContractId("12345"),
            new M.Pong(
              id,
              aliceId.toProtoPrimitive,
              List().asJava,
              responder,
              observers.toSeq.asJava,
            ),
            java.util.Optional.empty,
            observers.asJava,
            observers.asJava,
          )
        ),
        WorkflowId("workflowId"),
      )

    "happy ping path of single ping is reported correctly" in {
      val recipients = Set(bobId)
      val (service, id, pingRes) = setupTest(recipients, 5000, 5, 1)
      respond(id, bobId, service, recipients)
      verifySuccess(pingRes)
    }

    "happy bong is reported correctly" in {
      val recipients = Set(bobId, charlieId)
      val (service, id, pingRes) = setupTest(recipients, 5000, 5, 2)
      respond(id, charlieId, service, recipients)
      verifySuccess(pingRes)
    }

    "ping times out gracefully" in {
      val (_, _, pingRes) = setupTest(Set(bobId, charlieId), 2, 5, 3)
      verifyFailure(pingRes)
    }

    "discovers duplicate pongs correctly" in {
      loggerFactory.suppressWarningsAndErrors {
        val recipients = Set(bobId, charlieId)
        val (service, id, pingRes) = setupTest(recipients, 5000, 2000, 4)
        // we don't really care about the results of these responses
        // but we want to avoid running them concurrently to mirror how transactions are processed (one at a time)
        respond(id, charlieId, service, recipients)
        respond(id, bobId, service, Set(bobId, charlieId))

        verifyFailure(pingRes)
      }
    }
  }
}
