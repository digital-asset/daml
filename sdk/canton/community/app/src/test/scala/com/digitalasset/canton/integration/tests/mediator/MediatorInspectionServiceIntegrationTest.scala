// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.mediator

import com.digitalasset.canton.BigDecimalImplicits.IntToBigDecimal
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.mediator.admin.v30.VerdictResult.{
  VERDICT_RESULT_ACCEPTED,
  VERDICT_RESULT_REJECTED,
}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.synchronizer.sequencer.SendDecision.{HoldBack, Process}
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicy,
}

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue}
import scala.concurrent.{Future, Promise}

sealed trait MediatorInspectionServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  // helper methods to access mediator internals
  private def mediator(implicit env: TestConsoleEnvironment) =
    env.mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator
  private def mediatorState(implicit env: TestConsoleEnvironment) =
    mediator.state
  private def numVerdictsInStore()(implicit env: TestConsoleEnvironment): Long =
    mediatorState.finalizedResponseStore.count().futureValueUS
  private def highestRecordTime()(implicit environment: TestConsoleEnvironment) =
    mediatorState.finalizedResponseStore
      .highestRecordTime()
      .futureValueUS
      .value
  private def mediatorPrehead()(implicit
      env: TestConsoleEnvironment
  ): SequencerCounterCursorPrehead =
    mediator.sequencerCounterTrackerStore.preheadSequencerCounter.futureValueUS.value

  // blocking queue used to hold back confirmation responses with the programmable sequencer
  private val confirmationResponses = new LinkedBlockingQueue[Promise[Unit]]()
  private def nextResponsePromise(): Promise[Unit] = confirmationResponses.take()
  private def allowNextResponse(): Unit = nextResponsePromise().success(())

  private def holdBackConfirmationResponses()(implicit env: TestConsoleEnvironment): Unit = {
    assert(
      confirmationResponses.isEmpty,
      "There were still some leftover responses unhandled from a previous test case",
    )
    getProgrammableSequencer(env.sequencer1.name).setPolicy_("hold back confirmation responses")(
      SendPolicy.processTimeProofs_ { submissionRequest =>
        if (ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest)) {
          val p = Promise[Unit]()
          confirmationResponses.add(p)
          HoldBack(p.future)
        } else {
          Process
        }
      }
    )
  }

  "Mediator" should {
    "connect participant and upload examples dar" in { implicit env =>
      import env.*
      clue("participant1 connects to sequencer1") {
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant1.dars.upload(CantonExamplesPath)
      }
    }

    "detect timeouts on any sequenced time and properly serve verdicts" in { implicit env =>
      import env.*

      val originalConfirmationResponseTimeout = sequencer1.topology.synchronizer_parameters
        .get_dynamic_synchronizer_parameters(daId)
        .confirmationResponseTimeout
      val confirmationResponseTimeout = NonNegativeDuration.ofSeconds(5)
      sequencer1.topology.synchronizer_parameters.propose_update(
        daId,
        _.update(confirmationResponseTimeout =
          confirmationResponseTimeout.asNonNegativeFiniteApproximation
        ),
      )

      // we will generate a bunch of requests that will run into a timeout
      val nrRequests = 50
      val droppedMessages = new CountDownLatch(nrRequests)

      val s1 = getProgrammableSequencer(sequencer1.name)
      s1.setPolicy_("drop confirmation responses")(SendPolicy.processTimeProofs_ {
        submissionRequest =>
          // drop all confirmation responses to allow us to purposefully run into timeouts
          if (ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest)) {
            droppedMessages.countDown()
            SendDecision.Drop
          } else Process
      })

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          // generate nrRequests concurrent requests
          val requests = Future.sequence(List.fill(nrRequests)(submitSingleCreateAsync().failed))

          // subscribe to verdicts from the beginning and wait for nrRequests verdicts in a future
          val verdicts =
            Future(mediator1.inspection.verdicts(CantonTimestamp.MinValue, nrRequests))

          // wait for nrRequests dropped responses
          droppedMessages.await()

          mediator1.testing.await_synchronizer_time(
            environment.clock.now + (confirmationResponseTimeout * 2).toInternal,
            confirmationResponseTimeout * 3,
          )

          // the requests should all be unsuccessful
          requests.futureValue should have size nrRequests.toLong
          forAll(requests.futureValue)(_ shouldBe a[CommandFailure])

          // there should be nrRequests verdicts with rejection
          verdicts.futureValue should have size nrRequests.toLong
          forAll(verdicts.futureValue)(_.verdict shouldBe VERDICT_RESULT_REJECTED)
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq.empty,
          mayContain = Seq(entry =>
            entry.message should (include regex "Response message for request.*timed out at".r or include(
              "Rejected transaction due to a participant determined timeout"
            ) or include("Sequencing result message timed out")
              or include("Request failed for participant1"))
          ),
        ),
      )

      sequencer1.topology.synchronizer_parameters.propose_update(
        daId,
        _.update(confirmationResponseTimeout = originalConfirmationResponseTimeout),
      )
    }

    "emit verdicts in record time order" in { implicit env =>
      import env.*

      holdBackConfirmationResponses()

      var nextFromTimestamp = highestRecordTime()

      nextFromTimestamp = clue("single IOU create") {
        val recordTimeF = submitSingleCreateAsync()
        // allow the confirmation
        allowNextResponse()

        val recordTime = recordTimeF.futureValue
        val verdict = mediator1.inspection.verdicts(nextFromTimestamp, 1).loneElement

        verdict.verdict shouldBe VERDICT_RESULT_ACCEPTED
        verdict.recordTime.value shouldBe recordTime.toProtoTimestamp

        recordTime
      }

      nextFromTimestamp = clue("finalize request 1 after request 2") {
        val numVerdictsAtStart = numVerdictsInStore()
        val recordTime1F = submitSingleCreateAsync()
        // get hold of the response for request1
        val response1P = nextResponsePromise()

        val recordTime2F = submitSingleCreateAsync()
        // allow response 2
        allowNextResponse()

        // wait for the next verdict to be stored.
        // this serves as a barrier between response2P.success() and response1P.success(), to avoid flakes
        eventually() {
          numVerdictsInStore() should be > numVerdictsAtStart
        }

        // allow response 1
        response1P.success(())

        val recordTime1 = recordTime1F.futureValue
        val recordTime2 = recordTime2F.futureValue

        val verdicts = mediator1.inspection.verdicts(nextFromTimestamp, 2)
        val (verdict1, verdict2) = (verdicts.head, verdicts(1))
        verdict1.recordTime.value shouldBe recordTime1.toProtoTimestamp
        verdict2.recordTime.value shouldBe recordTime2.toProtoTimestamp

        verdict1.finalizationTime.value should be > verdict2.finalizationTime.value

        recordTime2
      }

      //    t1 t2 t3 t4 t5 t6
      //   ──┼──┼──┼──┼──┼──┼──►
      //    r1 r2 v1 r3 v3 v2
      nextFromTimestamp = clue("interleaved requests and verdicts") {
        val recordTime1F = submitSingleCreateAsync()
        // get hold of the response for request1
        val response1P = nextResponsePromise()

        val recordTime2F = submitSingleCreateAsync()
        // get hold of the response for request1
        val response2P = nextResponsePromise()

        // allow response 1
        response1P.success(())
        // now the first transaction should get emitted
        val recordTime1 = recordTime1F.futureValue
        // also the corresponding verdict
        mediator1.inspection
          .verdicts(nextFromTimestamp, 1)
          .loneElement
          .recordTime
          .value shouldBe recordTime1.toProtoTimestamp

        val numVerdictsBeforeResponse3 = numVerdictsInStore()
        val recordTime3F = submitSingleCreateAsync()
        // allow response 3
        allowNextResponse()

        // wait for the next verdict to be stored.
        // this serves as a barrier between response2P.success() and response1P.success(),
        // to assure the sequencing order of the responses
        eventually() {
          numVerdictsInStore() should be > numVerdictsBeforeResponse3
        }

        // allow response 2
        response2P.success(())

        val recordTime2 = recordTime2F.futureValue
        val recordTime3 = recordTime3F.futureValue

        val verdicts = mediator1.inspection.verdicts(nextFromTimestamp, 3)
        val (verdict1, verdict2, verdict3) = (verdicts.head, verdicts(1), verdicts(2))
        verdict1.recordTime.value shouldBe recordTime1.toProtoTimestamp
        verdict2.recordTime.value shouldBe recordTime2.toProtoTimestamp
        verdict3.recordTime.value shouldBe recordTime3.toProtoTimestamp

        // check the order of record times
        verdict1.recordTime.value should be < verdict2.recordTime.value
        verdict2.recordTime.value should be < verdict3.recordTime.value

        // check the expected order of finalization times
        verdict1.finalizationTime.value should be < verdict3.finalizationTime.value
        verdict3.finalizationTime.value should be < verdict2.finalizationTime.value

        recordTime3
      }

      // restart the mediator
      mediator1.stop()
      mediator1.start()

      // now check the mediator state after initialization
      mediatorState.youngestFinalizedRequest.get shouldBe nextFromTimestamp
      mediatorState.recordOrderTimeAwaiter.getCurrentKnownTime() shouldBe nextFromTimestamp
      highestRecordTime() shouldBe nextFromTimestamp
    }

    "reinitialize mediator state" in { implicit env =>
      import env.*

      holdBackConfirmationResponses()

      // submit a transaction so that we have predictable state before we start the actual test
      val preTestState = submitSingleCreateAsync()
      allowNextResponse()
      preTestState.futureValue.discard

      // Given the following timeline:
      //  t1 t2 t3 t4
      // ──┼──┼──┼──┼─►
      //  r1 r2 v2 v1
      // 1. Get request 2 finalized and therefore r2/t2 as highest record time into the finalized store
      // 2. replay from t1

      val preheadAtStart = mediatorPrehead()
      val highestRecordTimeAtStart = highestRecordTime()

      // submit request 1
      val recordTime1F = submitSingleCreateAsync()
      // hold back the response for request 1
      val response1P = nextResponsePromise()

      // submit request 2
      val recordTime2F = submitSingleCreateAsync()
      // allow response 2 and wait for request 2 to complete
      allowNextResponse()
      eventually() {
        val recordTimeOfRequest2 = highestRecordTime()
        // wait to see a new highest record time in the finalized response store
        recordTimeOfRequest2 should be > highestRecordTimeAtStart
        // and wait that the mediator has fully processed the send-receipt for the verdict,
        // to avoid a race between updating the mediator's prehead sequencer counter
        // and rewinding it here in the test for the sake of testing the re-initialization procedure
        val verdictOfRequest2 = mediatorState.finalizedResponseStore
          .fetch(RequestId(recordTimeOfRequest2))
          .value
          .futureValueUS
          .value
        mediatorPrehead().timestamp should be > verdictOfRequest2.finalizationTime
      }

      // now set the mediator's prehead to the prehead of request 1.
      // this should trigger a replay of request 1 and therefore the verdict for request 2 should not be emitted
      // and we should also see a a verdict of request 1.
      mediator.sequencerCounterTrackerStore
        .rewindPreheadSequencerCounter(Some(preheadAtStart))
        .futureValueUS

      clue("stopping mediator")(mediator1.stop())
      clue("starting mediator")(mediator1.start())

      // request verdicts but with a low timeout, because we expect not to receive any verdicts yet,
      // since request 1 hasn't been finalized yet
      val verdictsBeforeRequest1Finalization = mediator1.inspection.verdicts(
        highestRecordTimeAtStart,
        1,
        timeout = NonNegativeDuration.ofSeconds(2),
      )
      verdictsBeforeRequest1Finalization shouldBe empty

      // now sequence response 1 and wait for request 1 and 2 to complete
      response1P.success(())
      val recordTime1 = recordTime1F.futureValue
      val recordTime2 = recordTime2F.futureValue

      val verdicts = mediator1.inspection.verdicts(highestRecordTimeAtStart, 2)
      verdicts should have size 2
      verdicts.map(_.recordTime.value) shouldBe Seq(recordTime1, recordTime2).map(
        _.toProtoTimestamp
      )
      forAll(verdicts)(_.verdict shouldBe VERDICT_RESULT_ACCEPTED)
    }
  }

  /** Asynchronously submit a transaction that only involves participant1 for simpler
    * response/verdict handling.
    *
    * @return
    *   The record time in a `Future` after the processing of the transaction completed.
    */
  private def submitSingleCreateAsync()(implicit
      env: TestConsoleEnvironment
  ): Future[CantonTimestamp] = {
    import env.*
    val iouCommand =
      new Iou(
        participant1.adminParty.toProtoPrimitive,
        participant1.adminParty.toProtoPrimitive,
        new Amount(1.toBigDecimal, "stones"),
        new java.util.ArrayList(),
      ).create.commands.loneElement

    Future(
      CantonTimestamp
        .fromInstant(
          participant1.ledger_api.javaapi.commands
            .submit(
              actAs = Seq(participant1.adminParty),
              commands = Seq(iouCommand),
            )
            .getRecordTime
        )
        .value
    )
  }

}

class MediatorInspectionServiceIntegrationTestPostgres
    extends MediatorInspectionServiceIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
