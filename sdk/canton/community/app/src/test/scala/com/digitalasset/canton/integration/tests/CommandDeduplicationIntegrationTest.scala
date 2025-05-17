// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.javaapi.data.Transaction
import com.digitalasset.base
import com.digitalasset.base.error.GrpcStatuses
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{DbConfig, StorageConfig}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalSequencerReference,
  ParticipantReference,
}
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod, Offset}
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.examples.java.cycle as C
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.CommandDeduplicationIntegrationTest.DelayPromises
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.DuplicateCommand
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidDeduplicationPeriodField
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.UnsafeToPrune
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  SubmissionAlreadyInFlight,
  TimeoutError,
}
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, LedgerSubmissionId, config}
import com.digitalasset.daml.lf.data.Ref
import com.google.rpc.error_details.ErrorInfo
import org.scalatest.Assertion

import java.time.Duration
import scala.Ordered.orderingToOrdered
import scala.concurrent.{Future, Promise}
import scala.util.Try

import DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}

trait CommandDeduplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with CommandDeduplicationTestHelpers {

  private lazy val maxDedupDuration: Duration = java.time.Duration.ofHours(1)

  private val reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)

  protected val isTimestampManipulationSupported: Boolean = true

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .withSetup { implicit env =>
        import env.*

        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = reconciliationInterval),
            )
        )
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.dars.upload(CantonExamplesPath)
        participant1.parties.enable("Alice", synchronizer = daName)
        participant1.parties.enable("Alice", synchronizer = acmeName)
      }

  private def checkAccepted(
      completion: Completion,
      submissionId: String,
      // Use a Tuple so that we can use the mnemonic `->` notation
      // The lower bound is inclusive (because deduplication offsets are exclusive) and the upper bound is exclusive
      dedupStartBoundsInclusiveExclusive: (Option[Long], Option[Long]) = None -> None,
  ): Assertion = {
    completion.status.value.code shouldBe com.google.rpc.Code.OK_VALUE
    // Canton always reports an offset
    val dedupOffset = completion.deduplicationPeriod.deduplicationOffset.value
    dedupStartBoundsInclusiveExclusive._1.foreach { lower =>
      dedupOffset should be >= lower
    }
    dedupStartBoundsInclusiveExclusive._2.foreach { upper =>
      dedupOffset should be < upper
    }
    // If we had omitted the submission ID, then the ledger may generate one on its own
    if (submissionId.nonEmpty)
      completion.submissionId shouldBe submissionId
    else succeed
  }

  "deduplicate accepted commands over the command service" in
    WithContext { (alice, simClock) => implicit env =>
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-Command-Service",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val completionEnd = participant1.ledger_api.state.end()
      val commandId = "deduplicate-accept-command-service"
      val dedupPeriod1 = DeduplicationDuration(java.time.Duration.ofHours(1))
      val submissionId1 = "first-submission"

      def submit(submissionId: String, dedupPeriod: DeduplicationPeriod): Transaction =
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId), // Run on DA synchronizer
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = dedupPeriod.some,
        )

      // Obtain an offset before the first submission
      val offset0 = participant1.ledger_api.state.end()

      logger.debug("Submitting for the first time")
      submit(submissionId1, dedupPeriod1)
      val (completion1, offset1) =
        findCompletionFor(participant1, alice, completionEnd, commandId, submissionId1)
      checkAccepted(completion1, submissionId1)

      def submitAlreadyExists(
          submissionId: String,
          dedupPeriod: DeduplicationPeriod,
          alreadyExistsOffset: Long,
      ) = {
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          submit(submissionId, dedupPeriod),
          _.shouldBeCantonErrorCode(ConsistencyErrors.DuplicateCommand),
        )
        // We don't get the gRPC error on the Canton console. Go get it from the completion stream
        val (completion, offset) =
          findCompletionFor(participant1, alice, alreadyExistsOffset, commandId, submissionId)
        checkIsAlreadyExists(completion, submissionId, alreadyExistsOffset)
        offset
      }

      logger.debug("Resubmit with long deduplication duration")
      val submissionId2 = "second-submission-duration"
      submitAlreadyExists(submissionId2, dedupPeriod1, offset1)

      logger.debug("Resubmit with participant begin offset")
      val submissionIdParticipantBegin = "participant-begin-offset"
      val dedupPeriodParticipantBegin = DeduplicationOffset(None)
      submitAlreadyExists(submissionIdParticipantBegin, dedupPeriodParticipantBegin, offset1)

      logger.debug("Resubmit with early deduplication offset")
      val submissionId3 = "third-submission-offset"
      val dedupPeriod3 = DeduplicationOffset(Some(Offset.tryFromLong(offset0)))
      val offset3 = submitAlreadyExists(submissionId3, dedupPeriod3, offset1)

      logger.debug("Resubmit with recent deduplication offset")
      val submissionId4 = "fourth-submission-offset"
      val dedupPeriod4 = DeduplicationOffset(Some(Offset.tryFromLong(offset1)))
      submit(submissionId4, dedupPeriod4)
      val (completion4, offset4) =
        findCompletionFor(participant1, alice, offset3, commandId, submissionId4)
      checkAccepted(completion4, submissionId4, offset1.some -> offset4.some)

      logger.debug("Resubmit with short deduplication duration")
      simClock.advance(java.time.Duration.ofSeconds(600))
      val submissionId5 = "fifth-submission"
      submit(submissionId5, DeduplicationDuration(java.time.Duration.ofSeconds(599)))
      val (completion5, _) =
        findCompletionFor(participant1, alice, offset4, commandId, submissionId5)
      checkAccepted(completion5, submissionId5, offset4.some -> None)
    }

  "deduplicate accepted commands over the command submission service" in
    WithContext { (alice, simClock) => implicit env =>
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-Command-Submission-Service",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val completionEnd = participant1.ledger_api.state.end()
      val commandId = "deduplicate-accept-command-submission-service"
      val dedupPeriod1 = DeduplicationDuration(java.time.Duration.ofHours(1))
      val submissionId1 = "first-submission"

      def submitAsync(submissionId: String, dedupPeriod: DeduplicationPeriod): Unit =
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId), // Run on DA synchronizer
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = dedupPeriod.some,
        )

      // Obtain an offset before the first submission
      val offset0 = participant1.ledger_api.state.end()

      logger.debug("Submitting for the first time")
      submitAsync(submissionId1, dedupPeriod1)
      val (completion1, offset1) =
        findCompletionFor(participant1, alice, completionEnd, commandId, submissionId1)
      checkAccepted(completion1, submissionId1)

      logger.debug("Resubmit with long deduplication duration")
      val submissionId2 = "second-submission-duration"

      submitAsync(submissionId2, dedupPeriod1)
      val (completion2, offset2) =
        findCompletionFor(participant1, alice, offset1, commandId, submissionId2)
      checkIsAlreadyExists(completion2, submissionId2, offset1)

      logger.debug("Resubmit with early deduplication offset")
      val submissionId3 = "third-submission-offset"
      val dedupPeriod3 = DeduplicationOffset(Some(Offset.tryFromLong(offset0)))

      submitAsync(submissionId3, dedupPeriod3)
      val (completion3, offset3) =
        findCompletionFor(participant1, alice, offset2, commandId, submissionId3)
      checkIsAlreadyExists(completion3, submissionId3, offset1)

      logger.debug("Resubmit with recent deduplication offset")
      val submissionId4 = "fourth-submission"
      val dedupPeriod4 = DeduplicationOffset(Some(Offset.tryFromLong(offset1)))
      submitAsync(submissionId4, dedupPeriod4)
      val (completion4, offset4) =
        findCompletionFor(participant1, alice, offset3, commandId, submissionId4)
      checkAccepted(completion4, submissionId4, offset1.some -> offset4.some)

      logger.debug("Resubmit with short deduplication duration")
      simClock.advance(java.time.Duration.ofSeconds(600))
      val submissionId5 = "fifth-submission"
      submitAsync(submissionId5, DeduplicationDuration(java.time.Duration.ofSeconds(599)))
      val (completion5, _) =
        findCompletionFor(participant1, alice, offset4, commandId, submissionId5)
      checkAccepted(completion5, submissionId5, offset4.some -> None)
    }

  "deduplicate across synchronizers" in
    WithContext { (alice, _) => implicit env =>
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-across-Synchronizers",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val completionEnd = participant1.ledger_api.state.end()
      val commandId = "deduplicate-across-synchronizers"
      val submissionId1 = "first-submission"

      def submitAsync(
          submissionId: String,
          dedupPeriod: DeduplicationPeriod,
          synchronizerId: SynchronizerId,
      ): Unit =
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(synchronizerId),
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = dedupPeriod.some,
        )

      // Obtain an offset before the first submission
      val offset0 = participant1.ledger_api.state.end()

      logger.debug("Submit to synchronizer da")
      submitAsync(submissionId1, DeduplicationDuration(java.time.Duration.ofMillis(1)), daId)
      val (completion1, offset1) =
        findCompletionFor(participant1, alice, completionEnd, commandId, submissionId1)
      checkAccepted(completion1, submissionId1)

      logger.debug("Resubmit on synchronizer acme with deduplication duration")
      val submissionId2 = "second-submission-duration"

      submitAsync(submissionId2, DeduplicationDuration(java.time.Duration.ofHours(1)), acmeId)
      val (completion2, offset2) =
        findCompletionFor(participant1, alice, offset1, commandId, submissionId2)
      checkIsAlreadyExists(completion2, submissionId2, offset1)

      logger.debug("Resubmit on synchronizer acme with deduplication offset")
      val submissionId3 = "third-submission-offset"
      val dedupPeriod3 = DeduplicationOffset(Some(Offset.tryFromLong(offset0)))
      submitAsync(submissionId3, dedupPeriod3, acmeId)
      val (completion3, _offset3) =
        findCompletionFor(participant1, alice, offset2, commandId, submissionId3)
      checkIsAlreadyExists(completion3, submissionId3, offset1)

    }

  "do not deduplicate across submitters" in
    WithContext { (alice, _) => implicit env =>
      import env.*

      val bob = participant1.parties.enable("Bob", synchronizer = daName)

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-not-across-submitters",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val commandId = "do-not-deduplicate-across-submitters"

      def submit(actAs: Seq[PartyId]): Unit =
        participant1.ledger_api.javaapi.commands.submit(
          actAs,
          Seq(createCycleContract),
          synchronizerId = Some(daId), // Run on DA synchronizer
          commandId = commandId,
        )

      submit(Seq(alice))
      submit(Seq(alice, bob))
    }

  private def sequencerPolicyDelayRequestAndResponse(
      participantId: ParticipantId,
      baseSequencer: LocalSequencerReference,
  ): DelayPromises = {
    val sequencer = getProgrammableSequencer(baseSequencer.name)
    val requestRelease = Promise[Unit]()
    val requestReceived = Promise[Unit]()
    val responseRelease = Promise[Unit]()
    val responseReceived = Promise[Unit]()
    // We delay all messages from participant1 so that we can send another submission before and after sequencing
    sequencer.setPolicy_(s"delay request and response from $participantId") { submissionRequest =>
      if (submissionRequest.sender == participantId) {
        if (submissionRequest.isConfirmationRequest) {
          requestReceived.trySuccess(())
          SendDecision.HoldBack(requestRelease.future)
        } else {
          responseReceived.trySuccess(())
          SendDecision.HoldBack(responseRelease.future)
        }
      } else SendDecision.Process
    }
    DelayPromises(requestReceived.future, requestRelease, responseReceived.future, responseRelease)
  }

  private def submissionAlreadyInFlightError(
      commandId: String,
      submitter: PartyId,
      submissionId: String,
      synchronizerId: PhysicalSynchronizerId,
  )(logEntry: LogEntry): Assertion =
    if (submissionId.nonEmpty) {
      val changeId = ChangeId(
        Ref.UserId.assertFromString(LedgerApiCommands.defaultUserId),
        Ref.CommandId.assertFromString(commandId),
        Set(submitter.toLf),
      )
      val expectedError = SubmissionAlreadyInFlight(
        changeId,
        LedgerSubmissionId.assertFromString(submissionId).some,
        synchronizerId,
      )
      logEntry.message should include(expectedError.code.id)
      // TODO(#5990) We seem to be missing the context information in the log entry!
      // logEntry.shouldBeCantonError(expectedError)
    } else {
      // If the submission ID is unknown, then we can't compare the full error.
      logEntry.message should include(
        ConsistencyErrors.SubmissionAlreadyInFlight.id
      )
    }

  "reject multiple in-flight submissions via command submission service" in
    WithContext { (alice, simClock) => implicit env =>
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-multiple-in-flight-command-submission-service",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val commandId = "reject-multiple-in-flight-command-submission-service"

      def submitAsync(submissionId: String): Unit =
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId),
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = DeduplicationDuration(java.time.Duration.ofNanos(1000)).some,
        )

      val DelayPromises(requestReceived, requestRelease, responseReceived, responseRelease) =
        sequencerPolicyDelayRequestAndResponse(participant1.id, sequencer1)

      val completionEnd = participant1.ledger_api.state.end()

      val submissionId1 = "first-submission"
      submitAsync(submissionId1)
      logger.debug("Wait until request submission has reached the sequencer")
      requestReceived.futureValue

      simClock.advance(java.time.Duration.ofSeconds(1))

      val submissionId2 = "resubmission-before-sequencing"
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submitAsync(submissionId2),
        submissionAlreadyInFlightError(
          commandId,
          alice,
          submissionId2,
          initializedSynchronizers(daName).physicalSynchronizerId,
        ),
      )

      logger.debug(
        "Releasing the first request's submission, waiting for the response to be queued"
      )
      requestRelease.success(())
      responseReceived.futureValue

      simClock.advance(java.time.Duration.ofSeconds(1))

      val submissionId3 = "resubmission-after-sequencing"
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submitAsync(submissionId3),
        submissionAlreadyInFlightError(
          commandId,
          alice,
          submissionId3,
          initializedSynchronizers(daName).physicalSynchronizerId,
        ),
      )

      logger.debug("Releasing the first request's response")
      responseRelease.success(())
      val sequencer = getProgrammableSequencer(sequencer1.name) // For DA synchronizer
      sequencer.resetPolicy()

      val (completion1, offset1) =
        findCompletionFor(participant1, alice, completionEnd, commandId, submissionId1)
      checkAccepted(completion1, submissionId1)
    }

  "reject multiple in-flight submissions via command service" in
    WithContext { (alice, simClock) => implicit env =>
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-multiple-in-flight-command-service",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val commandId = "reject-multiple-in-flight-command-service"

      // We now check that the command service can track several submissions for the same change ID

      def submit(submissionId: String): Unit =
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId),
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = DeduplicationDuration(java.time.Duration.ofNanos(1000)).some,
        )

      val DelayPromises(requestReceived, requestRelease, responseReceived, responseRelease) =
        sequencerPolicyDelayRequestAndResponse(participant1.id, sequencer1) // For DA synchronizer

      val completionEnd = participant1.ledger_api.state.end()

      val submissionId1 = "first-submission"
      val submit1F = Future {
        submit(submissionId1)
      }
      logger.debug("Wait until request submission has reached the sequencer")
      requestReceived.futureValue

      simClock.advance(java.time.Duration.ofSeconds(1))
      val submissionId2 = "resubmission-before-sequencing"
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submit(submissionId2),
        submissionAlreadyInFlightError(
          commandId,
          alice,
          submissionId2,
          initializedSynchronizers(daName).physicalSynchronizerId,
        ),
      )
      simClock.advance(java.time.Duration.ofSeconds(1))

      logger.debug(
        "Releasing the first request's submission, waiting for the response to be queued"
      )
      requestRelease.success(())
      responseReceived.futureValue

      val submissionId3 = "resubmission-after-sequencing"
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        submit(submissionId3),
        submissionAlreadyInFlightError(
          commandId,
          alice,
          submissionId3,
          initializedSynchronizers(daName).physicalSynchronizerId,
        ),
      )

      logger.debug("Releasing the first request's response")
      responseRelease.success(())
      val sequencer = getProgrammableSequencer(sequencer1.name) // For DA synchronizer
      sequencer.resetPolicy()

      submit1F.futureValue
      val (completion1, offset1) =
        findCompletionFor(participant1, alice, completionEnd, commandId, submissionId1)
      checkAccepted(completion1, submissionId1)
    }

  "do not deduplicate rejected commands" in WithContext { (alice, simClock) => implicit env =>
    // TODO(#17334): verify
    // This test assumes that a protocol message can be delayed by advancing the clock time,
    //  so it isn't supported by the BFT Ordering Sequencer, as BFT time does not necessarily depend on
    //  the current clock time.
    if (isTimestampManipulationSupported) {
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-timeout",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val commandId = "reject-timeout"

      def submitAsync(submissionId: String): Unit =
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId),
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = DeduplicationDuration(maxDedupDuration).some,
        )

      val DelayPromises(requestReceived, requestRelease, responseReceived, responseRelease) =
        sequencerPolicyDelayRequestAndResponse(participant1.id, sequencer1) // For DA synchronizer
      val completionEnd = participant1.ledger_api.state.end()

      val submissionId1 = "timeout-submission"
      submitAsync(submissionId1)
      logger.debug("Wait until request submission has reached the sequencer")
      requestReceived.futureValue

      val tolerance = initializedSynchronizers(daName).synchronizerOwners.headOption
        .getOrElse(fail("synchronizer owners are missing"))
        .topology
        .synchronizer_parameters
        .get_dynamic_synchronizer_parameters(daId)
        .ledgerTimeRecordTimeTolerance
        .asJava
      require(
        tolerance < maxDedupDuration.minusSeconds(2),
        "test must be run with shorter ledger time tolerance",
      )
      simClock.advance(tolerance.plusSeconds(1)) // Advance the time beyond the max sequencing time
      val offset1 = loggerFactory.assertLogsUnordered(
        {
          requestRelease.success(())
          responseRelease.success(()) // We don't care about the response
          val (completion1, offset1) =
            findCompletionFor(participant1, alice, completionEnd, commandId, submissionId1)
          val status1 = completion1.status.value
          status1.code shouldBe com.google.rpc.Code.ABORTED_VALUE
          status1.message should include(TimeoutError.id)
          completion1.submissionId shouldBe submissionId1
          // Since some timeouts don't go through in the in-flight submission tracker,
          // we conservatively don't mark this as a definite answer
          // even though we could in this particular case
          GrpcStatuses.isDefiniteAnswer(status1) shouldBe false

          offset1
        },
        _.warningMessage should include("Submission timed out at"),
      )
      val sequencer = getProgrammableSequencer(sequencer1.name) // For DA synchronizer
      sequencer.resetPolicy()

      val submissionId2 = "successful-resubmission"
      submitAsync(submissionId2)
      val (completion2, offset2) =
        findCompletionFor(participant1, alice, offset1, commandId, submissionId2)
      // Even though we've advance the clock by an hour, the dedup period is much longer, so we expect the reported
      // dedup period to include
      checkAccepted(completion2, submissionId2, None -> offset1.some)
    }
  }

  "do not confuse submissions when submission ID is omitted" in
    WithContext { (alice, _) => implicit env =>
      import env.*
      // submission IDs are optional, so make sure that the command service doesn't get confused if we submit in parallel over the command submission service

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-no-submission-id",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val commandId = "no-submission-id"
      val emptySubmissionId = ""

      val DelayPromises(requestReceived, requestRelease, _responseReceived, responseRelease) =
        sequencerPolicyDelayRequestAndResponse(participant1.id, sequencer1) // For DA synchronizer

      val completionEnd = participant1.ledger_api.state.end()
      logger.debug("Submit first command via the command service")
      val submitF = Future {
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId),
          commandId = commandId,
          submissionId = emptySubmissionId,
          deduplicationPeriod = None,
        )
      }
      requestReceived.futureValue

      logger.debug("Submitting second command via the command submission service")
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(createCycleContract),
          commandId = commandId,
          submissionId = emptySubmissionId,
          deduplicationPeriod = None,
        ),
        submissionAlreadyInFlightError(
          commandId,
          alice,
          emptySubmissionId,
          initializedSynchronizers(daName).physicalSynchronizerId,
        ),
      )

      logger.debug("Releasing the first submission")
      requestRelease.success(())
      responseRelease.success(())

      // The first submission should succeed
      submitF.futureValue
      val (accept, offsetAccept) = findCompletionFor(
        participant1,
        alice,
        completionEnd,
        commandId,
        emptySubmissionId,
      )
      checkAccepted(accept, emptySubmissionId, None -> completionEnd.some)
    }

}

trait CommandDeduplicationTestHelpers { this: BaseTest with HasProgrammableSequencer =>
  protected def WithContext(
      code: (PartyId, SimClock) => TestConsoleEnvironment => Unit
  ): TestConsoleEnvironment => Unit = { implicit env =>
    import env.*

    val alices = participant1.parties.list(filterParty = "Alice", limit = 1)
    val alice = alices.headOption.value.party

    val simClock = env.environment.simClock.value
    code(alice, simClock)(env)
  }

  protected def findCompletionFor(
      participant: ParticipantReference,
      party: PartyId,
      startOffset: Long,
      commandId: String,
      submissionId: String,
  ): (Completion, Long) = {
    logger.debug(
      s"Finding completion for submission $commandId / $submissionId of $party, starting at $startOffset"
    )
    val completions = participant.ledger_api.completions.list(
      party,
      atLeastNumCompletions = 1,
      startOffset,
      filter = completion =>
        completion.commandId == commandId && (if (submissionId.isEmpty) true
                                              else
                                                completion.submissionId == submissionId),
    )
    completions match {
      case Seq(completion) =>
        // The convention is to emit an offset for each event.
        completion -> completion.offset
      case _ => fail(s"Not exactly one completion for $commandId / $submissionId: $completions")
    }
  }

  protected def findErrorInfoField(
      status: com.google.rpc.status.Status,
      fieldName: String,
  ): Seq[String] =
    status.details.mapFilter { any =>
      if (any.is(ErrorInfo.messageCompanion)) {
        Try(any.unpack(ErrorInfo.messageCompanion)).toOption.flatMap(_.metadata.get(fieldName))
      } else None
    }

  protected def findCompletionOffsetInStatus(
      status: com.google.rpc.status.Status
  ): Seq[String] =
    findErrorInfoField(status, base.error.GrpcStatuses.CompletionOffsetKey)

  protected def checkIsAlreadyExists(
      completion: Completion,
      expectedSubmissionId: String,
      existingCompletionOffset: Long,
  ): Assertion = {
    val status = completion.status.value
    status.code shouldBe com.google.rpc.Code.ALREADY_EXISTS_VALUE
    CantonBaseError.isStatusErrorCode(DuplicateCommand, status)
    val earlierCompletionOffset = findCompletionOffsetInStatus(status)
    earlierCompletionOffset shouldBe Seq(existingCompletionOffset.toString)
    // Just a check that we correctly set the definite answer flag.
    base.error.GrpcStatuses.isDefiniteAnswer(status) shouldBe true
    completion.submissionId shouldBe expectedSubmissionId
  }

  protected def checkDedupPeriodTooLong(
      completion: Completion,
      expectedSubmissionId: String,
      expectedEarliestOffset: Long,
  ): Assertion = {
    val status = completion.status.value
    status.code shouldBe com.google.rpc.Code.FAILED_PRECONDITION_VALUE
    CantonBaseError.isStatusErrorCode(InvalidDeduplicationPeriodField, status)
    completion.submissionId shouldBe expectedSubmissionId

    val earliestOffsets = findErrorInfoField(status, LedgerApiErrors.EarliestOffsetMetadataKey)
    earliestOffsets should contain(expectedEarliestOffset.toString)
  }
}

class CommandDeduplicationIntegrationTestInMemory extends CommandDeduplicationIntegrationTest {
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[StorageConfig.Memory](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

class CommandDeduplicationBftOrderingIntegrationTestInMemory
    extends CommandDeduplicationIntegrationTest {

  override protected val isTimestampManipulationSupported: Boolean = false

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

abstract class CommandDeduplicationPruningIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with CommandDeduplicationTestHelpers {
  lazy val maxDedupDuration = java.time.Duration.ofHours(1)

  private val reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .withSetup { implicit env =>
        import env.*

        // Make sure that ACS commitments do not block pruning
        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = reconciliationInterval),
            )
        )

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)

        participant1.parties.enable("Alice")
      }

  "block pruning for the max deduplication duration" in
    WithContext { (alice, simClock) => implicit env =>
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-Pruning",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val commandId1 = "warm-up"
      val commandId2 = "at-max-dedup-boundary"
      val commandId3 = "yet-another-commandId"
      def commandId4DuplicateRejected = commandId2
      def commandId5OutsideDedupBoundaryAccepted = commandId2
      val commandId6DeduplicationPeriodAccepted = "fresh-command-id"
      val commandId7DeduplicationPeriodTooLong = "command-id-too-long-period"

      def submit(
          commandId: String,
          dedupPeriod: DeduplicationPeriod = DeduplicationDuration(java.time.Duration.ofMinutes(1)),
          submissionId: String = "",
      ): Long =
        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(alice),
            Seq(createCycleContract),
            synchronizerId = Some(daId), // Run on DA synchronizer
            commandId = commandId,
            submissionId = submissionId,
            deduplicationPeriod = dedupPeriod.some,
          )
          .getOffset

      def submitAsync(
          commandId: String,
          dedupPeriod: DeduplicationPeriod,
          submissionId: String,
      ): Unit =
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId), // Run on DA synchronizer
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = dedupPeriod.some,
        )

      // send three transactions at t, t+1ms, and t+1ms+max_dedup_duration
      val after1 = submit(commandId1)
      simClock.advance(java.time.Duration.ofMillis(1))
      val before2 = simClock.now
      val after2 = submit(commandId2)
      simClock.advance(maxDedupDuration)
      val after3 = submit(commandId3)

      // We must not prune anything published within the max deduplication duration,
      // so we can prune only up to the first transaction
      val safe1 = eventually() {
        val safe = participant1.pruning.find_safe_offset().value
        safe should be >= after1
        safe
      }
      eventually() {
        val noOutstandingCommitmentsO = participant1.testing.state_inspection
          .noOutstandingCommitmentsTs(daName, CantonTimestamp.MaxValue)
        logger.debug(s"No outstanding commitment at $noOutstandingCommitmentsO")
        noOutstandingCommitmentsO.value should be > before2
      }

      // Test that we get a meaningful error message if we try to prune with a too high offset
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.pruning.prune(after2),
        logEntry => {
          logEntry.errorMessage should include(UnsafeToPrune.id)
          logEntry.errorMessage should include(
            show"due to max deduplication duration of $maxDedupDuration"
          )
          logEntry.errorMessage should include(s"safe_offset=>$safe1")
        },
      )

      participant1.pruning.prune(after1)

      logger.debug("resubmitting second command")
      val submissionId = "resubmission"
      submitAsync(
        commandId4DuplicateRejected,
        DeduplicationDuration(maxDedupDuration),
        submissionId,
      )
      val (completion4, _offset4) =
        findCompletionFor(participant1, alice, after3, commandId2, submissionId)
      checkIsAlreadyExists(completion4, submissionId, after2)

      // After advancing the time a bit, and wait for a timeproof (needed that the participants publication time moves higher) we should be able to prune more and resubmit
      simClock.advance(java.time.Duration.ofMillis(1L))
      participant1.testing.fetch_synchronizer_time(daId.toPhysical)
      val safe2 = eventually() {
        val safe = participant1.pruning.find_safe_offset().value
        safe should be >= after2
        safe
      }
      participant1.pruning.prune(safe2)
      val after5 = submit(
        commandId5OutsideDedupBoundaryAccepted,
        DeduplicationDuration(maxDedupDuration),
      )
      val outsideDedupBoundaryTs =
        valueOrFail(
          participant1.testing.state_inspection
            .lookupPublicationTime(after5)
            .value
            .futureValueUS
        )(s"Failed to locate publication time")

      logger.debug("submit fresh command with long dedup duration")
      simClock.advanceTo(
        before2.plus(
          maxDedupDuration
            .multipliedBy(2)
            // +1 millisecond to account for the millisecond added above between command submissions 4 and 5:
            .plusMillis(1)
        )
      )
      val after6 = submit(
        commandId6DeduplicationPeriodAccepted,
        DeduplicationDuration(maxDedupDuration.multipliedBy(2)),
      )

      logger.debug("submit command with too long dedup duration")
      val submissionIdTooLongDuration = "submission-id-too-long-duration"
      val dedupPeriod7 = DeduplicationDuration(maxDedupDuration.multipliedBy(2).plusMillis(1))
      submitAsync(
        commandId7DeduplicationPeriodTooLong,
        dedupPeriod7,
        submissionIdTooLongDuration,
      )
      val (completion7, offset7) = findCompletionFor(
        participant1,
        alice,
        after6,
        commandId7DeduplicationPeriodTooLong,
        submissionIdTooLongDuration,
      )
      checkDedupPeriodTooLong(completion7, submissionIdTooLongDuration, after2)

      // The logic in advanceTimeUntilFirstTimeProofRequest somewhat arbitrarily bumps the simClock by n seconds.
      // Per flake #12257, if the initial advanceTimeUntilFirstTimeProofRequest adds more seconds than the subsequent
      // call to advanceTimeUntilFirstTimeProofRequest, this can result in the event at offset `after5` being published
      // at less than maxDedupDuration earlier than the last event in this test, thus preventing
      // find_safe_offset()/prune(after5) at the end of this test from succeeding.
      if (outsideDedupBoundaryTs.plus(maxDedupDuration) > simClock.now) {
        simClock.advanceTo(outsideDedupBoundaryTs.plus(maxDedupDuration))
      }

      // Additionally add another millisecond because max-deduplication-time checks are "inclusive". This also avoids the flake
      // in #12257 in which the "command 5" event is published exactly 1 hour before the last (command 8) event.
      // Adding one additional millisecond ensures that the "command 5" event can be pruned (whereas the "command 6"
      // event happening almost an hour later and thus still cannot) ensuring that at the end of this test the safe
      // pruning offset matches "after5". The unpredictability stems from the eventually(simClock.advance) in
      // `advanceTimeUntilFirstTimeProofRequest`:
      simClock.advance(java.time.Duration.ofMillis(1L))

      logger.debug("submit command with too early deduplication offset")
      val submissionIdTooEarlyOffset = "submission-id-too-early-offset"
      val dedupPeriod8 = DeduplicationOffset(Some(Offset.tryFromLong(after1)))
      submitAsync(
        commandId7DeduplicationPeriodTooLong,
        dedupPeriod8,
        submissionIdTooEarlyOffset,
      )
      val (completion8, _offset8) = findCompletionFor(
        participant1,
        alice,
        offset7,
        commandId7DeduplicationPeriodTooLong,
        submissionIdTooEarlyOffset,
      )
      checkDedupPeriodTooLong(completion8, submissionIdTooEarlyOffset, after2)

      logger.debug("submit command with participant begin deduplication offset")
      val submissionIdParticipantBegin = "submission-id-participant-begin"
      val dedupPeriod9 = DeduplicationOffset(None)
      submitAsync(
        commandId7DeduplicationPeriodTooLong,
        dedupPeriod9,
        submissionIdParticipantBegin,
      )
      val (completion9, _offset9) = findCompletionFor(
        participant1,
        alice,
        offset7,
        commandId7DeduplicationPeriodTooLong,
        submissionIdTooEarlyOffset,
      )
      checkDedupPeriodTooLong(completion9, submissionIdTooEarlyOffset, after2)

      val safe5 = eventually() {
        val safe = participant1.pruning.find_safe_offset().value
        safe should be >= after5
        safe
      }
      participant1.pruning.prune(safe5)
    }
}

private object CommandDeduplicationIntegrationTest {
  final case class DelayPromises(
      requestReceived: Future[Unit],
      requestRelease: Promise[Unit],
      responseReceived: Future[Unit],
      responseRelease: Promise[Unit],
  )
}

class CommandDeduplicationPruningIntegrationTestPostgres
    extends CommandDeduplicationPruningIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
