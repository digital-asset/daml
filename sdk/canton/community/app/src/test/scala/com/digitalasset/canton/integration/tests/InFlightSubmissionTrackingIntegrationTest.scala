// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.SubmissionSynchronizerNotReady
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.{
  NoSynchronizerForSubmission,
  UnknownSubmitters,
}
import com.digitalasset.canton.error.TransactionRoutingError.UnableToQueryTopologySnapshot
import com.digitalasset.canton.examples.java.cycle
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.{
  SequencerRequest,
  SubmissionDuringShutdown,
  TimeoutError,
}
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.NotConnectedToAnySynchronizer
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}
import scala.util.{Random, Try}

trait InFlightSubmissionTrackingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with CommandDeduplicationTestHelpers {

  private val overrideMaxRequestSize = NonNegativeInt.tryCreate(100 * 1024)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        // Set a small request size for the participant so that the participant's sequencer client refuses to
        // send big requests to the sequencer and thus the participant can produce a rejection event
        // without having to wait for the max sequencing time elapsing.
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.sequencerClient.overrideMaxRequestSize).replace(Some(overrideMaxRequestSize))
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
      }

  private def createCycleContract(
      prefix: String,
      participant: ParticipantReference,
      small: Boolean,
  ): Unit = {
    import scala.jdk.CollectionConverters.*
    // Large payloads will be rejected by the sequencer client because of the request size limit
    val cyclePayload = if (small) { prefix + "-small" }
    else {
      prefix + "-big" + Seq
        // Use a random ASCII string so that it cannot be compressed easily beyond a factor of 2
        .fill(overrideMaxRequestSize.value * 2)(Random.nextPrintableChar().toString)
        .mkString("")
    }
    val party = participant.id.adminParty

    participant.ledger_api.javaapi.commands.submit_async(
      Seq(party),
      new cycle.Cycle(cyclePayload, party.toProtoPrimitive).create.commands.asScala.toSeq,
      commandId = s"$prefix-command-id",
      submissionId = s"$prefix-submission-id",
    )
  }

  private def traceIdOfCompletion(completion: Completion): String =
    LedgerClient.traceContextFromLedgerApi(completion.traceContext).traceId.value

  private def maxRequestSizeExceededLog(id: PhysicalSynchronizerId): String =
    s"Batch size \\(\\d+ bytes\\) is exceeding maximum size \\(MaxRequestSize\\(${overrideMaxRequestSize.unwrap}\\) bytes\\) for synchronizer $id"

  "generate completions for unsequenceable commands before observing the max sequencing time" in {
    implicit env =>
      import env.*

      val seenTraceIds = new AtomicReference[Set[String]](Set.empty)

      val party = participant1.adminParty
      val ledgerEnd1 = participant1.ledger_api.state.end()

      val p1id = participant1.id
      val seq1 = getProgrammableSequencer(sequencer1.name)
      seq1.setPolicy("record and drop confirmation requests by participant1")(
        SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
          if (submissionRequest.isConfirmationRequest && submissionRequest.sender == p1id) {
            logger.debug(s"Observing confirmation request")
            seenTraceIds.getAndUpdate(_ incl traceContext.traceId.value)
            SendDecision.Drop
          } else SendDecision.Process
        }
      )

      val completionFailures =
        try {
          val completionFailures = loggerFactory.assertLogs(
            {
              createCycleContract("submit-1", participant1, small = true)
              createCycleContract("submit-2", participant1, small = true)
              createCycleContract("submit-3", participant1, small = false)
              createCycleContract("submit-4", participant1, small = false)
              createCycleContract("submit-5", participant1, small = true)

              participant1.ledger_api.completions.list(party, 2, ledgerEnd1)
            },
            _.warningMessage should include regex maxRequestSizeExceededLog(daId.toPhysical),
            _.warningMessage should include regex maxRequestSizeExceededLog(daId.toPhysical),
          )
          completionFailures should have size 2L

          eventually() {
            seenTraceIds.get() should have size 3L
          }
          completionFailures
        } finally {
          seq1.resetPolicy()
        }

      // Make sure that we don't get a completion for messages that have been sent to the sequencer
      forAll(completionFailures) { completion =>
        seenTraceIds.get() shouldNot contain(traceIdOfCompletion(completion))
      }
      forAll(completionFailures) { completion =>
        CantonBaseError.isStatusErrorCode(
          SequencerRequest,
          completion.getStatus,
        ) shouldBe true
      }

      logger.info("Advance time so that the potentially sequenced submissions time out")
      val ledgerEnd2 = participant1.ledger_api.state.end()
      val completionTimeouts = loggerFactory.assertLogs(
        {
          environment.simClock.value.advance(java.time.Duration.ofDays(365))
          participant1.ledger_api.completions.list(party, 3, ledgerEnd2)
        },
        _.warningMessage should include("Submission timed out at"),
        _.warningMessage should include("Submission timed out at"),
        _.warningMessage should include("Submission timed out at"),
      )
      completionTimeouts should have size 3L
      forAll(completionTimeouts) { completion =>
        CantonBaseError.isStatusErrorCode(
          TimeoutError,
          completion.status.value,
        ) shouldBe true

        seenTraceIds.get() should contain(traceIdOfCompletion(completion))
      }
  }

  "generate completions for unsequenceable commands during a disconnect" in { implicit env =>
    import env.*

    logger.info("Generate a lot of transactions")
    val seenTraceIds = new AtomicReference[Set[String]](Set.empty)
    val observedSomeRequest = Promise[Unit]()

    val p1id = participant1.id
    val seq1 = getProgrammableSequencer(sequencer1.name)
    seq1.setPolicy("record and drop confirmation requests by participant1")(
      SendPolicy.processTimeProofs { implicit traceContext => submissionRequest =>
        if (submissionRequest.isConfirmationRequest && submissionRequest.sender == p1id) {
          logger.debug(s"Observing confirmation request")
          val previous = seenTraceIds.getAndUpdate(_ incl traceContext.traceId.value)
          if (previous.isEmpty) {
            observedSomeRequest.success(())
          }
          SendDecision.Drop
        } else SendDecision.Process
      }
    )

    val party = participant1.adminParty
    val ledgerEnd = participant1.ledger_api.state.end()

    val concurrentSubmissions = 100
    logger.info(s"Generating $concurrentSubmissions concurrent submissions")

    // Make sure that we don't run into backpressure
    participant1.resources.set_resource_limits(
      ResourceLimits(
        maxSubmissionRate = None,
        maxInflightValidationRequests = None,
      )
    )

    val (earlyRejects, completions) = loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        val submissionF = Future.traverse(1 to concurrentSubmissions) { i =>
          Future(Try {
            // Every second payload will be too large so that the sequencer client rejects it because of the request size limit
            createCycleContract(s"submit-during-shutdown-$i", participant1, small = i % 2 == 0)
          })
        }

        logger.info(
          "Wait with disconnecting until the sequencer sees the first request" +
            " and the participant rejects a request with a payload too large"
        )
        observedSomeRequest.future.futureValue
        eventually() {
          val recordedLogEntries = loggerFactory.fetchRecordedLogEntries
          recordedLogEntries.exists(
            _.warningMessage.matches(maxRequestSizeExceededLog(daId.toPhysical))
          )
        }

        participant1.synchronizers.disconnect(daName)

        val submissionOutcomes = submissionF.futureValue

        // Since `earlyRejects` contains only proper errors (as checked below by the log suppression),
        // these submissions then don't produce a completion, so there's no overlap with `completions`.
        val earlyRejects = submissionOutcomes.count(_.isFailure)
        val observed = seenTraceIds.get().size
        val maxExpected = concurrentSubmissions - earlyRejects - observed

        val completions = participant1.ledger_api.completions.list(
          party,
          maxExpected,
          ledgerEnd,
          timeout = config.NonNegativeDuration.ofSeconds(5L),
        )
        (earlyRejects, completions)
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (
            _.warningMessage should include regex maxRequestSizeExceededLog(daId.toPhysical),
            "request too large",
          )
        ),
        // All the various error codes that we can get back synchronously
        mayContain = Seq(
          _.shouldBeCantonErrorCode(NotConnectedToAnySynchronizer),
          _.shouldBeCantonErrorCode(SubmissionSynchronizerNotReady),
          _.shouldBeCantonErrorCode(NoSynchronizerForSubmission),
          _.shouldBeCantonErrorCode(UnknownSubmitters),
          _.shouldBeCantonErrorCode(SubmissionDuringShutdown),
          // TODO(#25385): clean-up these error expectations after the topology-aware package selection roll-out is finished
          _.shouldBeCantonErrorCode(UnableToQueryTopologySnapshot),
        ),
      ),
    )

    // Look again at the number of seen trace IDs, as the processing on the sequencer may still continue while we've retrieved the completions.
    val observed = seenTraceIds.get().size
    val missing = concurrentSubmissions - observed - completions.size - earlyRejects
    logger.info(
      s"Early rejects: $earlyRejects; completionCount: ${completions.size}; observed: $observed; missing: $missing"
    )
    // Missing are those that may have been sent to the sequencer, but have not actually reached the sequencer.
    // Since every other submission request is too large and at least one reaches the sequencer,
    // we can have conservative bounds on how many go missing.
    missing should be >= (0)
    missing should be <= (concurrentSubmissions / 2) - 1

    // Make sure that some completions are generated.
    // This relies on some submissions making it past the in-flight submission checker and yet not being sent to the sequencer.
    completions should not be Seq.empty

    // Make sure that we don't get a completion for messages that have been sent to the sequencer
    forAll(completions) { completion =>
      seenTraceIds.get() shouldNot contain(traceIdOfCompletion(completion))
    }

    forAll(completions) { completion =>
      val possibleErrors = Seq(SubmissionDuringShutdown, SequencerRequest)
      possibleErrors.exists(
        CantonBaseError.isStatusErrorCode(_, completion.getStatus)
      ) shouldBe true
    }
  }
}

// class InFlightSubmissionTrackingIntegrationTestH2
//     extends InFlightSubmissionTrackingIntegrationTest {
//   registerPlugin(new UseH2(loggerFactory))
//   registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//   registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
// }

class InFlightSubmissionTrackingIntegrationTestPostgres
    extends InFlightSubmissionTrackingIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
