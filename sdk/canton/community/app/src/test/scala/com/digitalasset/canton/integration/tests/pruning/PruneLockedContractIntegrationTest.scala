// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pruning

import cats.syntax.either.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.damltests.java.failedtransactionsdonotdivulge.One
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
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.LockedContracts
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.SendPolicy.processTimeProofs_
import com.digitalasset.canton.synchronizer.sequencer.{HasProgrammableSequencer, SendDecision}
import com.digitalasset.canton.topology.*
import org.scalactic.source.Position
import org.slf4j.event.Level

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

trait PruneLockedContractIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {
  private val confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(5)
  private val mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofSeconds(5)
  private val ledgerTimeRecordTimeTolerance = config.NonNegativeFiniteDuration.ofSeconds(1000)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime
      )
      .withSetup { env =>
        env.sequencer1.topology.synchronizer_parameters
          .propose_update(
            env.daId,
            _.update(
              mediatorDeduplicationTimeout = ledgerTimeRecordTimeTolerance * 2,
              preparationTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
            ),
          )

        env.sequencer1.topology.synchronizer_parameters.propose_update(
          env.daId,
          _.update(
            confirmationResponseTimeout = confirmationResponseTimeout,
            mediatorReactionTimeout = mediatorReactionTimeout * 10,
            ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
          ),
        )
      }

  "pruning can remove contracts cached in the conflict detector" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.dars.upload(CantonTestsPath)
    val alice = participant1.parties.enable("Alice")

    val sequencer = getProgrammableSequencer(sequencer1.name)

    val delay = confirmationResponseTimeout + mediatorReactionTimeout
    val delayS = delay.underlying
    val delayJ = delay.asJava

    val createCmd = new One(alice.toProtoPrimitive).create.commands.loneElement
    val createTx = participant1.ledger_api.javaapi.commands.submit_flat(
      Seq(alice),
      Seq(createCmd),
      commandId = "Creation",
    )
    val contractId =
      JavaDecodeUtil.decodeAllCreated(One.COMPANION)(createTx).loneElement.id

    val queuedRequests = new AtomicInteger(0)
    val resultCount = new AtomicInteger(0)
    val completedRequests = new AtomicInteger(0)

    val initialDelay = 1.second

    val startTime = env.environment.clock.now

    // Alice now submits several archivals for the created contract,
    // but we delay sequencing them such that there is always at least one transaction
    // in-flight and the safe-to-prune point in time moves past the first transaction
    //
    // In detail, the result for request i advances the sim clock such that request i+1 is delivered
    // and the response for request i+1 advances the sim clock such that the result for request i is delivered
    // Ideally, we should make sure that the sequencer delivers the delayed result message before
    // it advances the sim clock again, but the programmable sequencer backend doesn't allow us to do so
    // at the moment.
    sequencer.setPolicy_("spread transactions over time") {
      processTimeProofs_ { submissionRequest =>
        submissionRequest.sender match {
          case _: ParticipantId =>
            if (submissionRequest.isConfirmationRequest) {
              val previouslyQueued = queuedRequests.getAndIncrement()
              val delay = delayS.minus(2.millisecond) * previouslyQueued.toDouble + initialDelay
              SendDecision.DelayUntil(startTime.plusMillis(delay.toMillis))
            } else {
              if (isConfirmationResponse(submissionRequest)) {
                // This is a confirmation response so a request must have been delivered.
                // Advance the sim clock to release the confirmation result for the previous request.
                env.environment.simClock.value.advance(Duration.ofMillis(1))
              }
              SendDecision.Process
            }
          case _: SequencerId => SendDecision.Process
          case _: MediatorId =>
            // Trigger the delivery of the next request
            env.environment.simClock.value.advance(delayJ.minus(Duration.ofMillis(3)))
            resultCount.incrementAndGet()
            SendDecision.Delay(1.millisecond)
        }
      }
    }

    val archiveCmd = contractId.exerciseArchive().commands.loneElement
    val reqCount = 4

    loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.ERROR))(
      {
        val submitsF = Future.traverse((1 to reqCount).toList) { i =>
          Future {
            // Wrap everything in an Either such that we wait for all futures to complete
            val res = Either.catchOnly[CommandFailure] {
              participant1.ledger_api.javaapi.commands
                .submit_flat(Seq(alice), Seq(archiveCmd), commandId = f"Archival-$i%02d")
            }
            completedRequests.incrementAndGet()
            res
          }
        }

        utils.retry_until_true(queuedRequests.get == reqCount)

        // Release the first confirmation request
        env.environment.simClock.value.advance(Duration.ofMillis(initialDelay.toMillis))

        utils.retry_until_true(resultCount.get == reqCount)
        // Release the last confirmation result message
        env.environment.simClock.value.advance(Duration.ofMillis(1))

        val patience = defaultPatience.copy(timeout = defaultPatience.timeout * 2)
        submitsF.futureValue(patience, Position.here)
      },
      { (logEntries: Seq[LogEntry]) =>
        logEntries should have size (reqCount - 1).toLong
        logEntries.foreach(_.shouldBeCantonErrorCode(LockedContracts))
        val rejectedIds =
          logEntries.map(logEntry =>
            logEntry.errorMessage.split("commandId = 'Archival-")(1).substring(0, 2)
          )
        rejectedIds.distinct shouldBe rejectedIds
      },
    )

    completedRequests.get shouldBe reqCount
  }
}

class PruneLockedContractReferenceIntegrationTestPostgres
    extends PruneLockedContractIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
