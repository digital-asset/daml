// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.FeatureFlag
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.ledger.participant.state.SequencerIndex
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.sequencing.protocol.{DeliverError, MemberRecipient, TimeProof}
import com.digitalasset.canton.store.SequencedEventStore.{LatestUpto, OrdinarySequencedEvent}
import com.digitalasset.canton.synchronizer.sequencer.ProgrammableSequencerPolicies.isConfirmationResponse
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
  SendPolicy,
}
import com.digitalasset.canton.topology.{MediatorId, ParticipantId, PartyId}
import monocle.macros.syntax.lens.*

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.annotation.nowarn
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** The RepairSynchronizerRecoveryIntegrationTest verifies more intricate interactions of repair
  * requests and ConnectedSynchronizer recovery that "wedges in" repair requests without a sequencer
  * counter.
  */
@nowarn("msg=match may not be exhaustive")
trait RepairSynchronizerRecoveryIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with RepairTestUtil
    with HasProgrammableSequencer {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair),
        ConfigTransforms.setExitOnFatalFailures(false),
      )

  override val defaultParticipant: String = "participant1"

  protected val isClockTimeSupported: Boolean = true

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var initialized = false

  // Placing this helper in every test enables running any subset of tests in any order and provides syntactic sugar
  // providing parties Alice and Bob to every test.
  private def withParticipantInitialized[A](
      test: (PartyId, PartyId) => A
  )(implicit env: TestConsoleEnvironment): A = {
    import env.*

    if (!initialized) {
      sequencer1.topology.synchronizer_parameters
        .propose_update(
          daId,
          _.update(confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(5L)),
        )
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(CantonExamplesPath)
      participant1.dars.upload(CantonTestsPath)
      eventually()(
        assert(
          participant1.synchronizers.is_connected(initializedSynchronizers(daName).synchronizerId)
        )
      )

      participant1.parties.enable(
        aliceS
      )
      participant1.parties.enable(
        bobS
      )

      // ensure all participants have observed a point after the topology changes before disconnecting them
      participant1.testing.fetch_synchronizer_times()

      participant1.synchronizers.disconnect(daName)
      initialized = true
    }

    val Seq(alice, bob) = Seq(aliceS, bobS).map(_.toPartyId(participant1))
    test(alice, bob)
  }

  "crash recovery should wedge repair requests even if they do not fall on Deliver events" in {
    implicit env =>
      // TODO(#17334): verify
      // This test assumes that some protocol messages have timestamps within boundaries based on the current clock
      //  times, so it isn't supported by the BFT Ordering Sequencer, as BFT time does not necessarily depend on
      //  the current clock time.
      if (isClockTimeSupported) {
        withParticipantInitialized { (alice, _) =>
          import env.*

          val contractId = clue("Create some contract to be purged later on") {
            participant1.synchronizers.reconnect(daName)
            createContract(participant1, alice, alice)
          }

          clue("Interleave two requests with a non-deliver event") {
            val confirmationResponseCount = new AtomicInteger(0)
            val participantRequestCount = new AtomicInteger(0)
            val mediatorResultCount = new AtomicInteger(0)
            val releaseFirstConfirmationResponse = Promise[Unit]()
            val releaseSecondConfirmationResponse = Promise[Unit]()
            val holdingFirstResponse = Promise[Unit]()
            val holdingSecondResponse = Promise[Unit]()
            val releaseSecondMediatorResult = Promise[Unit]()

            val participant1Id = participant1.id

            val sequencer = getProgrammableSequencer(sequencer1.name)

            val dropSomeMessagesToP1 = new AtomicBoolean(false)

            /*
             Delay first and second ConfirmationResponse and second ConfirmationResult and invalidate second MediatorConfirmationRequest
             At some point (when dropSomeMessagesToP1 is true), drop time proofs and acs commitments sent to P1

             The reason we drop some messages is the following.
             We want to check the behavior when a non-Deliver event is sequence. Hence, we want to avoid
             subsequent deliver events to be sequenced. That could happen with time proofs and ACS commitments
             that are exchanged in the background.
             */
            sequencer.setPolicy("control the interleaving") { implicit traceContext => submission =>
              val toP1Only = submission.batch.allRecipients == Set(MemberRecipient(participant1.id))
              val droppable = TimeProof.isTimeProofSubmission(submission) ||
                ProgrammableSequencerPolicies.isAcsCommitment(submission)

              if (toP1Only && droppable && dropSomeMessagesToP1.get()) SendDecision.Drop
              else if (TimeProof.isTimeProofSubmission(submission)) SendDecision.Process
              else {
                submission.sender match {
                  case _: MediatorId =>
                    logger.debug(s"Received a message from the mediator:\n$submission")
                    val previous = mediatorResultCount.getAndIncrement()
                    if (previous == 1) {
                      SendDecision.HoldBack(releaseSecondMediatorResult.future)
                    } else SendDecision.Process
                  case _: ParticipantId =>
                    if (submission.isConfirmationRequest) {
                      val previous = participantRequestCount.getAndIncrement()
                      if (previous == 1) {
                        // Modify the request so that it gets refused
                        val modifiedRequest = submission
                          .focus(_.topologyTimestamp)
                          .replace(Some(CantonTimestamp.MaxValue))
                        val signedModifiedRequest = signModifiedSubmissionRequest(
                          modifiedRequest,
                          participant1.underlying.value.sync.syncCrypto
                            .tryForSynchronizer(daId, defaultStaticSynchronizerParameters),
                        )
                        dropSomeMessagesToP1.set(true)
                        SendDecision.Replace(signedModifiedRequest)
                      } else
                        SendDecision.Process
                    } else if (
                      submission.batch.allMembers.forall(_.isInstanceOf[MediatorId]) &&
                      (submission.sender == participant1Id)
                    ) {
                      val previous = confirmationResponseCount.getAndIncrement()
                      logger.info(
                        s"Received confirmation response #${previous + 1} from $participant1Id with message ID ${submission.messageId}"
                      )
                      if (previous == 0) {
                        holdingFirstResponse.success(())
                        SendDecision.HoldBack(releaseFirstConfirmationResponse.future)
                      } else if (previous == 1) {
                        holdingSecondResponse.success(())
                        SendDecision.HoldBack(releaseSecondConfirmationResponse.future)
                      } else SendDecision.Process
                    } else SendDecision.Process
                  case _ => SendDecision.Process
                }
              }
            }

            val cleanTimeOfRequest =
              participant1.testing.state_inspection
                .lookupCleanTimeOfRequest(daName)
                .value
                .futureValueUS
                .value
            logger.debug(s"Clean time of request: $cleanTimeOfRequest")

            logger.info("Submitting first request")
            val createCmd = new iou.Iou(
              alice.toProtoPrimitive,
              alice.toProtoPrimitive,
              new iou.Amount(123.toBigDecimal, "ETH"),
              List.empty.asJava,
            ).create.commands.asScala.toSeq
            participant1.ledger_api.javaapi.commands.submit_async(
              Seq(alice),
              createCmd,
              commandId = "request-to-become-clean",
            )

            logger.info("waiting for the participant to confirm the first request")
            holdingFirstResponse.future.futureValue

            val beforeRefusedRequest = environment.clock.uniqueTime()

            logger.info("Create a refused request")
            loggerFactory.assertLogs(
              participant1.ledger_api.javaapi.commands.submit_async(
                Seq(alice),
                createCmd,
                commandId = "request-to-be-refused",
              ),
              _.warningMessage should include("Submission was rejected by the sequencer at"),
            )

            val afterRefusedRequest = environment.clock.uniqueTime()

            logger.info("Submitting second request")
            participant1.ledger_api.javaapi.commands.submit_async(
              Seq(alice),
              createCmd,
              commandId = "request-to-remain-dirty",
            )

            logger.info("Waiting for the participant to respond to the second request")
            holdingSecondResponse.future.futureValue

            logger.info("Releasing response to first request")
            releaseFirstConfirmationResponse.success(())

            logger.info("Wait until the first request has become clean")
            eventually() {
              val newCleanRequestIndex =
                participant1.testing.state_inspection
                  .lookupCleanTimeOfRequest(daName)
                  .value
                  .futureValueUS
                  .value
              newCleanRequestIndex.rc shouldBe >(cleanTimeOfRequest.rc)
              newCleanRequestIndex.timestamp shouldBe <(beforeRefusedRequest)
            }

            logger.info("Release response to second request")
            // We must release the confirmation response here
            // so that the shutdown of the sequencer client is not blocked.
            // We therefore separately hold back the corresponding confirmation result
            // to ensure that the second request remains dirty.
            releaseSecondConfirmationResponse.success(())

            logger.info("Disconnect the participant")
            participant1.synchronizers.disconnect(daName)

            logger.info("Releasing the confirmation result for the second request")
            releaseSecondMediatorResult.success(())

            clue("Check the clean sequencer index") {
              val deliverErrorP1 = participant1.testing.state_inspection
                .findMessage(daName, LatestUpto(afterRefusedRequest))
                .value
                .value
              deliverErrorP1.timestamp shouldBe >(beforeRefusedRequest)
              inside(deliverErrorP1) { case OrdinarySequencedEvent(_, signedEvent) =>
                // Make sure that we actually test the right thing!
                signedEvent.content shouldBe a[DeliverError]
                logger.debug(s"Rewinding to event ${signedEvent.content}")
              }
              // as after MDEL-Indexer fusion the SequencerIndex cannot go further than the earliest dirty RequestIndex, it is expected that we are already at the same SequencerIndex
              val expectedSequencerIndex =
                SequencerIndex(deliverErrorP1.timestamp)
              val synchronizerIndex = participant1.testing.state_inspection
                .lookupCleanSynchronizerIndex(daName)
                .value
                .futureValueUS
              synchronizerIndex.value.sequencerIndex.value shouldBe expectedSequencerIndex
            }

            sequencer.resetPolicy()
          }

          clue("Add a repair request") {
            participant1.repair.purge(daName, Seq(contractId.toLf), ignoreAlreadyPurged = false)
          }

          clue("Reconnect the participant") {
            participant1.synchronizers.reconnect(daName)
          }
        }
      } else succeed
  }

  "work if there are inflight validation requests" in { implicit env =>
    withParticipantInitialized { (alice, bob) =>
      import env.*

      // 1. Create some contract
      participant1.synchronizers.reconnect(daName)
      val contractId = createContract(participant1, alice, bob)

      // 2. We now interleave two requests:
      // The first's confirmation response is delayed until we've disconnected the participant. So it remains dirty.
      // The second request archives the contract from step 1. It is confirmed by the participant, too.

      val confirmationResponseCount = new AtomicInteger(0)
      val mediatorResultCount = new AtomicInteger(0)
      val releaseFirstConfirmationResponse = Promise[Unit]()
      val holdingFirstResponse = Promise[Unit]()
      val secondResultAvailable = Promise[Unit]()
      val releaseSecondMediatorResult = Promise[Unit]()

      val sequencer = getProgrammableSequencer(sequencer1.name)
      sequencer.setPolicy_(
        "delay first confirmation response by participant1 and second confirmation result"
      ) {
        SendPolicy.processTimeProofs_ { submission =>
          submission.sender match {
            case _: ParticipantId =>
              if (isConfirmationResponse(submission)) {
                val previous = confirmationResponseCount.getAndIncrement()
                logger.info(
                  s"Confirmation response number $previous with messageId ${submission.messageId}"
                )
                if (previous == 0) {
                  logger.info("Holding first request's confirmation response")
                  holdingFirstResponse.success(())
                  SendDecision.HoldBack(releaseFirstConfirmationResponse.future)
                } else SendDecision.Process
              } else SendDecision.Process
            case _: MediatorId =>
              val previous = mediatorResultCount.getAndIncrement()
              logger.info(
                s"Confirmation result number $previous with messageId ${submission.messageId}"
              )
              if (previous == 1) {
                logger.info("Holding second request's confirmation result")
                secondResultAvailable.success(())
                SendDecision.HoldBack(releaseSecondMediatorResult.future)
              } else SendDecision.Process
            case _ => SendDecision.Process
          }
        }
      }

      logger.info("Submitting first request")
      val createCmd =
        new iou.Iou(
          alice.toProtoPrimitive,
          bob.toProtoPrimitive,
          new iou.Amount(123.toBigDecimal, "ETH"),
          List.empty.asJava,
        ).create.commands.asScala.toSeq
      participant1.ledger_api.javaapi.commands.submit_async(Seq(alice), createCmd)
      holdingFirstResponse.future.futureValue

      logger.info("Submitting second request")
      val archiveCmd =
        contractId
          .exerciseArchive()
          .commands
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand))
      participant1.ledger_api.commands.submit_async(Seq(alice), archiveCmd)

      secondResultAvailable.future.futureValue

      logger.info("Disconnecting participant1 from synchronizer")
      releaseFirstConfirmationResponse.success(())
      participant1.synchronizers.disconnect(daName)
      logger.info("Disconnected participant1 from synchronizer")
      releaseSecondMediatorResult.success(())

      // 3. We purge the contract that the second request archives

      participant1.repair.purge(daName, Seq(contractId.toLf), ignoreAlreadyPurged = false)

      // 4. We reconnect to the synchronizer. This should not be possible any more. There is no way to fix participant!

      logger.info("Now reconnect participant1 to synchronizer and fail")

      loggerFactory.suppressWarningsAndErrors {
        // Use a Try, because this may or may not fail depending on whether a sequencer subscription can be
        // created before asynchronous processing fails.
        Try(participant1.synchronizers.reconnect(daName))

        // Participant1's sequencer client observes the asynchronous failure in the application handler
        // only when it receives another event from the sequencer.
        // Force a time request just to make sure it receives something.
        // Of course it won't work on this synchronizer if it's already disconnected so the time request may likely fail/timeout.
        eventually() {
          Try(
            participant1.testing.fetch_synchronizer_time(
              daName,
              timeout = 5.seconds,
            )
          ).isFailure shouldBe true
        }

        logger.info("Synchronizer should eventually be disconnected")
        eventuallyForever(timeUntilSuccess = 10.seconds, durationOfSuccess = 10.seconds) {
          participant1.synchronizers.active(daName) shouldBe false
        }
        logger.info("Synchronizer has been disconnected")

        // The inactivity check completes before the ConnectedSynchronizer has actually been closed!
        // So we explicitly close the participant
        logger.info("Stopping participant1")
        participant1.stop()
        logger.info("Stopped participant1")
      }
    }
  }
}

class RepairSynchronizerRecoveryIntegrationTestPostgres
    extends RepairSynchronizerRecoveryIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory)
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}

class RepairSynchronizerRecoveryBftOrderingIntegrationTestPostgres
    extends RepairSynchronizerRecoveryIntegrationTest {

  override protected val isClockTimeSupported: Boolean = false

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(loggerFactory)
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
