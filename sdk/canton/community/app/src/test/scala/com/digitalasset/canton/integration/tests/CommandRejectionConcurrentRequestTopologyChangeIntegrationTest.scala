// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError.MalformedMessage
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{AcsInspection, EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.NoViewWithValidRecipients
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.sequencing.protocol.MemberRecipient
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.{BaseTest, config}
import io.grpc.Status.Code
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*

/*
The goal of this test is to ensure that concurrent confirmation request submission and topology
changes yields a command rejection

More precisely, if we denote by
- s_submission the topology snapshot used when creating a confirmation request
- s_request the topology snapshot used to validate the request (i.e., the topology at the sequencing time of the request),
then we want to assert that a command rejection is emitted in the following cases:
- set of hosting participants is extended
  s_submission contains party to participant mapping (party -> [P1, P2])
  s_request contains party to participant mapping (party -> [P1, P2, P3])
- set of hosting participants is shrunk
  s_submission contains party to participant mapping (party -> [P1, P2, P3])
  s_request contains party to participant mapping (party -> [P1, P2])

Setup:
- alice hosted on P1
- P2 and P3 for the multi-hosted parties
 */
sealed trait CommandRejectionConcurrentRequestTopologyChangeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection
    with HasProgrammableSequencer
    with EntitySyntax {

  private val alice = "alice"
  private var aliceId: PartyId = _

  private var programmableSequencer: ProgrammableSequencer = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        // Reduce max delay between submission and sequencing topology timestamp to be < maxSequencingTime
        _.focus(_.parameters.timeouts.processing.topologyChangeWarnDelay)
          .replace(config.NonNegativeDuration.tryFromDuration(10.seconds)),
      )
      .withSetup { implicit env =>
        import env.*

        // So that topology changes become effective as of sequencing time
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(topologyChangeDelay = config.NonNegativeFiniteDuration.Zero),
          )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(BaseTest.CantonExamplesPath)

        PartiesAllocator(participants.all.toSet)(
          Seq(alice -> participant1),
          Map(alice -> Map(daId -> (PositiveInt.one, Set(participant1.id -> Submission)))),
        )

        aliceId = alice.toPartyId(participant1)

        programmableSequencer = getProgrammableSequencer(sequencer1.name)
      }

  // Alice is the payer
  private def createIOU(owner: PartyId)(implicit env: TestConsoleEnvironment): Unit =
    env.participant1.ledger_api.javaapi.commands.submit_async(
      Seq(aliceId),
      new Iou(
        aliceId.toProtoPrimitive,
        owner.toProtoPrimitive,
        new Amount(100.0.toBigDecimal, "USD"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq,
    )

  private def enablePartyOnP3(
      party: PartyId
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    Seq(participant2, participant3).foreach(
      _.topology.party_to_participant_mappings.propose_delta(
        party = party,
        adds = List(participant3.id -> ParticipantPermission.Submission),
        store = daId,
      )
    )
  }

  private def disablePartyOnParticipant(participant: LocalParticipantReference, partyId: PartyId)(
      implicit env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    participant.topology.party_to_participant_mappings.propose_delta(
      party = partyId,
      removes = List(participant.id),
      store = daId,
    )
  }

  private def delayConfirmationRequestPolicy(
      delayingFuture: Future[Unit],
      confirmationRequestQueued: Promise[Unit],
  ): SendPolicyWithoutTraceContext = SendPolicy.processTimeProofs_ { submissionRequest =>
    submissionRequest.sender match {
      case _: ParticipantId =>
        if (submissionRequest.isConfirmationRequest) {
          confirmationRequestQueued.trySuccess(())
          SendDecision.HoldBack(delayingFuture)
        } else SendDecision.Process

      case _ => SendDecision.Process
    }
  }

  private def participantsHostingPartyFor(partyId: PartyId, participant: LocalParticipantReference)(
      implicit env: TestConsoleEnvironment
  ): Seq[ParticipantId] =
    participant.topology.party_to_participant_mappings
      .list(synchronizerId = env.daId, filterParty = partyId.filterString)
      .loneElement
      .item
      .participants
      .map(_.participantId)

  "Concurrent request and topology change" should {
    // In this case we consider the scenario where there are not any good views left.
    def extendSetOfHostingParticipantsTest(
        submissionToSequencingDelay: FiniteDuration = 0.seconds,
        mustContainWithClue: Seq[(LogEntry => Assertion, String)] = Seq.empty,
        mayContain: Seq[LogEntry => Assertion] = Seq(),
    )(implicit
        env: TestConsoleEnvironment
    ): Assertion = {
      import env.*

      PartiesAllocator(Set(participant1, participant2))(
        Seq("bob" -> participant2),
        Map(
          "bob" -> Map(
            daId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission))
          )
        ),
      )
      val bob = "bob".toPartyId(participant2)

      val topologyChangeP: Promise[Unit] = Promise[Unit]()
      // We don't want the topology change to be submitted before the confirmation request reaches the sequencer
      val confirmationRequestQueued: Promise[Unit] = Promise[Unit]()

      val ledgerEnd = participant1.ledger_api.state.end()

      programmableSequencer.setPolicy_("delayConfirmationResponse")(
        delayConfirmationRequestPolicy(topologyChangeP.future, confirmationRequestQueued)
      )

      val expectedLogs = LogEntry.assertLogSeq(mustContainWithClue, mayContain) _
      val commandCompletion = loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        {
          // The following two interleave
          createIOU(bob)

          confirmationRequestQueued.future.map(_ => enablePartyOnP3(bob)).futureValue

          // Use the provided delay to increase the time between submission and sequencing
          environment.simClock.value.advance(submissionToSequencingDelay.toJava)

          // Unblock the confirmation request
          topologyChangeP.trySuccess(())

          // At some point we should get a command completion
          participant1.ledger_api.completions
            .list(aliceId, atLeastNumCompletions = 1, beginOffsetExclusive = ledgerEnd)
            .loneElement
        },
        expectedLogs,
      )

      val commandStatus = NoViewWithValidRecipients.Error(CantonTimestamp.now())
      commandCompletion.synchronizerTime.value.synchronizerId shouldBe daId.toProtoPrimitive
      commandCompletion.status.value.code shouldBe Code.ABORTED.value()
      commandCompletion.status.value.message should include(commandStatus.cause)

      participant1.topology.party_to_participant_mappings.is_known(
        daId,
        bob,
        Seq(participant2, participant3),
      ) shouldBe true

      // Cleanup
      participant2.topology.party_to_participant_mappings.propose_delta(
        party = bob,
        removes = List(participant2.id, participant3.id),
        store = daId,
      )
      succeed
    }

    "trigger command rejection when set of hosting participants is extended (no good views)" in {
      implicit env =>
        // Keeping the delay between submission and sequencing short, we don't get warnings
        extendSetOfHostingParticipantsTest()
    }

    "trigger command rejection when set of hosting participants is extended (no good views), using an old submission topology timestamp" in {
      implicit env =>
        import env.*

        val expectedWarnings: Seq[(LogEntry => Assertion, String)] = Seq(
          (
            _.shouldBeCantonError(
              SyncServiceAlarm,
              _ should include(s"""where the view at List("") has missing recipients ${Set(
                  MemberRecipient(participant3.id)
                )}"""),
            ),
            "participant alarm",
          ),
          (
            _.shouldBeCantonError(
              MalformedMessage,
              _ should (include("Received a mediator confirmation request with id") and include(
                s"Reason: Missing root hash message for informee participants: ${participant3.id}"
              )),
            ),
            "mediator alarm",
          ),
        )

        // Providing a submission topology timestamp that exceeds the allowed delay results in warnings
        val maxDelay = environment.config.parameters.timeouts.processing.topologyChangeWarnDelay
        val submissionToSequencingDelay = maxDelay + 1.seconds

        extendSetOfHostingParticipantsTest(
          submissionToSequencingDelay.asFiniteApproximation,
          mustContainWithClue = expectedWarnings,
        )
    }

    /*
      In the above case, there are not any good views left. In this case, construct a scenario where
      there is a good view left.

      Contract: Iou(payer: alice, owner: charlie)
      Initial topology:
        - alice -> P1
        - charlie -> P2
        - donald -> P4

      Concurrent operations:
        - charlie reassigns the Iou to donald
        - charlie hosted on P3 as well

      The sub-view consisting of the creation of the reassigned Iou, visible to alice
      and donald is a good view because it does not involve charlie.
     */
    "trigger command rejection when set of hosting participants is extended (one good subview)" in {
      implicit env =>
        import env.*

        PartiesAllocator(Set(participant2, participant4))(
          Seq(
            "charlie" -> participant2,
            "donald" -> participant4,
          ),
          Map(
            "charlie" -> Map(
              daId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission))
            ),
            "donald" -> Map(
              daId -> (PositiveInt.one, Set(participant4.id -> ParticipantPermission.Submission))
            ),
          ),
        )

        val charlie = "charlie".toPartyId(participant2)
        val donald = "donald".toPartyId(participant4)

        val topologyChangeP: Promise[Unit] = Promise[Unit]()
        // We don't want the topology change to be submitted before the confirmation request reaches the sequencer
        val confirmationRequestQueued: Promise[Unit] = Promise[Unit]()

        val iou = IouSyntax.createIou(participant1)(payer = aliceId, owner = charlie)

        val transferCommand =
          iou.id.exerciseTransfer(donald.toProtoPrimitive).commands().asScala.toSeq

        val ledgerEnd = participant2.ledger_api.state.end()

        programmableSequencer.setPolicy_("delayConfirmationResponse")(
          delayConfirmationRequestPolicy(topologyChangeP.future, confirmationRequestQueued)
        )

        // The following two interleave
        participant2.ledger_api.javaapi.commands.submit_async(Seq(charlie), transferCommand)
        confirmationRequestQueued.future.map(_ => enablePartyOnP3(charlie)).futureValue

        // Unblock the confirmation request
        topologyChangeP.trySuccess(())

        // At some point we should get a command completion
        val commandCompletion = participant2.ledger_api.completions
          .list(charlie, atLeastNumCompletions = 1, beginOffsetExclusive = ledgerEnd)
          .loneElement

        commandCompletion.synchronizerTime.value.synchronizerId shouldBe daId.toProtoPrimitive
        commandCompletion.status.value.code shouldBe Code.INVALID_ARGUMENT.value()

        participant1.topology.party_to_participant_mappings.is_known(
          daId,
          charlie,
          Seq(participant2, participant3),
        ) shouldBe true
    }

    // In this case we consider the scenario where there are not good views left.
    "trigger command rejection when set of hosting participants is shrunk" in { implicit env =>
      import env.*

      val topologyChangeP: Promise[Unit] = Promise[Unit]()
      // We don't want the topology change to be submitted before the confirmation request reaches the sequencer
      val confirmationRequestQueued: Promise[Unit] = Promise[Unit]()

      PartiesAllocator(participants.all.toSet)(
        Seq("eve" -> participant2),
        Map(
          "eve" -> Map(
            daId -> (PositiveInt.one, Set(
              participant2.id -> Submission,
              participant3.id -> Submission,
            ))
          )
        ),
      )
      val eve = "eve".toPartyId(participant2)

      val ledgerEnd = participant1.ledger_api.state.end()

      programmableSequencer.setPolicy_("delayConfirmationResponse")(
        delayConfirmationRequestPolicy(topologyChangeP.future, confirmationRequestQueued)
      )

      // The following two interleave
      createIOU(eve)
      confirmationRequestQueued.future.map { _ =>
        disablePartyOnParticipant(participant3, eve)
        // check that the removal of eve from participant three is effective for participant1
        eventually() {
          participant1.topology.party_to_participant_mappings
            .list(daId, filterParty = eve.filterString)
            .loneElement
            .item
            .participants
            .map(_.participantId) should not contain participant3.id
        }
      }.futureValue

      // Unblock the confirmation request
      topologyChangeP.trySuccess(())

      // At some point we should get a command completion
      val commandCompletion = participant1.ledger_api.completions
        .list(aliceId, atLeastNumCompletions = 1, beginOffsetExclusive = ledgerEnd)
        .loneElement

      commandCompletion.synchronizerTime.value.synchronizerId shouldBe daId.toProtoPrimitive
      commandCompletion.status.value.code shouldBe Code.INVALID_ARGUMENT.value()

      participantsHostingPartyFor(eve, participant2) shouldBe Seq(participant2.id)
    }
  }
}

class CommandRejectionConcurrentRequestTopologyChangeIntegrationTestPostgres
    extends CommandRejectionConcurrentRequestTopologyChangeIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1"), Set("sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )
  // we need to register the ProgrammableSequencer after the ReferenceBlockSequencer
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
