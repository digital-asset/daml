// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.crypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  CostEstimationHints,
  PrepareSubmissionResponse,
}
import com.daml.ledger.javaapi.data.codegen.ContractId as CodeGenCID
import com.daml.ledger.javaapi.data.{Command, DisclosedContract, Identifier}
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
  TrafficControlParameters,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.examples.java.divulgence.DivulgeIouByExercise
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.{ExternalParty, Member, Party}

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.RichOption

final class InteractiveSubmissionTrafficCostEstimationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasProgrammableSequencer
    with HasCycleUtils {

  private var aliceE: ExternalParty = _
  private var bobE: ExternalParty = _
  private var danE: ExternalParty = _
  private var bankE: ExternalParty = _
  private var localEmily: Party = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        val daSequencerConnection =
          SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))
        participants.all.synchronizers.connect(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = daSequencerConnection,
            timeTracker = SynchronizerTimeTrackerConfig(observationLatency =
              config.NonNegativeFiniteDuration.Zero
            ),
          )
        )
        participants.all.dars.upload(CantonExamplesPath)
        participants.all.dars.upload(CantonTestsPath)
        aliceE = cpn.parties.testing.external.enable(
          "Alice",
          keysCount = PositiveInt.three,
          keysThreshold = PositiveInt.one,
        )
        bobE = epn.parties.testing.external.enable("Bob")
        danE = cpn.parties.testing.external.enable(
          "Dan",
          keysCount = PositiveInt.three,
          keysThreshold = PositiveInt.three,
        )
        bankE = ppn.parties.testing.external.enable("Bank")
        localEmily = participant1.parties.enable("Emily")
      }
      .withTrafficControl(
        // Update the traffic parameters to set base event cost and base traffic rate to 0
        // This allows for easier assertions over traffic consumed
        TrafficControlParameters.default.copy(
          maxBaseTrafficAmount = NonNegativeLong.zero,
          baseEventCost = NonNegativeLong.zero,
        )
      )
      .withSetup { implicit env =>
        import env.*
        // Top up all members with max traffic
        Seq[Member](participant1, participant2, participant3, mediator1.id).foreach { p =>
          sequencer1.traffic_control.set_traffic_balance(
            p,
            PositiveInt.one,
            NonNegativeLong.maxValue,
          )
        }

        Seq[Member](participant1, participant2, participant3, mediator1.id).foreach { p =>
          utils.retry_until_true(timeouts.default) {
            sequencer1.traffic_control
              .traffic_state_of_members(Seq(p))
              .trafficStates
              .headOption
              .value
              ._2
              .extraTrafficPurchased == NonNegativeLong.maxValue
          }
        }

        // "Deactivate" ACS commitments to not consume traffic in the background
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )
        sequencer1.topology.synchronisation.await_idle()

      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)

  private def estimateTrafficCost(
      hints: Option[CostEstimationHints],
      expectedPrecision: Double,
      preparingPn: LocalParticipantReference,
      submittingPn: LocalParticipantReference,
      partyId: Party,
      commands: Seq[Command],
      submit: PrepareSubmissionResponse => Unit,
      disclosedContracts: Seq[DisclosedContract] = Seq.empty,
  )(implicit env: FixtureParam) = {
    import env.*
    participant1
    val prepared = preparingPn.ledger_api.javaapi.interactive_submission.prepare(
      Seq(partyId),
      commands,
      commandId = UUID.randomUUID().toString,
      estimateTrafficCost = hints,
      disclosedContracts = disclosedContracts,
    )
    logger.debug(s"Estimated traffic cost: ${prepared.costEstimation.value}")
    val estimatedTrafficCost = prepared.costEstimation.value.totalTrafficCostEstimation
    // Get the traffic state before submitting...
    val ts1 = submittingPn.traffic_control.traffic_state(synchronizer1Id)
    submit(prepared)
    // ...and after
    val ts2 = submittingPn.traffic_control.traffic_state(synchronizer1Id)
    val actualCost = ts1.extraTrafficRemainder - ts2.extraTrafficRemainder
    estimatedTrafficCost should equal(
      actualCost +- (actualCost * expectedPrecision).toLong
    )
  }

  def createCycleCmdFromParty(party: Party) =
    createCycleCommandJava(party, UUID.randomUUID().toString)

  private def runCostEstimateOnAllNodeCombinations(
      commands: Seq[Command],
      party: ExternalParty = aliceE,
      disclosedContracts: Seq[DisclosedContract] = Seq.empty,
  )(implicit
      env: TestConsoleEnvironment
  ) =
    // Try all combinations of ppn / epn.
    // The accuracy will vary because of how hosting relationships affect view decomposition
    // and confirmation responses but it at least allows to check that the estimation is bounded
    List(ppn, cpn, epn)
      .combinations(2)
      .foreach {
        case List(p, e) =>
          estimateTrafficCost(
            None,
            0.1d,
            preparingPn = p,
            submittingPn = e,
            party.partyId,
            commands = commands,
            prepared => {
              e.ledger_api.commands.external.submit_prepared(party, prepared)
            },
            disclosedContracts = disclosedContracts,
          )
        case _ => fail("expected 2 node selectors")
      }

  "Traffic cost estimation" should {

    "estimate traffic cost accurately for a create cycle" in { implicit env =>
      runCostEstimateOnAllNodeCombinations(Seq(createCycleCmdFromParty(aliceE)))
    }

    "estimate traffic cost accurately for a transaction with views" in { implicit env =>
      import env.*

      val transaction = cpn.ledger_api.javaapi.commands.submit(
        Seq(bankE),
        Seq(
          new Iou(
            bankE.toProtoPrimitive,
            aliceE.toProtoPrimitive,
            new Amount(BigDecimal.decimal(10L).bigDecimal, "CC"),
            List.empty.asJava,
          )
            .create()
            .commands()
            .loneElement
        ),
        includeCreatedEventBlob = true,
      )

      val createdEvent = transaction.getEvents.loneElement.toProtoEvent.getCreated

      val iouTransaction =
        JavaDecodeUtil.decodeAllCreated(Iou.COMPANION)(transaction).loneElement

      val exerciseCommand = Iou.ContractId
        .fromContractId(new CodeGenCID(iouTransaction.id.contractId))
        .exerciseShare(bobE.toProtoPrimitive)

      estimateTrafficCost(
        None,
        0.1d,
        preparingPn = cpn,
        submittingPn = cpn,
        aliceE.partyId,
        commands = Seq(exerciseCommand.commands().loneElement),
        prepared => {
          cpn.ledger_api.commands.external.submit_prepared(aliceE, prepared)
        },
        disclosedContracts = Seq(
          new DisclosedContract(
            createdEvent.getCreatedEventBlob,
            synchronizer1Id.logical.toProtoPrimitive,
            Some(Identifier.fromProto(createdEvent.getTemplateId)).toJava,
            Some(iouTransaction.id.contractId).toJava,
          )
        ),
      )
    }

    "estimate traffic cost accurately for an exercise of a non consuming choice" in {
      implicit env =>
        import env.*

        val transaction = cpn.ledger_api.javaapi.commands.submit(
          Seq(aliceE),
          Seq(
            new DivulgeIouByExercise(aliceE.toProtoPrimitive, aliceE.toProtoPrimitive)
              .create()
              .commands()
              .loneElement
          ),
          includeCreatedEventBlob = true,
        )

        val createdEvent = transaction.getEvents.loneElement.toProtoEvent.getCreated

        val divulgenceTransaction =
          JavaDecodeUtil.decodeAllCreated(DivulgeIouByExercise.COMPANION)(transaction).loneElement

        val exerciseCommand = DivulgeIouByExercise.ContractId
          .fromContractId(new CodeGenCID(divulgenceTransaction.id.contractId))
          .exerciseImmediateDivulgeIou()

        runCostEstimateOnAllNodeCombinations(
          Seq(exerciseCommand.commands().loneElement),
          disclosedContracts = Seq(
            new DisclosedContract(
              createdEvent.getCreatedEventBlob,
              synchronizer1Id.logical.toProtoPrimitive,
              Some(Identifier.fromProto(createdEvent.getTemplateId)).toJava,
              Some(divulgenceTransaction.id.contractId).toJava,
            )
          ),
        )
    }

    "estimate traffic cost accurately with hints" in { implicit env =>
      estimateTrafficCost(
        Some(
          CostEstimationHints.defaultInstance.withExpectedSignatures(
            danE.signingFingerprints.map(_ => SIGNING_ALGORITHM_SPEC_ED25519).forgetNE
          )
        ),
        0.1d,
        preparingPn = ppn,
        submittingPn = epn,
        aliceE.partyId,
        commands = Seq(createCycleCmdFromParty(aliceE)),
        prepared => {
          val signatures = Map(
            aliceE.partyId -> env.global_secret
              // Use all keys, to sign instead of just "threshold"
              // The estimation without hints uses threshold signatures, but because of the hint we should get an
              // accurate estimation still
              .sign(prepared.preparedTransactionHash, aliceE, useAllKeys = true)
          )
          epn.ledger_api.interactive_submission
            .execute_and_wait_for_transaction(
              preparedTransaction = prepared.getPreparedTransaction,
              transactionSignatures = signatures,
              submissionId = "",
              hashingSchemeVersion = prepared.hashingSchemeVersion,
            )
        },
      )
    }

    "estimate traffic cost very accurately when ppn == epn = cpn" in { implicit env =>
      estimateTrafficCost(
        None,
        // Expect less than 1% error when all nodes align
        0.01,
        preparingPn = cpn,
        submittingPn = cpn,
        bobE.partyId,
        commands = Seq(createCycleCmdFromParty(bobE)),
        prepared => {
          cpn.ledger_api.commands.external.submit_prepared(bobE, prepared)
        },
      )
    }

    "estimate the traffic cost of a prepared transaction for a local party" in { implicit env =>
      import env.*
      val commands = Seq(createCycleCmdFromParty(localEmily))
      estimateTrafficCost(
        None,
        // For local parties, we have the equivalent of ppn = epn = cpn, so accuracy should be very good as well
        0.01d,
        participant1,
        participant1,
        localEmily,
        commands = commands,
        _ =>
          participant1.ledger_api.javaapi.commands
            .submit(Seq(localEmily), commands, workflowId = UUID.randomUUID().toString),
      )
    }

    "disable traffic cost estimation" in { implicit env =>
      ppn(env).ledger_api.javaapi.interactive_submission
        .prepare(
          Seq(aliceE.partyId),
          Seq(createCycleCmdFromParty(aliceE)),
          commandId = UUID.randomUUID().toString,
          estimateTrafficCost = Some(CostEstimationHints.defaultInstance.withDisabled(true)),
        )
        .costEstimation shouldBe empty
    }

    "estimate traffic cost 0 when traffic control is disabled" in { implicit env =>
      import env.*
      sequencer1.topology.synchronizer_parameters.propose_update(
        synchronizerId = synchronizer1Id,
        _.update(trafficControl = None),
        synchronize = Some(environmentTimeouts.default),
      )
      val costEstimation = ppn(env).ledger_api.javaapi.interactive_submission
        .prepare(
          Seq(aliceE.partyId),
          Seq(createCycleCmdFromParty(aliceE)),
          commandId = UUID.randomUUID().toString,
        )
        .costEstimation
        .value

      costEstimation.confirmationRequestTrafficCostEstimation shouldBe 0L
      costEstimation.confirmationResponseTrafficCostEstimation shouldBe 0L
      costEstimation.totalTrafficCostEstimation shouldBe 0L
    }
  }
}
