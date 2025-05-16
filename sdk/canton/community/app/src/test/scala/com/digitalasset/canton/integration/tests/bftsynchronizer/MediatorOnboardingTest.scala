// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.sequencing.SequencerConnections
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{ForceFlag, PartyId, UniqueIdentifier}

import scala.jdk.CollectionConverters.*

trait MediatorOnboardingTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with EntitySyntax {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 3,
        numSequencers = 1,
        numMediators = 2,
      )
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        clue("participants connect to sequencer1") {
          participants.local.synchronizers.connect_local(sequencer1, daName)
        }
      }

  /** This test replicates a scenario mediator onboarding bug found by canton network:
    * https://github.com/DACH-NY/canton-network-node/issues/10503
    */
  "Onboard the mediator while a transaction is going on" in { implicit env =>
    import env.*
    participant1.dars.upload(CantonExamplesPath)
    participant2.dars.upload(CantonExamplesPath)
    participant3.dars.upload(CantonExamplesPath)

    val AliceName = "Alice"

    PartiesAllocator(Set(participant1, participant2, participant3))(
      newParties = Seq(AliceName -> participant1.id),
      targetTopology = Map(
        AliceName -> Map(
          daId -> (PositiveInt.tryCreate(1) -> Set(
            participant1.id -> ParticipantPermission.Submission,
            participant2.id -> ParticipantPermission.Observation,
          ))
        )
      ),
    )
    val alice = AliceName.toPartyId(participant1)

    // create an Iou contract, for which we later exercise the Cal choice
    participant3.ledger_api.javaapi.commands.submit_flat(
      Seq(participant3.id.adminParty),
      new Iou(
        participant3.id.adminParty.toProtoPrimitive,
        alice.toProtoPrimitive,
        new Amount(17.toBigDecimal, "CC"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq,
      commandId = "commandId",
    )

    val iou = participant1.ledger_api.state.acs
      .active_contracts_of_party(
        alice,
        filterTemplates = Seq(TemplateId.fromJavaIdentifier(Iou.TEMPLATE_ID)),
      )
      .loneElement

    // onboard the mediator
    val mediator2Identity = mediator2.topology.transactions.identity_transactions()
    sequencer1.topology.transactions.load(
      mediator2Identity,
      store = daId,
      ForceFlag.AlienMember,
    )
    eventually() {
      sequencer1.topology.transactions
        .list(
          store = daId,
          filterNamespace = mediator2.namespace.filterString,
          filterMappings =
            Seq(TopologyMapping.Code.NamespaceDelegation, TopologyMapping.Code.OwnerToKeyMapping),
        )
        .result
        .map(_.mapping.code) should have size 2
    }

    sequencer1.topology.mediators.propose(
      daId,
      PositiveInt.one,
      active = Seq(mediator1.id, mediator2.id),
      group = NonNegativeInt.zero,
    )

    // create topology transactions manually so that we can upload it together with additional
    // topology transactions via transactions.load in one fell swoop,
    // with the aim that they are sequenced with the same sequenced time (aka in the same block).
    val p2GainsSubmissionPermission = generateTransactionToGrantP2SubmissionPermission(alice)

    // submit the exercise asynchronously ...
    participant1.ledger_api.javaapi.commands.submit_async(
      actAs = Seq(alice),
      new Iou.ContractId(iou.createdEvent.value.contractId)
        .exerciseCall()
        .commands()
        .asScala
        .toList,
    )

    // ... so that we can upload the elevation of participant2 to Submission permission
    // before the responses get through
    sequencer1.topology.transactions
      .load(p2GainsSubmissionPermission, store = daId)

    // initialize the mediator
    mediator2.setup.assign(
      daId.toPhysical,
      SequencerConnections.single(sequencer1.sequencerConnection),
    )
    mediator2.health.wait_for_initialized()

    // run an extra ping to verify that everything went through correctly
    participant3.health.ping(participant1)
  }

  private def generateTransactionToGrantP2SubmissionPermission(
      alice: PartyId
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    val p2GainsSubmissionPermission = SignedTopologyTransaction
      .signAndCreate(
        TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.two,
          PartyToParticipant.tryCreate(
            alice,
            threshold = PositiveInt.one,
            participants = Seq(participant1, participant2).map(p =>
              HostingParticipant(p.id, ParticipantPermission.Submission)
            ),
          ),
          BaseTest.testedProtocolVersion,
        ),
        signingKeys = NonEmpty(Set, participant1.fingerprint),
        isProposal = false,
        crypto = participant1.underlying.value.sync.syncCrypto.crypto.privateCrypto,
        BaseTest.testedProtocolVersion,
      )
      .failOnShutdown
      .futureValue

    val extraPayload = createExtraTopologyPayload()
    p2GainsSubmissionPermission +: extraPayload
  }

  // we need to produce a few more topology transactions, so that the topology processing
  // takes enough time for the next transaction to "overtake" the topology processing.
  // this is only happens when the mediator initialization calls
  // (in pseudocode) topologyClient.updateHead(snapshot.maxTimestamp.immediateSuccessor).
  private def createExtraTopologyPayload()(implicit env: TestConsoleEnvironment) = {
    import env.*

    (1 to 5).map(serial =>
      SignedTopologyTransaction
        .signAndCreate(
          TopologyTransaction(
            TopologyChangeOp.Replace,
            serial = PositiveInt.tryCreate(serial),
            PartyToParticipant.tryCreate(
              PartyId(
                UniqueIdentifier
                  .tryCreate("bob", participant1.fingerprint.toProtoPrimitive)
              ),
              threshold = PositiveInt.one,
              participants = Seq(
                HostingParticipant(
                  participant1.id,
                  if (serial % 2 == 0) ParticipantPermission.Submission
                  else ParticipantPermission.Confirmation,
                )
              ),
            ),
            BaseTest.testedProtocolVersion,
          ),
          signingKeys = NonEmpty(Set, participant1.fingerprint),
          isProposal = false,
          crypto = participant1.underlying.value.sync.syncCrypto.crypto.privateCrypto,
          BaseTest.testedProtocolVersion,
        )
        .failOnShutdown
        .futureValue
    )

  }
}

class MediatorOnboardingTestPostgres extends MediatorOnboardingTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
