// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade

import better.files.File
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.multihostedparties.CoinFactoryHelpers
import com.digitalasset.canton.integration.tests.upgrade.MajorUpgradeUtils.CantonNodes
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  ParticipantPermission,
}
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.Monocle.toAppliedFocusOps

/*
 * This test is used to test the major upgrade of the Canton network.
 * The focus is on consortium party, and late participant migration.
 * steps:
 *   - Start Canton with the old version
 *   - Migrate nodes except one participant (simulate one late migration) to a newer version
 *   - Perform some activity
 *   - Migrate the late node
 *   - Test that everything is working
 */

final class MajorUpgradeComplexWriterIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with MajorUpgradeUtils {

  override protected def testName: String = "major-upgrade-complex"

  private var consortiumPartyId: PartyId = _
  private var bob: PartyId = _

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1", "sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S2M2.withSetup(networkBootstrapSetup(_))

  override def beforeAll(): Unit = {
    createDirectory()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    copyFiles()
    super.afterAll()
  }

  "Major upgrade for early MainNet" should {
    "happy path" onlyRunWith ProtocolVersion.latest in { implicit env =>
      import env.*

      eventually() {
        participant1.topology.party_to_participant_mappings
          .list(daId, filterParty = consortiumPartyId.toProtoPrimitive) should not be empty
      }

      CoinFactoryHelpers.createCoinsFactory(
        consortiumPartyId,
        bob,
        participant1,
        sync = false,
      )

      // change the threshold on the consortium party
      participants.all.foreach(
        _.topology.party_to_participant_mappings.propose(
          party = consortiumPartyId,
          newParticipants = participants.all.map(_.id -> ParticipantPermission.Confirmation),
          threshold = 3,
          store = sequencer1.synchronizer_id,
        )
      )
      CoinFactoryHelpers.createCoins(bob, participant1, Seq(10, 11, 12), sync = false)

      exportNodesData()
    }
  }

  protected def networkBootstrapSetup(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // connect all participants to the domain
    participants.all.foreach { p =>
      p.synchronizers.connect_local_bft(
        sequencers = Seq(sequencer1, sequencer2),
        synchronizerAlias = daName,
      )
    }

    File(CantonLfV21).copyToDirectory(baseExportDirectory)
    participants.all.dars.upload(CantonLfV21, synchronizerId = daId)

    val decentralizedNamespace = participants.all
      .map(
        _.topology.decentralized_namespaces
          .propose_new(
            owners = participants.all.map(_.id.uid.namespace).toSet,
            threshold = PositiveInt.tryCreate(3),
            store = daId,
          )
          .transaction
          .mapping
      )
      .toSet
      .head

    val initialDecentralizedNamespace = decentralizedNamespace.namespace

    consortiumPartyId = PartyId.tryCreate("consortium-party", initialDecentralizedNamespace)

    participants.all.foreach(
      _.topology.party_to_participant_mappings.propose(
        party = consortiumPartyId,
        newParticipants = participants.all.map(_.id -> ParticipantPermission.Submission),
        threshold = PositiveInt.tryCreate(1), // will be increased
        store = daId,
      )
    )

    // create a proposal to test a corner case in sequencer initialization
    sequencer1.topology.decentralized_namespaces.propose(
      DecentralizedNamespaceDefinition
        .create(
          daId.namespace,
          PositiveInt.two,
          NonEmpty(Set, sequencer1.id.namespace, sequencer2.id.namespace, mediator1.id.namespace),
        )
        .getOrElse(throw new RuntimeException("invalid DND")),
      daId,
      signedBy = Seq(sequencer1.id.fingerprint),
    )

    bob = participant1.parties.enable("bob")

    // only used on the read side
    participant2.parties.enable("alice")
  }
}

final class MajorUpgradeComplexReaderIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with MajorUpgradeUtils {
  override protected def testName: String = "major-upgrade-complex"

  private var migratedCantonNodes: CantonNodes = _

  private lazy val s3Ref = getPreviousDump()

  // triggers the download from s3 when accessed
  private lazy val exportDirectory: File = s3Ref.localDownloadPath / testName

  private var consortiumPartyId: PartyId = _
  private var bob: PartyId = _
  private var alice: PartyId = _
  private var synchronizerId: SynchronizerId = _
  private var physicalSynchronizerId: PhysicalSynchronizerId = _

  registerPlugin(new UsePostgres(loggerFactory))

  // We use sequencer's groups to isolate the sequencers from each other, the same way we have different synchronizers
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(Set("sequencer1", "sequencer2"))
          .map(_.map(InstanceName.tryCreate))
      ),
    )
  )

  private lazy val envDef = EnvironmentDefinition.P4S2M2_Config
  override lazy val environmentDefinition: EnvironmentDefinition =
    envDef.withManualStart
      .addConfigTransform(
        ConfigTransforms.disableAutoInit(envDef.baseConfig.allLocalNodes.keySet.map(_.unwrap))
      )
      .withSetup { implicit env =>
        import env.*

        migratedCantonNodes = CantonNodes(
          participants = Seq(participant1, participant2, participant3, participant4),
          sequencers = Seq(sequencer1, sequencer2),
          mediators = Seq(mediator1, mediator2),
        )

        participants.local.start()
        sequencers.local.start()
        mediators.local.start()
      }
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            "participant1",
            "participant2",
            "participant3",
            "participant4",
          )
        )
      )

  override def beforeAll(): Unit = {
    createDirectory()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    copyFiles()
    super.afterAll()
  }

  "Major upgrade for early MainNet" should {
    s"happy path: migrating all nodes except participant 4 (${s3Ref.localDownloadPath} with ${ProtocolVersion.latest})" onlyRunWith ProtocolVersion.latest in {
      implicit env =>
        import env.*

        logger.info(s"Starting major upgrade test with dump ${s3Ref.localDownloadPath}")

        synchronizerId =
          SynchronizerId.tryFromString((exportDirectory / "synchronizer-id").contentAsString)

        val newStaticSynchronizerParameters =
          StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion = testedProtocolVersion)
        physicalSynchronizerId =
          PhysicalSynchronizerId(synchronizerId, newStaticSynchronizerParameters.toInternal)

        // step: migrate all nodes except the late one
        migratedCantonNodes.all.filterNot(_.name == participant4.name).foreach { migratedNode =>
          suppressDarSelfConsistencyWarning(
            migrateNode(
              migratedNode = migratedNode,
              newStaticSynchronizerParameters = newStaticSynchronizerParameters,
              synchronizerId = physicalSynchronizerId,
              newSequencers = migratedCantonNodes.sequencers,
              dars = Seq((exportDirectory / "CantonLfV21-3.4.0.dar").canonicalPath),
              exportDirectory = exportDirectory,
            )
          )
        }
        // check that the synchronizer's decentralized namespace also has a "pending" proposal
        sequencer1.topology.decentralized_namespaces
          .list(
            synchronizerId,
            proposals = true,
            filterNamespace = synchronizerId.namespace.filterString,
          ) should not be empty

        val parties = sequencer1.topology.party_to_participant_mappings
          .list(synchronizerId)
          .map(_.item)
          .map(_.partyId)

        alice = parties.find(_.identifier.str == "alice").value
        bob = parties.find(_.identifier.str == "bob").value
        consortiumPartyId = parties.find(_.identifier.str == "consortium-party").value

        // Import ACS except for p4
        migratedCantonNodes.participants.filterNot(_.name == participant4.name).foreach { newNode =>
          importAcs(newNode, migratedCantonNodes.sequencers, daName, exportDirectory)
        }

        // update synchronizer parameters
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId,
          _.update(
            confirmationRequestsMaxRate = NonNegativeInt.tryCreate(30000)
          ),
        )

        sequencer1.synchronizer_parameters.static
          .get()
          .protocolVersion shouldBe testedProtocolVersion

        eventually() {
          participant1.topology.party_to_participant_mappings
            .list(
              synchronizerId,
              filterParty = consortiumPartyId.toProtoPrimitive,
            ) should not be empty
        }

        participant1.ledger_api.state.acs
          .of_party(bob)
          .map(_.contractId)
          .size shouldBe 4 // 3 coins + 1 factory

        CoinFactoryHelpers.createCoins(bob, participant1, Seq(9), sync = false)
        CoinFactoryHelpers.transferCoin(
          consortiumPartyId,
          bob,
          participant1,
          alice,
          participant2,
          9,
          sync = false,
        )
    }

    "late migration of P4" onlyRunWith ProtocolVersion.latest in { implicit env =>
      import env.*

      val newStaticSynchronizerParameters =
        StaticSynchronizerParameters.defaultsWithoutKMS(protocolVersion = testedProtocolVersion)

      suppressDarSelfConsistencyWarning(
        migrateNode(
          migratedNode = participant4,
          newStaticSynchronizerParameters = newStaticSynchronizerParameters,
          synchronizerId = physicalSynchronizerId,
          newSequencers = migratedCantonNodes.sequencers,
          dars = Seq((exportDirectory / "CantonLfV21-3.4.0.dar").canonicalPath),
          exportDirectory = exportDirectory,
        )
      )

      importAcs(participant4, migratedCantonNodes.sequencers, daName, exportDirectory)

      participant1.health.ping(participant4.id)
      CoinFactoryHelpers.getCoins(participant4, consortiumPartyId).size shouldBe 4

      CoinFactoryHelpers.transferCoin(
        consortiumPartyId,
        alice,
        participant2,
        bob,
        participant1,
        9,
      )
      CoinFactoryHelpers.getCoins(participant4, alice) shouldBe empty
    }
  }
}
