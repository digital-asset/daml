// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.*
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.admin.api.client.commands.TopologyAdminCommands.Write.GenerateTransactions
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.PrivateKeyMetadata
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.cycle as C
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{PartiesAllocator, PartyToParticipantDeclarative}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.ForceFlag.{
  AllowInsufficientParticipantPermissionForSignatoryParty,
  AllowUnvalidatedSigningKeys,
  DisablePartyWithActiveContracts,
}
import com.digitalasset.canton.topology.TopologyManagerError.{
  NoAppropriateSigningKeyInStore,
  UnauthorizedTransaction,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import org.slf4j.event.Level.DEBUG

import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.*

@nowarn("msg=match may not be exhaustive")
trait TopologyManagementIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasCycleUtils
    with SecurityTestSuite
    with AccessTestScenario {

  // TODO(#16283): disable participant / roll keys while the affected nodes are busy

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*
      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonExamplesPath)
      sequencer1.topology.synchronizer_parameters.propose_update(
        daId,
        _.update(
          reconciliationInterval = PositiveDurationSeconds.ofDays(7)
        ),
      )
    }

  "A Canton operator" can {
    "reconnect a participant to a synchronizer twice" in { implicit env =>
      import env.*

      // making sure that attempting to connect when already connected will not leave things in a bad state
      participant1.synchronizers.reconnect(daName)
      participant2.synchronizers.reconnect(daName)
      participant1.synchronizers.reconnect(daName)

    }

    "issue new certificates through the sequencer admin api" in { implicit env =>
      import env.*

      // create a new intermediate CA on the sequencer
      val intermediateCAKey =
        sequencer1.keys.secret
          .generate_signing_key("intermediate-ca", SigningKeyUsage.NamespaceOnly)
      val nd = sequencer1.topology.namespace_delegations.list(store = daId)

      sequencer1.topology.namespace_delegations.propose_delegation(
        sequencer1.namespace,
        intermediateCAKey,
        CanSignAllButNamespaceDelegations,
      )

      eventually() {
        val nd3 = sequencer1.topology.namespace_delegations.list(store = daId)
        assertResult(1, nd3.map(_.item))(nd3.length - nd.length)
      }
    }

    "allocate new parties" taggedAs_ { scenario =>
      SecurityTest(
        property = Authorization,
        asset = "ledger api",
        happyCase = scenario,
      )
    } in { implicit env =>
      import env.*

      val p1Id = participant1.id

      // add new party
      val newParty = PartyId(p1Id.uid.tryChangeId("Donald"))
      participant1.topology.party_to_participant_mappings.propose(
        newParty,
        List(p1Id -> ParticipantPermission.Submission),
        store = daId,
      )

      eventually() {
        val pr1 = sequencer1.topology.party_to_participant_mappings.list(
          filterParty = "Donald",
          synchronizerId = daId,
          filterParticipant = participant1.filterString,
        )
        assertResult(1, pr1)(pr1.length)
      }

      def checkPartyExists(
          participant: LocalParticipantReference,
          name: String,
          exists: Boolean = true,
      ): Unit =
        eventually() {
          val plist1 = participant.parties.list(name)
          if (exists) {
            plist1 should have length 1
            plist1.map(_.party.toProtoPrimitive) contains name
          } else {
            plist1 shouldBe empty
          }
        }

      checkPartyExists(participant1, newParty.toProtoPrimitive)

      participant1.parties.enable("someone")
      checkPartyExists(participant1, "someone")

      // check party management macros
      // architecture-handbook-entry-begin: EnableParty
      val name = "Gottlieb"
      val partyId = participant1.parties.enable(name)
      // architecture-handbook-entry-end: EnableParty

      checkPartyExists(participant1, name)
      // architecture-handbook-entry-begin: DisableParty
      participant1.parties.disable(partyId)
      // architecture-handbook-entry-end: DisableParty
      checkPartyExists(participant1, name, exists = false)
    }

    "permission a party on two nodes" taggedAs_ { action =>
      SecurityTest(
        property = Authorization,
        asset = "ledger api",
        happyCase = action,
      )
    } in { implicit env =>
      import env.*

      // enable party on participant1 (will invoke topology.party_to_participant_mappings.propose) under the hood
      val partyId = participant1.parties.enable(
        "Jesper",
        synchronizeParticipants = Seq(participant2),
      )
      val p2id = participant2.id

      // authorize mapping of Jesper to P2
      Seq(participant1, participant2).foreach(p =>
        p.topology.party_to_participant_mappings.propose_delta(
          party = partyId, // party unique identifier
          adds = List(p2id -> ParticipantPermission.Submission),
          store = daId,
        )
      )

      eventually() {
        Seq(participant1, participant2).foreach { p =>
          p.parties
            .list("Jesper")
            .flatMap(_.participants.filter(_.synchronizers.nonEmpty).map(_.participant))
            .toSet shouldBe Set(participant1.id, participant2.id)
        }
      }
      clue("running cycle after permissioning party on two nodes") {
        runCycle(partyId, participant1, participant2)
      }
    }

    "not import same transaction twice" in { implicit env =>
      import env.*

      val tx = participant1.topology.party_to_participant_mappings.propose(
        PartyId(participant1.uid.tryChangeId("Watslav")),
        newParticipants = List(participant1.id -> ParticipantPermission.Submission),
        store = daId,
      )
      // propose a decentralized namespace with many owners, so that we have multiple
      // transactions with the same hash. This should not trip up the idempotent import.
      val dns = nodes.all
        .map { node =>
          node.topology.decentralized_namespaces
            .propose_new(
              nodes.all.map(_.namespace).toSet,
              PositiveInt.tryCreate(nodes.all.size),
              daId,
            )
            .mapping
        }
        .headOption
        .value
      // now make a change so that we have multiple transactions for the same hash for both an older
      // serial and the latest serial
      nodes.all.foreach { node =>
        eventually() {
          node.topology.decentralized_namespaces
            .list(daId, filterNamespace = dns.namespace.filterString) should not be empty
        }
        node.topology.decentralized_namespaces.propose(
          DecentralizedNamespaceDefinition.tryCreate(
            dns.namespace,
            PositiveInt.tryCreate(dns.threshold.decrement.value),
            dns.owners,
          ),
          daId,
          serial = Some(PositiveInt.two),
        )
      }

      val snapshot = participant1.topology.transactions.export_topology_snapshotV2(daId)

      // ignores duplicate transaction
      loggerFactory.assertLogsSeq(
        SuppressionRule.LevelAndAbove(DEBUG) && SuppressionRule
          .forLogger[SyncPersistentState]
      )(
        {
          participant1.topology.transactions.load(Seq(tx), daId)
          participant1.topology.transactions.import_topology_snapshotV2(snapshot, daId)
        },
        { logEntries =>
          logEntries should not be empty
          logEntries.exists(_.message.contains("Processing 0/")) shouldBe true
          logEntries.exists(
            _.message.contains(s"Ignoring existing transactions: ${Seq(tx).toVector}")
          ) shouldBe true
        },
      )

    }

    "not add the same party twice" in { implicit env =>
      import env.*

      def add() = participant1.topology.party_to_participant_mappings.propose(
        PartyId(participant1.uid.tryChangeId("Boris")),
        newParticipants = List(participant1.id -> ParticipantPermission.Submission),
        store = daId,
      )

      // add once
      val tx = add()
      // add twice
      assertThrowsAndLogsCommandFailuresUnordered(
        add(),
        _.shouldBeCantonErrorCode(TopologyManagerError.MappingAlreadyExists),
      )
      // ignores duplicate transaction
      loggerFactory.assertLogsSeq(
        SuppressionRule.LevelAndAbove(DEBUG) && SuppressionRule
          .forLogger[SyncPersistentState]
      )(
        participant1.topology.transactions.load(Seq(tx), daId),
        { logEntries =>
          logEntries should not be empty
          logEntries.exists(
            _.message.contains(s"Ignoring existing transactions: ${Seq(tx).toVector}")
          ) shouldBe true
        },
      )
      // also fails when adding party twice
      assertThrowsAndLogsCommandFailures(
        participant1.parties.enable("Boris"),
        _.shouldBeCantonErrorCode(TopologyManagerError.MappingAlreadyExists),
      )

      // disable user to avoid conflicts with future tests
      participant1.parties.disable(tx.transaction.mapping.partyId)
    }

    "allow explicitly allocating an admin party" in { implicit env =>
      import env.*

      // explicitly allocating participant1's admin party works
      participant1.topology.party_to_participant_mappings.propose(
        participant1.id.adminParty,
        newParticipants = Seq(participant1.id -> ParticipantPermission.Submission),
        threshold = PositiveInt.one,
        store = daId,
      )

      // let's check that participant2 sees the party allocation as well
      eventually() {
        participant2.topology.party_to_participant_mappings
          .list(
            daId,
            filterParty = participant1.id.adminParty.filterString,
            filterParticipant = participant1.id.filterString,
          )
          .loneElement
          .context
          // checking the serial, since we already filter by party and participant
          .serial shouldBe PositiveInt.one
      }

      // participant1 allocating a party with participant2's UID fails
      assertThrowsAndLogsCommandFailures(
        participant1.topology.party_to_participant_mappings.propose(
          participant2.id.adminParty,
          newParticipants = Seq(participant1.id -> ParticipantPermission.Submission),
          threshold = PositiveInt.one,
          store = daId,
        ),
        _.shouldBeCantonErrorCode(TopologyManagerError.PartyIdConflictWithAdminParty),
      )
    }

    "not disable party if active contracts" in { implicit env =>
      import env.*

      val Vlad = participant1.parties.enable(
        "Vlad",
        synchronizeParticipants = Seq(participant2),
      )
      // enable Vlad on participant2
      Seq(participant1, participant2).foreach(
        _.topology.party_to_participant_mappings
          .propose_delta(
            Vlad,
            adds = List(participant2.id -> ParticipantPermission.Submission),
            store = daId,
          )
      )

      eventually() {
        Seq(participant1, participant2).foreach(
          _.topology.party_to_participant_mappings.list(
            synchronizerId = daId,
            filterParty = "Vlad",
            filterParticipant = participant2.filterString,
          ) should not be empty
        )
      }

      createCycleContract(participant1, Vlad, "Disable-Party-Active-Contracts")

      def offboardVladFromParticipants() =
        participant1.topology.party_to_participant_mappings.propose_delta(
          PartyId(Vlad.uid),
          removes = List(participant1.id, participant2.id),
          store = daId,
        )

      // try to remove
      assertThrowsAndLogsCommandFailures(
        offboardVladFromParticipants(),
        _.shouldBeCantonErrorCode(
          TopologyManagerError.ParticipantTopologyManagerError.DisablePartyWithActiveContractsRequiresForce
        ),
      )

      createCycleContract(participant2, Vlad, "Disable-Party-Active-Contracts-2")
      val Seq(c1, c2) = participant2.ledger_api.javaapi.state.acs.filter(C.Cycle.COMPANION)(Vlad)

      // archive contract
      participant1.ledger_api.javaapi.commands.submit(
        Seq(Vlad),
        c1.id.exerciseArchive().commands.asScala.toSeq,
      )

      participant2.ledger_api.javaapi.commands.submit(
        Seq(Vlad),
        c2.id.exerciseArchive().commands.asScala.toSeq,
      )

      offboardVladFromParticipants()
    }

    "deserialize a PTK with threshold > number of keys" in { implicit env =>
      import env.*

      val partyKey =
        global_secret.keys.secret.generate_keys(PositiveInt.one, usage = SigningKeyUsage.All).head1

      val party = PartyId.tryCreate("alice", Namespace(partyKey.fingerprint))

      val ptkProto = com.digitalasset.canton.protocol.v30.PartyToKeyMapping(
        party.toProtoPrimitive,
        threshold = 5,
        Seq(partyKey.toProtoV30),
      )

      PartyToKeyMapping.fromProtoV30(ptkProto).isRight shouldBe true
    }

    "deserialize a PTK with duplicate keys" in { implicit env =>
      import env.*

      val partyKey =
        global_secret.keys.secret.generate_keys(PositiveInt.one, usage = SigningKeyUsage.All).head1

      val party = PartyId.tryCreate("alice", Namespace(partyKey.fingerprint))

      val ptkProto = com.digitalasset.canton.protocol.v30.PartyToKeyMapping(
        party.toProtoPrimitive,
        threshold = 1,
        signingKeys = Seq(partyKey.toProtoV30, partyKey.toProtoV30),
      )

      val transaction = com.digitalasset.canton.protocol.v30.TopologyTransaction(
        com.digitalasset.canton.protocol.v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
        serial = 1,
        mapping = Some(
          com.digitalasset.canton.protocol.v30.TopologyMapping(
            com.digitalasset.canton.protocol.v30.TopologyMapping.Mapping.PartyToKeyMapping(ptkProto)
          )
        ),
      )

      val wrapped = UntypedVersionedMessage(
        UntypedVersionedMessage.Wrapper.Data(transaction.toByteString),
        version = testedProtocolVersion.v,
      )

      val originalByteString = wrapped.toByteString

      val deserialized: TopologyTransaction[TopologyChangeOp, TopologyMapping] =
        TopologyTransaction.fromByteString(testedProtocolVersion, originalByteString).value
      val ptkMappingDeserialized = deserialized.mapping.select[PartyToKeyMapping].value
      ptkMappingDeserialized.signingKeys.forgetNE should have size 1
      ptkMappingDeserialized.signingKeys.forgetNE.toSeq should contain theSameElementsAs Seq(
        partyKey
      )

      // Sanity check that the memoized bytes are the same as the original
      deserialized.getCryptographicEvidence shouldBe originalByteString
    }

    "deserialize a PTP with conflicting permissions for the same participant" in { implicit env =>
      import env.*

      val partyKey =
        global_secret.keys.secret.generate_keys(PositiveInt.one, usage = SigningKeyUsage.All).head1

      val party = PartyId.tryCreate("alice", Namespace(partyKey.fingerprint))

      val ptpProto = com.digitalasset.canton.protocol.v30.PartyToParticipant(
        party.toProtoPrimitive,
        threshold = 1,
        Seq(
          // Participant 2 is listed twice with different permissions
          com.digitalasset.canton.protocol.v30.PartyToParticipant.HostingParticipant(
            participant2.toProtoPrimitive,
            com.digitalasset.canton.protocol.v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION,
            None,
          ),
          com.digitalasset.canton.protocol.v30.PartyToParticipant.HostingParticipant(
            participant2.toProtoPrimitive,
            com.digitalasset.canton.protocol.v30.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION,
            None,
          ),
        ),
        None,
      )

      val transaction = com.digitalasset.canton.protocol.v30.TopologyTransaction(
        com.digitalasset.canton.protocol.v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
        serial = 1,
        mapping = Some(
          com.digitalasset.canton.protocol.v30.TopologyMapping(
            com.digitalasset.canton.protocol.v30.TopologyMapping.Mapping
              .PartyToParticipant(ptpProto)
          )
        ),
      )

      val wrapped = UntypedVersionedMessage(
        UntypedVersionedMessage.Wrapper.Data(transaction.toByteString),
        version = testedProtocolVersion.v,
      )

      val originalByteString = wrapped.toByteString

      val deserialized: TopologyTransaction[TopologyChangeOp, TopologyMapping] =
        TopologyTransaction.fromByteString(testedProtocolVersion, originalByteString).value
      val ptpMappingDeserialized = deserialized.mapping.select[PartyToParticipant].value
      val hosting = ptpMappingDeserialized.participants
      hosting should have size 1
      // Submission is higher than confirmation - p2 should have submission
      hosting
        .find(_.participantId == participant2.id)
        .value
        .permission shouldBe ParticipantPermission.Submission

      // Sanity check that the memoized bytes are the same as the original
      deserialized.getCryptographicEvidence shouldBe originalByteString
    }

    "cannot submit a new PartyToParticipant with threshold > number of keys" in { implicit env =>
      import env.*

      val partyKey =
        global_secret.keys.secret.generate_keys(PositiveInt.one, usage = SigningKeyUsage.All).head1

      val partyToParticipantMapping = TopologyTransaction(
        TopologyChangeOp.Replace,
        PositiveInt.one,
        PartyToParticipant.tryCreate(
          PartyId.tryCreate("alice", Namespace(partyKey.fingerprint)),
          threshold = PositiveInt.one,
          participants = Seq(HostingParticipant(participant2, ParticipantPermission.Confirmation)),
          partySigningKeysWithThreshold = Some(
            SigningKeysWithThreshold(NonEmpty.mk(Set, partyKey), PositiveInt.two)
          ),
        ),
        testedProtocolVersion,
      )

      val signed = SignedTopologyTransaction
        .create(
          partyToParticipantMapping,
          NonEmpty.mk(
            Set,
            SingleTransactionSignature(
              partyToParticipantMapping.hash,
              global_secret.sign(
                partyToParticipantMapping.hash.hash.getCryptographicEvidence,
                partyKey.fingerprint,
                SigningKeyUsage.All,
              ),
            ),
          ),
          isProposal = false,
          testedProtocolVersion,
        )
        .value

      val signedByParticipant2 = participant2.topology.transactions.sign(
        Seq(signed),
        store = daId,
      )

      assertThrowsAndLogsCommandFailures(
        participant2.topology.transactions.load(
          signedByParticipant2,
          store = daId,
        ),
        _.message should include(
          "Tried to set a signing threshold (2) above the number of signing keys (1)"
        ),
      )
    }

    "cannot submit a new PartyToKeyMapping with threshold > number of keys" in { implicit env =>
      import env.*

      val partyKey =
        global_secret.keys.secret.generate_keys(PositiveInt.one, usage = SigningKeyUsage.All).head1

      val partyToKeyMapping = TopologyTransaction(
        TopologyChangeOp.Replace,
        PositiveInt.one,
        PartyToKeyMapping.tryCreate(
          PartyId.tryCreate("alice", Namespace(partyKey.fingerprint)),
          threshold = PositiveInt.two,
          signingKeys = NonEmpty.mk(Seq, partyKey),
        ),
        testedProtocolVersion,
      )

      val signed = SignedTopologyTransaction
        .create(
          partyToKeyMapping,
          NonEmpty.mk(
            Set,
            SingleTransactionSignature(
              partyToKeyMapping.hash,
              global_secret.sign(
                partyToKeyMapping.hash.hash.getCryptographicEvidence,
                partyKey.fingerprint,
                SigningKeyUsage.All,
              ),
            ),
          ),
          isProposal = false,
          testedProtocolVersion,
        )
        .value

      assertThrowsAndLogsCommandFailures(
        participant2.topology.transactions.load(
          Seq(signed),
          store = daId,
        ),
        _.message should include(
          "Tried to set a signing threshold (2) above the number of signing keys (1)"
        ),
      )
    }

    "cannot disable a party if the threshold is not met anymore without a force flag" in {
      implicit env =>
        import env.*
        val alice2S = "Alice2"
        val Seq(alice2) = PartiesAllocator(participants.all.toSet)(
          Seq(alice2S -> participant1),
          Map(
            alice2S -> Map(
              daId -> (PositiveInt.two, Set(
                (participant1, Submission),
                (participant2, Submission),
              ))
            )
          ),
        )

        assertThrowsAndLogsCommandFailures(
          participant2.topology.party_to_participant_mappings.propose_delta(
            PartyId(alice2.uid),
            removes = List(participant2.id),
            store = daId,
          ),
          _.message should include(
            "Tried to set a confirming threshold (2) above the number of hosting nodes (1)"
          ),
        )
    }

    "participant or party can unilaterally downgrade the hosting permission" in { implicit env =>
      import env.*
      val downgradePartyS = "Downgrade"
      // start with hosting the party on participant2
      val Seq(downgradeParty) = PartiesAllocator(participants.all.toSet)(
        Seq(downgradePartyS -> participant1),
        Map(downgradePartyS -> Map(daId -> (PositiveInt.one, Set((participant2, Submission))))),
      )

      def downgrade(
          executing: ParticipantReference,
          verifying: ParticipantReference,
          targetParticipant: ParticipantId,
          permission: Option[ParticipantPermission],
      ) = {
        val p2ChangedPermission = executing.topology.party_to_participant_mappings.propose(
          downgradeParty,
          permission.map(targetParticipant -> _).toList,
          store = daId,
          forceFlags = ForceFlags(
            Option.when(permission.isEmpty)(ForceFlag.AllowConfirmingThresholdCanBeMet).toList*
          ),
        )
        eventually() {
          val updatedMapping = verifying.topology.party_to_participant_mappings
            .list(daId, filterParty = downgradeParty.filterString)
            .loneElement
          updatedMapping.context.serial shouldBe p2ChangedPermission.serial
          updatedMapping.item.participants
            .find(_.participantId == targetParticipant)
            .map(_.permission) shouldBe permission
        }
      }

      // the participant can unilaterally downgrade
      Seq(Some(Confirmation), Some(Observation), None).foreach { permission =>
        downgrade(
          executing = participant2,
          verifying = participant1,
          targetParticipant = participant2.id,
          permission = permission,
        )
      }

      // host the party on participant3
      PartiesAllocator(participants.all.toSet)(
        Seq(downgradePartyS -> participant1),
        Map(downgradePartyS -> Map(daId -> (PositiveInt.one, Set((participant3, Submission))))),
      )
      // the party can unilaterally downgrade
      Seq(Some(Confirmation), Some(Observation), None).foreach { permission =>
        downgrade(
          executing = participant1,
          verifying = participant3,
          targetParticipant = participant3.id,
          permission = permission,
        )
      }
    }

    "parties or participants cannot unilaterally upgrade the hosting permission" in {
      implicit env =>
        import env.*
        val upgradePartyS = "Upgrade"
        val Seq(upgradeParty) = PartiesAllocator(participants.all.toSet)(
          Seq(upgradePartyS -> participant1),
          Map(upgradePartyS -> Map(daId -> (PositiveInt.one, Set((participant2, Observation))))),
        )

        Seq(Confirmation, Submission).foreach { p2permission =>
          // the party's namespace tries to upgrade the hosting permission
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant1.topology.party_to_participant_mappings.propose_delta(
              upgradeParty,
              adds = Seq(participant2.id -> p2permission),
              mustFullyAuthorize = true,
              store = daId,
            ),
            _.shouldBeCantonErrorCode(UnauthorizedTransaction),
            _.errorMessage should include("Request failed for participant1"),
          )
          // the participant2 tries to upgrade the hosting permission
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            participant2.topology.party_to_participant_mappings.propose_delta(
              upgradeParty,
              adds = Seq(participant2.id -> p2permission),
              mustFullyAuthorize = true,
              store = daId,
            ),
            _.shouldBeCantonErrorCode(UnauthorizedTransaction),
            _.errorMessage should include("Request failed for participant2"),
          )

          // it works when both the party and the participant agree to upgrade the permission
          val p2ChangedPermission = Seq(participant1, participant2)
            .map(
              _.topology.party_to_participant_mappings.propose_delta(
                upgradeParty,
                adds = Seq(participant2.id -> p2permission),
                store = daId,
              )
            )
            .headOption
            .value
          eventually() {
            val updatedMapping = participant1.topology.party_to_participant_mappings
              .list(daId, filterParty = upgradeParty.filterString)
              .loneElement
            updatedMapping.context.serial shouldBe p2ChangedPermission.serial
            updatedMapping.item.participants
              .map(_.permission)
              .loneElement shouldBe p2permission
          }
        }
    }

    "clearing the onboarding flag requires the participant's authorization" in { implicit env =>
      import env.*
      val onboardingFlagParty =
        PartyId(UniqueIdentifier.tryCreate("OnboardingFlag", participant1.namespace))

      val onboardingFlagSet = Seq(participant1, participant2)
        .map(
          _.topology.party_to_participant_mappings.propose(
            onboardingFlagParty,
            Seq(participant2.id -> Submission),
            threshold = PositiveInt.one,
            store = daId,
            participantsRequiringPartyToBeOnboarded = Seq(participant2),
          )
        )
        .headOption
        .value

      eventually() {
        val updatedMapping = participant1.topology.party_to_participant_mappings
          .list(daId, filterParty = onboardingFlagParty.filterString)
          .loneElement
        updatedMapping.context.serial shouldBe onboardingFlagSet.serial
        updatedMapping.item.participants.loneElement.onboarding shouldBe true
      }

      // the party cannot clear the onboarding flag
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.party_to_participant_mappings.propose(
          onboardingFlagParty,
          Seq(participant2.id -> Submission),
          threshold = PositiveInt.one,
          store = daId,
        ),
        _.shouldBeCantonErrorCode(NoAppropriateSigningKeyInStore),
      )

      // the participant can clear its onboarding flag
      participant2.topology.party_to_participant_mappings.propose(
        onboardingFlagParty,
        Seq(participant2.id -> Submission),
        threshold = PositiveInt.one,
        store = daId,
      )

      eventually() {
        participant1.topology.party_to_participant_mappings
          .list(daId, filterParty = onboardingFlagParty.filterString)
          .loneElement
          .item
          .participants
          .loneElement
          .onboarding shouldBe false
      }
    }

    "a mixture of changes are properly authorized" in { implicit env =>
      import env.*
      val downgradeAndOnboardFlagClearanceParty =
        PartyId(
          UniqueIdentifier.tryCreate("DowngradeAndOnboardFlagClearance", participant1.namespace)
        )

      Seq(participant1, participant2, participant3)
        .foreach(
          _.topology.party_to_participant_mappings.propose(
            downgradeAndOnboardFlagClearanceParty,
            Seq(participant2.id -> Submission, participant3.id -> Observation),
            threshold = PositiveInt.one,
            store = daId,
            participantsRequiringPartyToBeOnboarded = Seq(participant3),
          )
        )

      Seq(participant1, participant2, participant3).foreach { participant =>
        eventually() {
          val updatedMapping = participant.topology.party_to_participant_mappings
            .list(daId, filterParty = downgradeAndOnboardFlagClearanceParty.filterString)
            .loneElement
          updatedMapping.item.participants should have size 2
        }
      }

      def submitDowngradeAndClear(executing: ParticipantReference, mustFullyAuthorize: Boolean) =
        executing.topology.party_to_participant_mappings.propose(
          downgradeAndOnboardFlagClearanceParty,
          Seq(participant2.id -> Confirmation, participant3.id -> Observation),
          threshold = PositiveInt.one,
          store = daId,
          mustFullyAuthorize = mustFullyAuthorize,
        )

      // the downgrading participant cannot unilaterally fully authorize the combined change
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        // force the failure with mustFullyAuthorize=true
        submitDowngradeAndClear(participant2, mustFullyAuthorize = true),
        _.shouldBeCantonErrorCode(UnauthorizedTransaction),
        _.errorMessage should include("Request failed for participant2"),
      )
      // the participant clearing the onboarding flag cannot unilaterally fully authorize the combined change
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        // force the failure with mustFullyAuthorize=true
        submitDowngradeAndClear(participant3, mustFullyAuthorize = true),
        _.shouldBeCantonErrorCode(UnauthorizedTransaction),
        _.errorMessage should include("Request failed for participant3"),
      )

      // the party cannot cannot unilaterally fully authorize the combined change
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        // force the failure with mustFullyAuthorize=true
        submitDowngradeAndClear(participant1, mustFullyAuthorize = true),
        _.shouldBeCantonErrorCode(UnauthorizedTransaction),
        _.errorMessage should include("Request failed for participant1"),
      )

      Seq(participant2, participant3).foreach { participant =>
        submitDowngradeAndClear(participant, mustFullyAuthorize = false)
      }

      eventually() {
        val ptp = participant1.topology.party_to_participant_mappings
          .list(daId, filterParty = downgradeAndOnboardFlagClearanceParty.filterString)
          .loneElement
          .item

        ptp.participants
          .find(_.participantId == participant2.id)
          .value
          .permission shouldBe Confirmation
        ptp.participants
          .find(_.participantId == participant3.id)
          .value
          .onboarding shouldBe false
      }
    }

    "Don't allow to change permission to pure observer only if the party is as signatory" in {
      implicit env =>
        import env.*
        val alexS = "Alex"
        val Seq(alex) = PartiesAllocator(participants.all.toSet)(
          Seq(alexS -> participant1),
          Map(
            alexS -> Map(
              daId -> (PositiveInt.one, Set(
                (participant1, Submission),
                (participant2, Submission),
              ))
            )
          ),
        )

        createCycleContract(participant1, alex, "change-vlad-permission")

        assertThrowsAndLogsCommandFailures(
          PartyToParticipantDeclarative.forParty(Set(participant1), daId)(
            participant1,
            alex,
            PositiveInt.two,
            Set(participant1.id -> Observation, participant2.id -> Observation),
          ),
          _.shouldBeCantonErrorCode(
            TopologyManagerError.ParticipantTopologyManagerError.InsufficientParticipantPermissionForSignatoryParty
          ),
        )

        // use force flag
        PartyToParticipantDeclarative.forParty(Set(participant1), daId)(
          participant1,
          alex,
          PositiveInt.two,
          Set(participant1.id -> Observation, participant2.id -> Observation),
          forceFlags = ForceFlags(AllowInsufficientParticipantPermissionForSignatoryParty),
        )
    }

    "Allow changing participant permission to pure observer for a party that is an observer on a contract" in {
      implicit env =>
        import env.*
        val alice =
          participant1.parties.enable("Alice")
        val signatory =
          participant1.parties.enable("signatory")

        IouSyntax.createIou(participant1, Some(daId))(signatory, alice)

        participant1.topology.party_to_participant_mappings.propose_delta(
          alice,
          adds = List((participant1, ParticipantPermission.Observation)),
          store = daId,
        )

        assertThrowsAndLogsCommandFailures(
          participant1.topology.party_to_participant_mappings.propose_delta(
            signatory,
            adds = List((participant1, ParticipantPermission.Observation)),
            store = daId,
          ),
          _.shouldBeCantonErrorCode(
            TopologyManagerError.ParticipantTopologyManagerError.InsufficientParticipantPermissionForSignatoryParty
          ),
        )

        // use force flag
        participant1.topology.party_to_participant_mappings.propose_delta(
          signatory,
          adds = List((participant1, ParticipantPermission.Observation)),
          store = daId,
          forceFlags = ForceFlags(AllowInsufficientParticipantPermissionForSignatoryParty),
        )
    }

    "Don't allow to change permission to pure observer if the multi-hosted party is a signatory with threshold of 1" in {
      implicit env =>
        import env.*

        val bobString = "Bob"
        val Seq(bob) = PartiesAllocator(participants.all.toSet)(
          Seq(bobString -> participant1),
          Map(
            bobString -> Map(
              daId -> (PositiveInt.one, Set(
                (participant1.id, Submission),
                (participant2, Submission),
              ))
            )
          ),
        )

        createCycleContract(participant1, bob, "change-bob-permission")

        PartyToParticipantDeclarative.forParty(Set(participant1), daId)(
          participant1,
          bob,
          PositiveInt.one,
          Set(participant1.id -> Observation, participant2.id -> Submission),
        )

        assertThrowsAndLogsCommandFailures(
          PartyToParticipantDeclarative.forParty(Set(participant1), daId)(
            participant1,
            bob,
            PositiveInt.one,
            Set(participant1.id -> Observation, participant2.id -> Observation),
          ),
          _.shouldBeCantonErrorCode(
            TopologyManagerError.ParticipantTopologyManagerError.InsufficientParticipantPermissionForSignatoryParty
          ),
        )

        PartyToParticipantDeclarative.forParty(Set(participant1), daId)(
          participant1,
          bob,
          PositiveInt.one,
          Set(participant1.id -> Observation, participant2.id -> Observation),
          forceFlags = ForceFlags(AllowInsufficientParticipantPermissionForSignatoryParty),
        )
    }

    "Allow to host a party with observation permission when it's already a pure observer" in {
      implicit env =>
        import env.*

        val bank =
          participant1.parties.enable("bank")
        createCycleContract(participant1, bank, "change-alice-permission")

        // force to change to pure observer
        PartyToParticipantDeclarative.forParty(Set(participant1), daId)(
          participant1,
          bank,
          PositiveInt.one,
          Set(participant1.id -> Observation),
          forceFlags = ForceFlags(AllowInsufficientParticipantPermissionForSignatoryParty),
        )

        PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
          participant1,
          bank,
          PositiveInt.one,
          Set(participant1.id -> Observation, participant2.id -> Observation),
        )

    }

    "disable party with active contract with force" in { implicit env =>
      import env.*

      val Rick = participant1.parties.enable("Rick")
      createCycleContract(participant1, Rick, "Disable-Party-Active-Contracts-Force")

      participant1.ledger_api.javaapi.state.acs.await(C.Cycle.COMPANION)(Rick)

      // remove with force
      participant1.topology.party_to_participant_mappings.propose_delta(
        PartyId(Rick.uid),
        removes = List(participant1.id),
        forceFlags = ForceFlags(DisablePartyWithActiveContracts),
        store = daId,
      )

      eventually(timeUntilSuccess = 30.seconds) {
        participant1.topology.party_to_participant_mappings.list(
          synchronizerId = daId,
          filterParty = "Rick",
          filterParticipant = participant1.filterString,
        ) shouldBe empty
      }
    }

    "add the same party twice if authorized with different keys" taggedAs_ { action =>
      SecurityTest(
        property = Authorization,
        asset = "ledger api",
        happyCase = action,
      )
    } in { implicit env =>
      import env.*

      def add(signingKey: Option[Fingerprint]) =
        participant1.topology.party_to_participant_mappings.propose(
          PartyId(participant1.uid.tryChangeId("Jeremias")),
          newParticipants = List(participant1.id -> ParticipantPermission.Submission),
          signedBy = signingKey.toList,
          store = daId,
        )
      // vanilla add
      add(Some(participant1.fingerprint))
      eventually() {
        participant1.parties.list("Jeremias") should not be empty
      }
      // add new namespace delegation
      val key1 =
        participant1.keys.secret
          .generate_signing_key("test cert", SigningKeyUsage.NamespaceOnly)
      participant1.topology.namespace_delegations.propose_delegation(
        participant1.namespace,
        key1,
        CanSignAllButNamespaceDelegations,
        store = daId,
      )
      // add previous statement again but signed with a different key
      add(Some(key1.fingerprint))

    }

    def genTx[Op <: TopologyChangeOp, M <: TopologyMapping](
        participant: LocalParticipantReference,
        tx: TopologyTransaction[Op, M],
        key: SigningPublicKey,
    )(implicit ec: ExecutionContext): SignedTopologyTransaction[Op, M] = {
      val crypto = participant.underlying
        .map(_.sync.syncCrypto.crypto)
        .getOrElse(sys.error("where is my crypto?"))
      SignedTopologyTransaction
        .signAndCreate(
          tx,
          NonEmpty.mk(Set, key.fingerprint),
          isProposal = false,
          crypto.privateCrypto,
          testedProtocolVersion,
        )
        .valueOrFailShutdown("failed to create tx")
        .futureValue
    }

    "not remove a non-existent transaction" in { implicit env =>
      import env.*

      val key1 =
        participant1.keys.secret
          .generate_signing_key("test-key1", SigningKeyUsage.NamespaceOnly)
      val tx = genTx(
        participant1,
        TopologyTransaction(
          TopologyChangeOp.Remove,
          PositiveInt.tryCreate(1),
          NamespaceDelegation.tryCreate(participant1.namespace, key1, CanSignAllMappings),
          testedProtocolVersion,
        ),
        key1,
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.transactions.load(Seq(tx), daId),
        _.shouldBeCantonErrorCode(TopologyManagerError.NoCorrespondingActiveTxToRevoke),
      )

    }

    def unauthorizedTxTest(reason: String) = SecurityTest(
      property = Authorization,
      asset = "sequencer node",
      attack = Attack(
        actor = "an admin api user",
        threat = s"register an unauthorized topology transaction ($reason)",
        mitigation = "reject transaction",
      ),
    )

    "not add an unauthorized transaction" taggedAs unauthorizedTxTest("unauthorizedKey") in {
      implicit env =>
        import env.*

        val key =
          participant1.keys.secret
            .generate_signing_key("unauthorized-sequencer", SigningKeyUsage.ProtocolOnly)
        val tx = genTx(
          participant1,
          TopologyTransaction(
            TopologyChangeOp.Replace,
            PositiveInt.tryCreate(2),
            OwnerToKeyMapping.tryCreate(sequencer1.id, NonEmpty(Seq, key)),
            testedProtocolVersion,
          ),
          key,
        )

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          sequencer1.topology.transactions.load(Seq(tx), daId),
          _.shouldBeCantonErrorCode(TopologyManagerError.UnauthorizedTransaction),
          _.commandFailureMessage should include(
            s"INVALID_ARGUMENT/${LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API}"
          ),
        )
    }

    "not add a topology transaction without a signing key" taggedAs unauthorizedTxTest(
      "missing key"
    ) in { implicit env =>
      import env.*

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.party_to_participant_mappings.propose(
          PartyId(participant2.uid.tryChangeId("NothingToSignWith")),
          newParticipants = List(participant2.id -> ParticipantPermission.Submission),
          signedBy = Seq.empty,
          store = daId,
        ),
        _.shouldBeCommandFailure(TopologyManagerError.NoAppropriateSigningKeyInStore),
      )
    }

    "not add a topology transaction if the signing key is not in the store" taggedAs unauthorizedTxTest(
      "key not in store"
    ) in { implicit env =>
      import env.*

      val p2Key =
        participant2.keys.secret
          .generate_signing_key("some-secret-key", SigningKeyUsage.NamespaceOnly)

      def add(force: Boolean) = participant1.topology.party_to_participant_mappings.propose(
        PartyId(participant2.uid.tryChangeId("NothingToSignWith")),
        newParticipants = List(participant2.id -> ParticipantPermission.Submission),
        signedBy = Seq(p2Key.fingerprint),
        forceFlags = if (force) ForceFlags(AllowUnvalidatedSigningKeys) else ForceFlags.none,
        store = daId,
      )

      assertThrowsAndLogsCommandFailures(
        add(force = true),
        _.shouldBeCantonErrorCode(TopologyManagerError.SecretKeyNotInStore),
      )

      assertThrowsAndLogsCommandFailures(
        add(force = false),
        _.shouldBeCantonErrorCode(TopologyManagerError.NoAppropriateSigningKeyInStore),
      )

      participant1.keys.public
        .upload(p2Key.toByteString(testedProtocolVersion), Some("p2-some-key"))
      assertThrowsAndLogsCommandFailures(
        add(force = true),
        _.shouldBeCantonErrorCode(TopologyManagerError.SecretKeyNotInStore),
      )
      assertThrowsAndLogsCommandFailures(
        add(force = false),
        _.shouldBeCantonErrorCode(TopologyManagerError.NoAppropriateSigningKeyInStore),
      )

    }

    "not add a topology transaction if the signing key is valid for the transaction" taggedAs unauthorizedTxTest(
      "key not valid for transaction"
    ) in { implicit env =>
      import env.*

      val p1Key = participant1.keys.secret
        .generate_signing_key("some-secret-key", SigningKeyUsage.NamespaceOnly)

      def add(force: Boolean) = participant1.topology.party_to_participant_mappings.propose(
        PartyId(participant1.uid.tryChangeId("NothingToSignWith")),
        newParticipants = List(participant1.id -> ParticipantPermission.Submission),
        signedBy = Seq(p1Key.fingerprint),
        forceFlags = if (force) ForceFlags(AllowUnvalidatedSigningKeys) else ForceFlags.none,
        store = daId,
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        add(force = true),
        _.shouldBeCantonErrorCode(TopologyManagerError.UnauthorizedTransaction),
        _.commandFailureMessage should include(
          s"INVALID_ARGUMENT/${LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API}"
        ),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        add(force = false),
        _.shouldBeCantonErrorCode(TopologyManagerError.NoAppropriateSigningKeyInStore),
      )
    }

    "not add a topology transaction if the signature is invalid" taggedAs unauthorizedTxTest(
      "invalid signature"
    ) in { implicit env =>
      import env.*

      val key1 =
        participant1.keys.secret
          .generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)
      val sig = participant1.keys.secret
        .list(filterFingerprint = participant1.fingerprint.unwrap)
        .collectFirst { case PrivateKeyMetadata(x: SigningPublicKeyWithName, _, _) =>
          x.publicKey
        }
        .getOrElse(sys.error("where is my namespace key?"))

      def create(serial: Int) = genTx(
        participant1,
        TopologyTransaction(
          TopologyChangeOp.Replace,
          PositiveInt.tryCreate(serial),
          NamespaceDelegation.tryCreate(participant1.namespace, key1, CanSignAllMappings),
          testedProtocolVersion,
        ),
        sig,
      )

      val tx1 = create(1)
      val tx2 = create(2)

      // steal the sig of tx2 and use it for tx1
      val fakeTx = SignedTopologyTransaction.withTopologySignatures(
        transaction = tx1.transaction,
        signatures = tx2.signatures.toSeq,
        isProposal = tx1.isProposal,
        testedProtocolVersion,
      )

      // can not add it
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.transactions.load(Seq(fakeTx), daId),
        _.shouldBeCantonErrorCode(TopologyManagerError.InvalidSignatureError),
        _.commandFailureMessage should include(
          s"INVALID_ARGUMENT/${LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API}"
        ),
      )
      // but i can add the valid one
      participant1.topology.transactions.load(Seq(tx1), daId)

    }

    "export and import secret keys" in { implicit env =>
      import env.*

      val key1P =
        participant1.keys.secret
          .generate_signing_key("export", SigningKeyUsage.NamespaceOnly)
      val keyPair1 = participant1.keys.secret.download(key1P.fingerprint, testedProtocolVersion)

      clue("can import keys") {
        participant2.keys.secret.upload(keyPair1, Some("IMPORTED"))
      }

      participant2.keys.secret.list(key1P.fingerprint.unwrap) should not be empty
    }

    "not break the synching mechanism between authorized store and synchronizer store with only a single rejected transaction" in {
      implicit env =>
        import env.*

        val key1 =
          participant1.keys.secret
            .generate_signing_key("key1", SigningKeyUsage.NamespaceOnly)
        participant1.topology.namespace_delegations.propose_delegation(
          participant1.namespace,
          key1,
          CanSignAllButNamespaceDelegations,
        )

        val key2 =
          participant1.keys.secret
            .generate_signing_key("key2", SigningKeyUsage.NamespaceOnly)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.topology.namespace_delegations.propose_delegation(
            participant1.namespace,
            key2,
            CanSignAllButNamespaceDelegations,
            signedBy = Seq(key1.fingerprint),
            // use the force flag so that we actually get to the point where the topology transaction
            // is rejected, instead of failing the request early during signing key validation
            forceFlags = ForceFlags(ForceFlag.AllowUnvalidatedSigningKeys),
          ),
          _.shouldBeCantonError(
            UnauthorizedTransaction,
            _ should include(
              s"Topology transaction authorization cannot be verified due to missing namespace delegations for keys ${key1.fingerprint}"
            ),
          ),
          _.errorMessage should (include regex "INVALID_ARGUMENT.*Please contact the operator"),
        )
    }

    "query namespace delegations" in { implicit env =>
      import env.*

      val key1 =
        participant1.keys.secret
          .generate_signing_key("ns-test1", SigningKeyUsage.NamespaceOnly)
      val key2 =
        participant1.keys.secret
          .generate_signing_key("ns-test2", SigningKeyUsage.NamespaceOnly)

      Seq(key1, key2).map(key =>
        clue(s"registering root certificate for $key")(
          participant1.topology.namespace_delegations.propose_delegation(
            Namespace(key.fingerprint),
            key,
            CanSignAllMappings,
            store = daId,
          )
        )
      )

      clue("both keys are known")(eventually() {
        // we should see both
        val all = participant1.topology.namespace_delegations
          .list(daId)
          .map(_.item.namespace.fingerprint)
        forAll(Seq(key1, key2)) { key =>
          all should contain(key.fingerprint)
        }
        // filtering should work
        participant1.topology.namespace_delegations
          .list(daId, filterNamespace = key1.fingerprint.unwrap)
          .map(_.item.namespace.fingerprint) shouldBe Seq(key1.fingerprint)
      })

      // remove
      Seq(key1, key2).map(key =>
        clue(s"removing $key")(
          participant1.topology.namespace_delegations.propose_revocation(
            Namespace(key.fingerprint),
            key,
            store = daId,
          )
        )
      )

      eventually() {
        val known = participant1.topology.namespace_delegations
          .list(daId)
          .map(_.item.namespace.fingerprint)
        forAll(Seq(key1.fingerprint, key2.fingerprint)) { fp =>
          known should not contain fp
        }
      }

      // add and remove txs should be found
      val fndReplace = clue(s"find addition of $key1")(
        participant1.topology.namespace_delegations
          .list(
            daId,
            // Don't provide a starting date for the range, because the BFT Ordering Service
            // may apply a sequencing timestamp from before even the test started.
            // Besides, the filter is specific enough that it will only return the desired NSD.
            timeQuery = TimeQuery.Range(None, None),
            filterNamespace = key1.fingerprint.unwrap,
            operation = Some(TopologyChangeOp.Replace),
          )
          .map { rr =>
            (rr.context.operation, rr.item.namespace.fingerprint)
          }
      )
      val fndRemove = clue(s"find removal of $key1")(
        participant1.topology.namespace_delegations
          .list(
            daId,
            // Don't provide a starting date for the range, because the BFT Ordering Service
            // may apply a sequencing timestamp from before even the test started.
            // Besides, the filter is specific enough that it will only return the desired NSD.
            timeQuery = TimeQuery.Range(None, None),
            filterNamespace = key1.fingerprint.unwrap,
            operation = Some(TopologyChangeOp.Remove),
          )
          .map { rr =>
            (rr.context.operation, rr.item.namespace.fingerprint)
          }
      )
      fndReplace.concat(fndRemove).toSet shouldBe Set(
        (TopologyChangeOp.Replace, key1.fingerprint),
        (TopologyChangeOp.Remove, key1.fingerprint),
      )
    }

    "query owner to key mappings" in { implicit env =>
      import env.*

      val key =
        participant1.keys.secret
          .generate_signing_key(usage = SigningKeyUsage.ProtocolOnly)
      participant1.topology.owner_to_key_mappings.add_key(
        key.fingerprint,
        key.purpose,
      )

      eventually() {
        participant1.topology.owner_to_key_mappings.list(
          filterKeyOwnerUid = participant1.filterString,
          filterKeyOwnerType = Some(SequencerId.Code),
        ) shouldBe empty
        participant1.topology.owner_to_key_mappings
          .list(
            filterKeyOwnerUid = participant1.filterString,
            filterKeyOwnerType = Some(ParticipantId.Code),
            proposals = true,
          )
          .flatMap(_.item.keys.forgetNE.map(_.fingerprint)) should not contain key.fingerprint

        participant1.topology.owner_to_key_mappings
          .list(
            filterKeyOwnerUid = participant1.filterString,
            filterKeyOwnerType = Some(ParticipantId.Code),
          )
          .flatMap(_.item.keys.forgetNE.map(_.fingerprint)) should contain(key.fingerprint)
      }
    }

    "query party to participant mappings" in { implicit env =>
      import env.*

      participant1.topology.party_to_participant_mappings.propose(
        PartyId(participant1.uid.tryChangeId("Bertram")),
        newParticipants = List(participant1.id -> ParticipantPermission.Submission),
        store = daId,
      )

      eventually() {
        participant1.topology.party_to_participant_mappings.list(
          daId,
          filterParty = "Bertram",
          filterParticipant = participant1.filterString,
        ) should not be empty
        participant1.topology.party_to_participant_mappings.list(
          daId,
          filterParty = "Bertram",
          filterParticipant = participant2.id.filterString,
        ) shouldBe empty
        participant1.topology.party_to_participant_mappings.list(
          daId,
          filterParty = "Bertram",
          proposals = true,
        ) shouldBe empty
        participant1.topology.party_to_participant_mappings.list(
          daId,
          filterParty = "Bertram",
          operation = Some(TopologyChangeOp.Remove),
        ) shouldBe empty
      }

    }

    "query migration announcements" in { implicit env =>
      import env.*

      val upgradeTime = CantonTimestamp.now().plusSeconds(60)

      val announcementMapping = synchronizerOwners1
        .map { owner =>
          owner.topology.synchronizer_upgrade.announcement.propose(
            PhysicalSynchronizerId(daId, NonNegativeInt.two, testedProtocolVersion),
            upgradeTime,
          )
        }
        .headOption
        .value
        .mapping

      eventually() {
        forAll(
          synchronizerOwners1.map(
            _.topology.synchronizer_upgrade.announcement
              .list(daId)
              .loneElement
              .item
          )
        )(result => result shouldBe announcementMapping)
      }
      synchronizerOwners1.foreach(
        _.topology.synchronizer_upgrade.announcement.revoke(
          PhysicalSynchronizerId(daId, NonNegativeInt.two, testedProtocolVersion),
          upgradeTime,
        )
      )
    }

    "issue topology transactions concurrently" in { implicit env =>
      import env.*

      val partiesF = (1 to 100).map { idx =>
        Future {
          blocking {
            val partyId = PartyId(participant1.uid.tryChangeId(s"party$idx"))
            participant1.topology.party_to_participant_mappings.propose(
              partyId,
              newParticipants = List(participant1.id -> ParticipantPermission.Submission),
              store = daId,
            )
            partyId
          }
        }
      }
      val parties =
        Await.result(Future.sequence(partiesF), 300.seconds).map(_.identifier.unwrap).toSet
      eventually(timeUntilSuccess = 60.seconds) {
        val registered =
          participant1.topology.party_to_participant_mappings
            .list(daId)
            .map(_.item.partyId.identifier.unwrap)
            .toSet
        (parties -- registered) shouldBe empty
      }

      eventually(timeUntilSuccess = 60.seconds) {
        // wait for the ledger api server to be notified of the parties
        // ... mostly to avoid flakes due to warning logs from LedgerServerPartyNotifier during shutdown
        val registeredOnLedgerApi =
          participant1.ledger_api.parties.list().map(_.party.identifier.unwrap).toSet
        (parties -- registeredOnLedgerApi) shouldBe empty
      }

    }

    "generate transactions for external signing" in { implicit env =>
      import env.*

      // Illustrate external signing of topology transactions by asking p1 to generate a namespace delegation
      // and party to participant transactions so we can onboard a new party
      // For simplicity in the test we'll manage max's key on the participant, but it can entirely be managed externally
      val signingKey = Await.result(
        participant1.crypto.privateCrypto
          .generateSigningKey(usage = SigningKeyUsage.NamespaceOnly)
          .valueOrFailShutdown("generate signing key"),
        10.seconds,
      )
      val max = PartyId.tryCreate("Max", Namespace(signingKey.fingerprint))
      val namespaceDelegationMapping = NamespaceDelegation.tryCreate(
        Namespace(signingKey.fingerprint),
        signingKey,
        CanSignAllMappings,
      )
      val partyHostingMapping = PartyToParticipant
        .create(
          partyId = max,
          threshold = PositiveInt.one,
          participants = Seq(
            HostingParticipant(
              env.participant1.id,
              ParticipantPermission.Confirmation,
            )
          ),
          partySigningKeysWithThreshold = None,
        )
        .value

      val transactions = participant1.topology.transactions.generate(
        Seq(
          GenerateTransactions.Proposal(
            namespaceDelegationMapping,
            TopologyStoreId.Authorized,
          ),
          GenerateTransactions.Proposal(
            partyHostingMapping,
            TopologyStoreId.Authorized,
          ),
        )
      )

      transactions should contain theSameElementsAs List(
        TopologyTransaction.apply(
          TopologyChangeOp.Replace,
          PositiveInt.one,
          namespaceDelegationMapping,
          testedProtocolVersion,
        ),
        TopologyTransaction.apply(
          TopologyChangeOp.Replace,
          PositiveInt.one,
          partyHostingMapping,
          testedProtocolVersion,
        ),
      )

      val Seq(preparedNamespaceTx, preparedHostingTx) = transactions

      def sign(
          tx: TopologyTransaction[TopologyChangeOp, TopologyMapping],
          clue: String,
          isProposal: Boolean = false,
      ) =
        Await.result(
          SignedTopologyTransaction
            .signAndCreate(
              tx,
              NonEmpty(Set, signingKey.fingerprint),
              isProposal = isProposal,
              participant1.crypto.privateCrypto,
              testedProtocolVersion,
            )
            .valueOrFailShutdown(clue),
          5.seconds,
        )

      // Assert participant hosts the party with the expected permission
      def assertHostingRelationship(
          participant: LocalParticipantReference,
          permission: ParticipantPermission,
      ) = {
        val hostingParticipants = participant.parties
          .hosted(filterParty = max.filterString)
          .find(_.party == max)
          .value
          .participants

        hostingParticipants
          .find(_.participant == participant.id)
          .value
          .synchronizers
          .find(_.synchronizerId == daId.logical)
          .value
          .permission shouldBe permission
      }

      def assertSignaturesOnPartyToParticipant(
          participant: LocalParticipantReference,
          signatures: Set[Fingerprint],
      ) =
        participant.topology.party_to_participant_mappings
          .list(daId)
          .find(_.item.partyId == max)
          .value
          .context
          .signedBy
          .forgetNE
          .toSet should contain theSameElementsAs signatures

      // Sign and load namespace tx
      val signedNamespaceTransaction = sign(preparedNamespaceTx, "Sign namespace transaction")
      participant1.topology.transactions
        .load(Seq(signedNamespaceTransaction), TopologyStoreId.Authorized)
      // Sign and load hosting tx
      val signedPreparedTx = sign(preparedHostingTx, "Sign hosting transaction")
      // It also needs to be signed by p1 because it's the hosting participant
      val p1SignedPreparedTx =
        participant1.topology.transactions.sign(
          Seq(signedPreparedTx),
          TopologyStoreId.Authorized,
          // normally we wouldn't have to specify the key here, but an earlier test
          // adds an IDD for participant1, so we specify the root namespace explicitly
          // so that we can assert on the signatures
          signedBy = Seq(participant1.id.fingerprint),
        )
      participant1.topology.transactions.load(p1SignedPreparedTx, TopologyStoreId.Authorized)

      eventually() {
        assertHostingRelationship(participant1, ParticipantPermission.Confirmation)
        assertSignaturesOnPartyToParticipant(
          participant1,
          Set(signingKey.fingerprint, participant1.id.fingerprint),
        )
      }

      // Now let's add p2 as a submitting participant for max
      // Create a new mapping with both p1 and p2
      val hostingTransaction2 = PartyToParticipant
        .create(
          partyId = max,
          threshold = PositiveInt.one,
          participants = Seq(
            HostingParticipant(
              env.participant1.id,
              ParticipantPermission.Confirmation,
            ),
            HostingParticipant(
              env.participant2.id,
              ParticipantPermission.Submission,
            ),
          ),
          partySigningKeysWithThreshold = None,
        )
        .value

      // Generate a new transaction for it
      val transactions2 = participant1.topology.transactions.generate(
        Seq(
          GenerateTransactions.Proposal(
            hostingTransaction2,
            TopologyStoreId.Authorized,
          )
        )
      )

      // Now we expect the serial returned to be 2 (because it's the second PartyToParticipant mapping for Max)
      transactions2 should contain theSameElementsAs List(
        TopologyTransaction.apply(
          TopologyChangeOp.Replace,
          PositiveInt.two,
          hostingTransaction2,
          testedProtocolVersion,
        )
      )

      val preparedHostingTx2 = transactions2.head

      // Sign it again with namespace key
      val signedPreparedTx2 =
        sign(preparedHostingTx2, "Sign hosting transaction", isProposal = true)
      // Sign with P2 - no need to sign again with P1 because it already signed the previous tx
      val p2SignedPreparedTx2: Seq[GenericSignedTopologyTransaction] =
        participant2.topology.transactions.sign(
          Seq(signedPreparedTx2),
          TopologyStoreId.Authorized,
          // normally we wouldn't have to specify the key here, but an earlier test
          // adds an IDD for participant1, so we specify the root namespace explicitly
          // so that we can assert on the signatures
          Seq(participant2.id.fingerprint),
        )

      // Load them to P1 and P2
      participant1.topology.transactions.load(p2SignedPreparedTx2, TopologyStoreId.Authorized)
      // We can't load to P2 to authorized store. They need to go directly into Synchronizer store
      // as otherwise the state processor check will complain because the serial doesn't start at the right
      // place
      participant2.topology.transactions
        .load(p2SignedPreparedTx2, TopologyStoreId.Synchronizer(sequencer1.synchronizer_id))

      eventually() {
        // Observe that Max is hosted on P1 with confirmation and P2 with submission
        assertHostingRelationship(participant1, ParticipantPermission.Confirmation)
        assertHostingRelationship(participant2, ParticipantPermission.Submission)
        val expectedSignatures =
          Set(signingKey.fingerprint, participant2.id.uid.namespace.fingerprint)
        assertSignaturesOnPartyToParticipant(participant1, expectedSignatures)
        assertSignaturesOnPartyToParticipant(participant2, expectedSignatures)
      }
    }
  }

  "correctly absorb additional signatures" in { implicit env =>
    import env.*
    // this test was used to reproduce and fix https://github.com/DACH-NY/canton-network-internal/issues/2116
    val storeId = daId
    val party = PartyId.tryCreate("AdditionalSiggy", participant1.id.namespace)

    participant1.topology.party_to_participant_mappings.propose(
      party,
      newParticipants = Seq(
        (participant1.id, ParticipantPermission.Submission),
        (participant2.id, ParticipantPermission.Confirmation),
      ),
      store = storeId,
    )

    val proposal = eventually() {
      participant2.topology.party_to_participant_mappings
        .list_hosting_proposals(sequencer1.synchronizer_id, participant2.id)
        .loneElement
    }
    val tx =
      participant2.topology.transactions.authorize(sequencer1.synchronizer_id, proposal.txHash)

    // now send the same tx but as a proposal with only one signature
    val tx2 = tx.copy(isProposal = true, signatures = NonEmpty.from(tx.signatures.drop(1)).value)

    logger.debug("Load transaction again")
    participant2.topology.transactions.load(Seq(tx2), storeId)

    eventually() {
      participant1.ledger_api.parties.list().map(_.party) should contain(party)
    }

    eventually() {
      val actualTx = participant2.topology.party_to_participant_mappings
        .list(
          TopologyStoreId.Synchronizer(sequencer1.synchronizer_id),
          timeQuery = TimeQuery.Range(None, None),
          filterParty = party.filterString,
        )

      val proposal = participant2.topology.party_to_participant_mappings
        .list(
          TopologyStoreId.Synchronizer(sequencer1.synchronizer_id),
          proposals = true,
          timeQuery = TimeQuery.Range(None, None),
          filterParty = party.filterString,
        )
      logger.debug(
        "Stored authorized\n  " + actualTx
          .map { c =>
            s"serial=${c.context.serial}, from=${c.context.validFrom}, until=${c.context.validUntil}, signedBy=${c.context.signedBy}, "
          }
          .mkString("\n  ")
      )

      logger.debug(
        "Stored proposal\n  " + proposal
          .map { c =>
            s"serial=${c.context.serial}, from=${c.context.validFrom}, until=${c.context.validUntil}, signedBy=${c.context.signedBy}, "
          }
          .mkString("\n  ")
      )

      actualTx should have length (2)
      forAll(actualTx.map(_.context.signedBy.forgetNE)) { sigs =>
        sigs should have length (2)
      }
      proposal.loneElement.context.signedBy.forgetNE should have length (1)

    }

  }

}

class TopologyManagementBftOrderingIntegrationTestPostgres
    extends TopologyManagementIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
