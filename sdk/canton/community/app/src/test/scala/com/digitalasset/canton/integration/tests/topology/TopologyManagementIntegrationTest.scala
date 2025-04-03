// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.*
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.admin.api.client.commands.TopologyAdminCommands.Write.GenerateTransactions
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, PositiveDurationSeconds}
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.PrivateKeyMetadata
import com.digitalasset.canton.examples.java.cycle as C
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
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
import com.digitalasset.canton.topology.TopologyManagerError.UnauthorizedTransaction
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
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

  protected val isBftSequencer: Boolean = false

  // TODO(#16283): disable participant / roll keys while the affected nodes are busy

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
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
        isRootDelegation = false,
      )

      eventually() {
        val nd3 = sequencer1.topology.namespace_delegations.list(store = daId)
        assertResult(1, nd3.map(_.item))(nd3.length - nd.length)
      }

      // add a new identifier delegation with a new key for the sequencer, using the intermediate CA
      val identifierCAKey =
        sequencer1.keys.secret
          .generate_signing_key("identifier-ca", SigningKeyUsage.IdentityDelegationOnly)

      val idt = sequencer1.topology.identifier_delegations.list(daId)
      sequencer1.topology.identifier_delegations.propose(
        daId.unwrap,
        identifierCAKey,
        store = daId,
      )
      eventually() { () =>
        val idt2 = sequencer1.topology.identifier_delegations.list(
          daId,
          operation = Some(TopologyChangeOp.Replace),
        )
        assert(idt.length + 1 == idt2.length)
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

    }

    "not add the same party twice" in { implicit env =>
      import env.*

      def add() = participant1.topology.party_to_participant_mappings.propose(
        PartyId(participant1.uid.tryChangeId("Boris")),
        newParticipants = List(participant1.id -> ParticipantPermission.Submission),
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

    "cannot disable a party if the threshold is not met anymore" in { implicit env =>
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
          "cannot meet threshold of 2 confirming participants with participants"
        ),
      )
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
        isRootDelegation = false,
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
          IdentifierDelegation(participant1.uid, key1),
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
            OwnerToKeyMapping(sequencer1.id, NonEmpty(Seq, key)),
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
          IdentifierDelegation(participant1.uid, key1),
          testedProtocolVersion,
        ),
        sig,
      )

      val tx1 = create(1)
      val tx2 = create(2)

      // steal the sig of tx2 and add it to tx1
      val fakeTx = tx1.addSingleSignatures(tx2.signatures.map(_.signature))
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
          isRootDelegation = false,
        )

        val key2 =
          participant1.keys.secret
            .generate_signing_key("key2", SigningKeyUsage.NamespaceOnly)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.topology.namespace_delegations.propose_delegation(
            participant1.namespace,
            key2,
            isRootDelegation = false,
            signedBy = Seq(key1.fingerprint),
            // use the force flag so that we actually get to the point where the topology transaction
            // is rejected, instead of failing the request early during signing key validation
            forceFlags = ForceFlags(ForceFlag.AllowUnvalidatedSigningKeys),
          ),
          _.shouldBeCantonError(
            UnauthorizedTransaction,
            _ should include(s"No delegation found for keys ${key1.fingerprint}"),
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
            isRootDelegation = true,
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

      val timestamp = env.environment.clock.now

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

      // WARNING: Skipping BFT Ordering Sequencer because it implements BFT Time, which does not depend on the current
      // clock time for any given block; it depends on previous block times. Therefore, the current clock time cannot be
      // used to query for a snapshot.
      if (!isBftSequencer) clue(s"querying snapshot at $timestamp") {
        // querying for snapshots should work
        val sp = participant1.topology.namespace_delegations
          .list(
            daId,
            timeQuery = TimeQuery.Snapshot(timestamp),
            filterNamespace = key1.fingerprint.unwrap,
          )
          .map(_.item.namespace.fingerprint)
        sp shouldBe Seq(key1.fingerprint)
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

    "query identifier delegations" in { implicit env =>
      import env.*

      val key =
        participant1.keys.secret
          .generate_signing_key(usage = SigningKeyUsage.IdentityDelegationOnly)
      participant1.topology.identifier_delegations.propose(
        participant1.uid,
        key,
      )
      eventually() {
        participant1.topology.identifier_delegations
          .list(
            store = TopologyStoreId.Authorized,
            filterUid = participant1.filterString,
          )
          .map(_.item.target) should contain(key)
      }
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
        isRootDelegation = true,
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
          .find(_.synchronizerId == daId)
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
      participant2.topology.transactions.load(p2SignedPreparedTx2, TopologyStoreId.Authorized)

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
}

class TopologyManagementReferenceIntegrationTestPostgres extends TopologyManagementIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class TopologyManagementBftOrderingIntegrationTestPostgres
    extends TopologyManagementIntegrationTest {

  override protected val isBftSequencer: Boolean = true

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
