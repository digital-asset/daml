// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import cats.syntax.option.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionAndWaitResponse,
  HashingSchemeVersion,
  PrepareSubmissionResponse,
}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.examples.java.cycle
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.{ParticipantId, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.UUID

trait PartyToParticipantAuthIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.synchronizers.connect_local(sequencer1, alias = daName)
      participant3.synchronizers.connect_local(sequencer1, alias = daName)

      participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
    }

  private def loadPartyToParticipant(
      partyId: PartyId,
      protocolSigningKeys: NonEmpty[Seq[SigningPublicKey]],
      signingFingerprints: NonEmpty[Seq[Fingerprint]],
      hostingParticipants: Seq[HostingParticipant],
      signingParticipants: Seq[LocalParticipantReference],
      serial: PositiveInt,
      topologyChangeOp: TopologyChangeOp,
      signingThreshold: PositiveInt,
  )(implicit
      env: FixtureParam
  ): Unit = {
    import env.*

    val partyToParticipantTx = topologyTransaction(
      PartyToParticipant
        .tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          participants = hostingParticipants,
          partySigningKeysWithThreshold = SigningKeysWithThreshold
            .tryCreate(
              keys = protocolSigningKeys,
              threshold = signingThreshold,
            )
            .some,
        ),
      serial = serial,
      topologyChangeOp = topologyChangeOp,
    )

    val signatures = signingFingerprints.map { fingerprint =>
      sign(partyToParticipantTx, fingerprint)
    }

    val topologyTransactionSignatures = signatures.map { signature =>
      SingleTransactionSignature(partyToParticipantTx.hash, signature)
    }

    val signedPartyToParticipant: GenericSignedTopologyTransaction = SignedTopologyTransaction
      .withTopologySignatures(
        partyToParticipantTx,
        NonEmpty.mk(
          Seq,
          topologyTransactionSignatures.head,
          topologyTransactionSignatures.tail*
        ),
        isProposal = false,
        protocolVersion = testedProtocolVersion,
      )

    val fullySignedTx =
      signingParticipants.foldLeft(Seq(signedPartyToParticipant)) { (txs, participant) =>
        participant.topology.transactions.sign(txs, synchronizer1Id)
      }

    participant1.topology.transactions.load(
      transactions = fullySignedTx,
      store = synchronizer1Id,
    )
  }

  // Creates a PTP transaction with signing keys, then loads it and tests if it was successful
  def testLoadingPartyToParticipantSigningKeys(
      partyId: PartyId,
      protocolSigningKeys: NonEmpty[Seq[SigningPublicKey]],
      signingFingerprints: NonEmpty[Seq[Fingerprint]],
      hostingParticipants: Seq[HostingParticipant],
      signingParticipants: Seq[LocalParticipantReference] = Seq.empty,
      serial: PositiveInt = PositiveInt.one,
      topologyChangeOp: TopologyChangeOp = TopologyChangeOp.Replace,
      signingThreshold: PositiveInt = PositiveInt.one,
  )(implicit
      env: FixtureParam
  ): Assertion = {
    import env.*

    loadPartyToParticipant(
      partyId = partyId,
      protocolSigningKeys = protocolSigningKeys,
      signingFingerprints = signingFingerprints,
      signingThreshold = signingThreshold,
      signingParticipants = signingParticipants,
      hostingParticipants = hostingParticipants,
      serial = serial,
      topologyChangeOp = topologyChangeOp,
    )

    val result = participant1.topology.party_to_participant_mappings
      .list(synchronizer1Id, filterParty = partyId.filterString, operation = topologyChangeOp.some)
      .loneElement

    result.item.partyId shouldBe partyId
    result.item.partySigningKeys.toSeq should contain theSameElementsAs protocolSigningKeys
    result.item.participants should contain theSameElementsAs hostingParticipants
  }

  "PartyToParticipant auth mechanism" should {
    "allow a self-authorized transaction with a single signing key" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId =
        PartyId(UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint))

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
        signingParticipants = Seq(participant1),
        hostingParticipants = Seq(hostingParticipant(participant1)),
      )
    }

    "allow a self-authorized transaction with multiple signing keys" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val keys = namespaceKey +: generateProtocolSigningKeys(2)
      val partyId =
        PartyId(UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint))

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = keys,
        signingFingerprints = keys.map(_.fingerprint),
        signingThreshold = PositiveInt.three,
        signingParticipants = Seq(participant1),
        hostingParticipants = Seq(hostingParticipant(participant1)),
      )
    }

    "not allow a self-signed transaction when the participant keys are missing" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      val newSigningKeys = generateProtocolSigningKeys(3)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        intercept[CommandFailure] {
          testLoadingPartyToParticipantSigningKeys(
            partyId = partyId,
            protocolSigningKeys = newSigningKeys,
            signingFingerprints = (namespaceKey +: newSigningKeys).map(_.fingerprint),
            signingParticipants = Seq(participant1),
            hostingParticipants = Seq(
              hostingParticipant(participant1.id),
              hostingParticipant(participant2.id), // Not signed by participant2
            ),
          )
        },
        logEntrySeqAssertion(
          detailed = "Topology transaction is missing authorizations by namespaces"
        ),
      )
    }

    "overwrite the PartyToKeyMapping keys using the PartyToParticipant keys" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      // Load the first signing key the old way (NamespaceDelegation + PartyToKeyMapping)
      loadNamespaceDelegation(
        partyId = partyId,
        partyNamespaceKey = namespaceKey,
        fingerprint = namespaceKey.fingerprint,
        participant = participant1,
      )

      val signedPartyToKey = signedTopologyTransaction(
        PartyToKeyMapping.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          signingKeys = NonEmpty.mk(Seq, namespaceKey),
        ),
        Seq(namespaceKey.fingerprint),
      )

      participant1.topology.transactions.load(
        transactions = Seq(signedPartyToKey),
        store = synchronizer1Id,
      )

      val newSigningKey = generateSigningKey(forNamespace = false)

      // Load the PartyToParticipant transaction with the new key
      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = NonEmpty.mk(Seq, newSigningKey),
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey, newSigningKey).map(_.fingerprint),
        signingParticipants = Seq(participant1),
        hostingParticipants = Seq(hostingParticipant(participant1)),
      )

      // The new key should succeed because PTP takes precedence over PTK
      createContractWithSigningKey(participant1, partyId, newSigningKey)

      loggerFactory.assertLogs(
        // The first key should not succeed
        intercept[CommandFailure] {
          createContractWithSigningKey(participant1, partyId, namespaceKey)
        },
        _.errorMessage should include(
          "Received 0 valid signatures from distinct keys (1 invalid)"
        ),
      )
    }

    "require the expected auth when adding new signing keys backed by a namespace delegation" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        loadNamespaceDelegation(
          partyId = partyId,
          partyNamespaceKey = namespaceKey,
          fingerprint = namespaceKey.fingerprint,
          participant = participant1,
        )

        val allNewSigningKeys = generateProtocolSigningKeys(4)
        val firstTwoNewSigningKeys = NonEmpty.from(allNewSigningKeys.take(2)).value
        val firstThreeNewSigningKeys = NonEmpty.from(allNewSigningKeys.take(3)).value

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = firstTwoNewSigningKeys,
          signingFingerprints = (namespaceKey +: firstTwoNewSigningKeys).map(_.fingerprint),
          signingParticipants = Seq(participant1),
          hostingParticipants = Seq(hostingParticipant(participant1)),
        )

        // Party namespace key included, should work
        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = firstThreeNewSigningKeys,
          signingFingerprints =
            (namespaceKey +: NonEmpty.mk(Seq, allNewSigningKeys(2))).map(_.fingerprint),
          serial = PositiveInt.two,
          signingParticipants = Seq.empty,
          hostingParticipants = Seq(hostingParticipant(participant1)),
        )

        // Party namespace key not included, should fail
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          intercept[CommandFailure] {
            testLoadingPartyToParticipantSigningKeys(
              partyId = partyId,
              protocolSigningKeys = allNewSigningKeys,
              signingFingerprints = NonEmpty.mk(Seq, allNewSigningKeys(3).fingerprint),
              serial = PositiveInt.three,
              signingParticipants = Seq.empty,
              hostingParticipants = Seq(hostingParticipant(participant1)),
            )
          },
          logEntrySeqAssertion(
            detailed = "Topology transaction is not properly authorized by any namespace key"
          ),
        )
    }

    "require the expected auth when adding new signing keys and transaction is self-authorized" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        val allNewSigningKeys = generateProtocolSigningKeys(4)
        val firstTwoNewSigningKeys = NonEmpty.from(allNewSigningKeys.take(2)).value
        val firstThreeNewSigningKeys = NonEmpty.from(allNewSigningKeys.take(3)).value

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = namespaceKey +: firstTwoNewSigningKeys,
          signingFingerprints = (namespaceKey +: firstTwoNewSigningKeys).map(_.fingerprint),
          signingParticipants = Seq(participant1),
          hostingParticipants = Seq(hostingParticipant(participant1)),
        )

        // Key included, should work
        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = namespaceKey +: firstThreeNewSigningKeys,
          signingFingerprints =
            NonEmpty.mk(Seq, namespaceKey, allNewSigningKeys(2)).map(_.fingerprint),
          serial = PositiveInt.two,
          hostingParticipants = Seq(hostingParticipant(participant1)),
        )

        // Key not included, should fail
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          intercept[CommandFailure] {
            testLoadingPartyToParticipantSigningKeys(
              partyId = partyId,
              protocolSigningKeys = namespaceKey +: allNewSigningKeys,
              signingFingerprints =
                NonEmpty.mk(Seq, allNewSigningKeys(3).fingerprint), // Key not included on purpose
              serial = PositiveInt.three,
              hostingParticipants = Seq(hostingParticipant(participant1)),
            )
          },
          logEntrySeqAssertion(
            detailed = "Topology transaction is not properly authorized by any namespace key"
          ),
        )
    }

    "allow a participant to remove themselves unilaterally if the rest of the mapping is unchanged" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          signingParticipants = Seq(participant1, participant2),
          hostingParticipants = Seq(
            hostingParticipant(participant1.id),
            hostingParticipant(participant2.id),
          ),
        )

        participant2.topology.transactions.propose(
          mapping = PartyToParticipant.tryCreate(
            partyId = partyId,
            threshold = PositiveInt.one,
            participants = Seq(
              hostingParticipant(participant1.id)
            ),
            partySigningKeysWithThreshold = SigningKeysWithThreshold
              .tryCreate(
                keys = NonEmpty.mk(Seq, namespaceKey),
                threshold = PositiveInt.one,
              )
              .some,
          ),
          store = synchronizer1Id,
          signedBy = Seq.empty,
          serial = PositiveInt.two.some,
        )

        eventually() {
          val result = participant1.topology.party_to_participant_mappings
            .list(synchronizer1Id, filterParty = partyId.filterString)
            .loneElement
          result.item.participants.length shouldBe 1
          result.item.participants.head.participantId shouldBe participant1.id
        }
    }

    "require party authorization when removing a participant without the participant authorization" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          signingParticipants = Seq(participant1, participant2, participant3),
          hostingParticipants = Seq(
            hostingParticipant(participant1.id),
            hostingParticipant(participant2.id),
            hostingParticipant(participant3.id),
          ),
        )

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          hostingParticipants = Seq(
            hostingParticipant(participant1.id),
            hostingParticipant(participant2.id),
          ),
          serial = PositiveInt.two,
        )

        val newSigningKey = generateSigningKey(forNamespace = false)

        // Key not included, should fail
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          intercept[CommandFailure] {
            testLoadingPartyToParticipantSigningKeys(
              partyId = partyId,
              protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
              // Skipping party key on purpose, using another key because it can't be empty
              signingFingerprints = NonEmpty.mk(Seq, newSigningKey.fingerprint),
              hostingParticipants = Seq(
                hostingParticipant(participant1.id)
              ),
              serial = PositiveInt.three,
            )
          },
          logEntrySeqAssertion(
            detailed =
              "Topology transaction authorization cannot be verified due to missing namespace delegations for keys"
          ),
        )
    }

    "require the expected auth when changing the onboarding status" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      loadNamespaceDelegation(
        partyId = partyId,
        partyNamespaceKey = namespaceKey,
        fingerprint = namespaceKey.fingerprint,
        participant = participant1,
      )

      val newSigningKeys = generateProtocolSigningKeys(3)

      val participantsWithP2Onboarding = Seq(
        hostingParticipant(participant1.id),
        hostingParticipant(participant2.id, onboarding = true),
      )

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = newSigningKeys,
        signingFingerprints = (namespaceKey +: newSigningKeys).map(_.fingerprint),
        hostingParticipants = participantsWithP2Onboarding,
        signingParticipants = Seq(participant1, participant2),
      )

      val participantsWithNoneOnboarding = Seq(
        hostingParticipant(participant1.id),
        hostingParticipant(participant2.id, onboarding = false),
      )

      // Changing from true to false should require only participant2 fingerprint
      participant2.topology.transactions.propose(
        mapping = PartyToParticipant.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          participants = participantsWithNoneOnboarding,
          partySigningKeysWithThreshold = SigningKeysWithThreshold
            .tryCreate(
              keys = newSigningKeys,
              threshold = PositiveInt.one,
            )
            .some,
        ),
        store = synchronizer1Id,
        signedBy = Seq(participant2.fingerprint),
        serial = PositiveInt.two.some,
      )

      eventually() {
        val result = participant1.topology.party_to_participant_mappings
          .list(synchronizer1Id, filterParty = partyId.filterString)
          .loneElement
        result.item.partyId shouldBe partyId
        result.item.participants shouldBe participantsWithNoneOnboarding
      }

      // Changing back from false to true should fail
      loggerFactory.assertLogs(
        intercept[CommandFailure] {
          participant2.topology.transactions.propose(
            mapping = PartyToParticipant.tryCreate(
              partyId = partyId,
              threshold = PositiveInt.one,
              participants = participantsWithP2Onboarding,
              partySigningKeysWithThreshold = SigningKeysWithThreshold
                .tryCreate(
                  keys = newSigningKeys,
                  threshold = PositiveInt.one,
                )
                .some,
            ),
            store = synchronizer1Id,
            signedBy = Seq(participant2.fingerprint),
            serial = PositiveInt.three.some,
          )
        },
        _.errorMessage should include(
          "Could not find an appropriate signing key to issue the topology transaction"
        ),
      )
    }

    "require the expected auth when adding participants" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      loadNamespaceDelegation(
        partyId = partyId,
        partyNamespaceKey = namespaceKey,
        fingerprint = namespaceKey.fingerprint,
        participant = participant1,
      )

      val newSigningKeys = generateProtocolSigningKeys(3)

      val allHostingParticipants =
        Seq(participant1, participant2, participant3).map(hostingParticipant(_))

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = newSigningKeys,
        signingFingerprints = (namespaceKey +: newSigningKeys).map(_.fingerprint),
        hostingParticipants = Seq(hostingParticipant(participant1)),
        signingParticipants = Seq(participant1),
      )

      // Adding the 2nd participant when signing with p2 should work
      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = newSigningKeys,
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
        hostingParticipants = Seq(participant1, participant2).map(hostingParticipant(_)),
        signingParticipants = Seq(participant2),
        serial = PositiveInt.two,
      )

      // Adding the 3rd participant without signing with p3 should not work
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        intercept[CommandFailure] {
          testLoadingPartyToParticipantSigningKeys(
            partyId = partyId,
            protocolSigningKeys = newSigningKeys,
            signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
            hostingParticipants = allHostingParticipants,
            signingParticipants = Seq.empty, // No third participant on purpose
            serial = PositiveInt.three,
          )
        },
        logEntrySeqAssertion(
          detailed = "Topology transaction is missing authorizations by namespaces"
        ),
      )
    }

    /*
      1. NSD-RootCert             NS-ABC123 -> K-ABC123
      2. NSD                      NS-ABC123 -> K-XYZ789
      3. PTP:                     Alice@ABC123 -> keys=K-XYZ789 -> SignedBy K-XYZ789
      4. Remove intermediate NSD: NS-ABC123 -> K-XYZ789
      5. change PTP:              Alice@ABC123 -> keys=K-ABC123 -> SignedBy K-ABC123
     */
    "allow updates to self-signed PTP after intermediate namespace delegation has been revoked" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        val rootSignedNamespaceDelegation = loadNamespaceDelegation(
          partyId = partyId,
          partyNamespaceKey = namespaceKey,
          fingerprint = namespaceKey.fingerprint,
          participant = participant1,
        )

        NamespaceDelegation.isRootCertificate(rootSignedNamespaceDelegation) shouldBe true
        participant1.topology.namespace_delegations
          .list(synchronizer1Id, filterNamespace = partyId.namespace.filterString)
          .length shouldBe 1

        val delegatedNamespaceKey = generateSigningKey(forNamespace = true)
        val nonRootSignedNamespaceDelegation = loadNamespaceDelegation(
          partyId = partyId,
          partyNamespaceKey = delegatedNamespaceKey,
          fingerprint = namespaceKey.fingerprint,
          participant = participant1,
        )

        NamespaceDelegation.isRootCertificate(nonRootSignedNamespaceDelegation) shouldBe false
        participant1.topology.namespace_delegations
          .list(synchronizer1Id, filterNamespace = partyId.namespace.filterString)
          .length shouldBe 2

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, delegatedNamespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, delegatedNamespaceKey.fingerprint),
          hostingParticipants = Seq(hostingParticipant(participant1)),
          signingParticipants = Seq(participant1),
        )

        // Revoking the intermediate delegation should not prevent usage of the namespace key
        loadNamespaceDelegation(
          partyId = partyId,
          partyNamespaceKey = delegatedNamespaceKey,
          fingerprint = namespaceKey.fingerprint,
          participant = participant1,
          serial = PositiveInt.two,
          topologyChangeOp = TopologyChangeOp.Remove,
        )

        // We can re-create the PTP
        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          hostingParticipants = Seq(hostingParticipant(participant1)),
          signingParticipants = Seq.empty,
          serial = PositiveInt.two,
        )
    }

    /*
      1. NSD-RootCert            NS-ABC123 -> K-ABC123
      2. PTP:                    Alice@ABC123 -> keys=K-ABC123 -> SignedBy K-ABC123
      3. Remove NSD-RootCert     NS-ABC123 -> K-ABC123
      4. change PTP self-authorized: Alice@ABC123 -> keys=K-ABC123 -> SignedBy K-ABC123 => DISALLOWED
     */
    "not allow self-signing PTP after removal of the root certificate" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      val rootSignedNamespaceDelegation = loadNamespaceDelegation(
        partyId = partyId,
        partyNamespaceKey = namespaceKey,
        fingerprint = namespaceKey.fingerprint,
        participant = participant1,
      )

      NamespaceDelegation.isRootCertificate(rootSignedNamespaceDelegation) shouldBe true
      participant1.topology.namespace_delegations
        .list(synchronizer1Id, filterNamespace = partyId.namespace.filterString)
        .length shouldBe 1

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
        hostingParticipants = Seq(hostingParticipant(participant1)),
        signingParticipants = Seq(participant1),
      )

      loadNamespaceDelegation(
        partyId = partyId,
        partyNamespaceKey = namespaceKey,
        fingerprint = namespaceKey.fingerprint,
        participant = participant1,
        topologyChangeOp = TopologyChangeOp.Remove,
        serial = PositiveInt.two,
      )

      participant1.topology.namespace_delegations
        .list(
          synchronizer1Id,
          filterNamespace = partyId.namespace.filterString,
          operation = TopologyChangeOp.Remove.some,
        )
        .length shouldBe 1

      val newSigningKey = generateSigningKey(forNamespace = false)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        intercept[CommandFailure] {
          testLoadingPartyToParticipantSigningKeys(
            partyId = partyId,
            protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey, newSigningKey),
            signingFingerprints =
              NonEmpty.mk(Seq, namespaceKey.fingerprint, newSigningKey.fingerprint),
            hostingParticipants = Seq(hostingParticipant(participant1)),
            signingParticipants = Seq.empty,
            serial = PositiveInt.two,
          )
        },
        logEntrySeqAssertion(
          detailed = "has been revoked",
          level = Level.ERROR,
        ),
      )
    }

    /*
      1. PTP self-authorized: Alice@ABC123 -> keys=K-ABC123 -> SignedBy K-ABC123
      2. TopologyChangeOp=Remove: PTP self-authorized: Alice@ABC123 -> K-ABC123 -> SignedBy K-ABC123
      3. assert that party is not usable anymore in a Daml transaction
      4. create PTP self-authorized again: Alice@ABC123 -> keys=K-ABC123 -> SignedBy K-ABC123
      5. assert that party is usable again anymore in a Daml transaction
     */
    "allow removing and recreating a self-authorized party" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
        hostingParticipants = Seq(hostingParticipant(participant1)),
        signingParticipants = Seq(participant1),
      )

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
        hostingParticipants = Seq(hostingParticipant(participant1)),
        signingParticipants = Seq.empty,
        topologyChangeOp = TopologyChangeOp.Remove,
        serial = PositiveInt.two,
      )

      loggerFactory.assertLogs(
        intercept[CommandFailure] {
          createContractWithSigningKey(participant1, partyId, namespaceKey)
        },
        _.errorMessage should include(
          "The participant is not connected to any synchronizer where the given informees are known."
        ),
      )

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
        hostingParticipants = Seq(hostingParticipant(participant1)),
        signingParticipants = Seq(participant1),
        serial = PositiveInt.three,
      )

      createContractWithSigningKey(participant1, partyId, namespaceKey)
    }

    /*
      1. PTP self-authorized: Alice@ABC123 -> keys=K-ABC123 -> SignedBy K-ABC123
      2. PTP: replace K-ABC123 with another key in the list of signing keys in mapping-> SignedBy K-ABC123 -> rejected: not authorized
     */
    "not be able to authorize the removal of the self-signing namespace key" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      testLoadingPartyToParticipantSigningKeys(
        partyId = partyId,
        protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
        signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
        hostingParticipants = Seq(hostingParticipant(participant1)),
        signingParticipants = Seq(participant1),
      )

      val newSigningKey = generateSigningKey(forNamespace = false)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        intercept[CommandFailure] {
          testLoadingPartyToParticipantSigningKeys(
            partyId = partyId,
            protocolSigningKeys = NonEmpty.mk(Seq, newSigningKey),
            signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
            hostingParticipants = Seq(hostingParticipant(participant1)),
            signingParticipants = Seq.empty,
            serial = PositiveInt.two,
          )
        },
        logEntrySeqAssertion(
          detailed =
            " Topology transaction authorization cannot be verified due to missing namespace delegations for keys"
        ),
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        intercept[CommandFailure] {
          testLoadingPartyToParticipantSigningKeys(
            partyId = partyId,
            protocolSigningKeys = NonEmpty.mk(Seq, newSigningKey),
            signingFingerprints =
              NonEmpty.mk(Seq, namespaceKey.fingerprint, newSigningKey.fingerprint),
            hostingParticipants = Seq(hostingParticipant(participant1)),
            signingParticipants = Seq.empty,
            serial = PositiveInt.two,
          )
        },
        logEntrySeqAssertion(
          detailed = " Topology transaction is not properly authorized by any namespace key"
        ),
      )
    }

    /*
      1. PTP self-authorized: Alice@ABC123 -> keys=K-ABC123 -> SignedBy K-ABC123
      2. NSD-RootCert:    NS-ABC123 -> K-ABC123
      3. PTP:             replace K-ABC123 in the list of signing keys -> works
     */
    "allow replacing a PTP self-signing namespace key if the authorization can be validated via a root certificate" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          hostingParticipants = Seq(hostingParticipant(participant1)),
          signingParticipants = Seq(participant1),
        )

        val rootSignedNamespaceDelegation = loadNamespaceDelegation(
          partyId = partyId,
          partyNamespaceKey = namespaceKey,
          fingerprint = namespaceKey.fingerprint,
          participant = participant1,
        )

        NamespaceDelegation.isRootCertificate(rootSignedNamespaceDelegation) shouldBe true
        participant1.topology.namespace_delegations
          .list(synchronizer1Id, filterNamespace = partyId.namespace.filterString)
          .length shouldBe 1

        val newSigningKey = generateSigningKey(forNamespace = false)

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, newSigningKey),
          signingFingerprints = NonEmpty.mk(
            Seq,
            newSigningKey.fingerprint,
            namespaceKey.fingerprint,
          ),
          hostingParticipants = Seq(hostingParticipant(participant1)),
          signingParticipants = Seq.empty,
          serial = PositiveInt.two,
        )
    }

    "require all participants signatures for a self signed PTP" in { implicit env =>
      import env.*

      val namespaceKey = generateSigningKey(forNamespace = true)
      val partyId = PartyId(
        UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
      )

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        intercept[CommandFailure] {
          testLoadingPartyToParticipantSigningKeys(
            partyId = partyId,
            protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
            signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
            hostingParticipants =
              Seq(hostingParticipant(participant1), hostingParticipant(participant2)),
            // Only sign with P1
            signingParticipants = Seq(participant1),
          )
        },
        logEntrySeqAssertion(
          detailed = " Topology transaction is missing authorizations"
        ),
      )
    }

    "allow recreating a previously revoked self signed PTP as long as no corresponding NSD has been revoked" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          hostingParticipants = Seq(hostingParticipant(participant1)),
          signingParticipants = Seq(participant1),
        )

        // Revoke it
        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          hostingParticipants = Seq(hostingParticipant(participant1)),
          signingParticipants = Seq.empty,
          topologyChangeOp = Remove,
          serial = PositiveInt.two,
        )

        // Recreate it
        testLoadingPartyToParticipantSigningKeys(
          partyId = partyId,
          protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
          signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
          hostingParticipants = Seq(hostingParticipant(participant1)),
          signingParticipants = Seq(participant1),
          serial = PositiveInt.three,
        )
    }

    "disallow self-signed PTP creation by revoking the namespace key ahead of time" in {
      implicit env =>
        import env.*

        val namespaceKey = generateSigningKey(forNamespace = true)
        val partyId = PartyId(
          UniqueIdentifier.tryCreate("test", namespaceKey.fingerprint)
        )

        loadNamespaceDelegation(
          partyId,
          namespaceKey,
          partyId.fingerprint,
          participant1,
          PositiveInt.one,
          TopologyChangeOp.Remove,
        )

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          intercept[CommandFailure] {
            testLoadingPartyToParticipantSigningKeys(
              partyId = partyId,
              protocolSigningKeys = NonEmpty.mk(Seq, namespaceKey),
              signingFingerprints = NonEmpty.mk(Seq, namespaceKey.fingerprint),
              hostingParticipants = Seq(hostingParticipant(participant1)),
              signingParticipants = Seq(participant1),
            )
          },
          logEntrySeqAssertion(
            detailed = " has been revoked",
            level = Level.ERROR,
          ),
        )
    }
  }

  private def logEntrySeqAssertion(detailed: String, level: Level = Level.WARN) =
    LogEntry.assertLogSeq(
      Seq(
        (
          // Hardcoded error message, since it's the same for all scenarios
          _.errorMessage should include("Request failed for participant1"),
          "Expected generic error",
        ),
        (
          { le =>
            le.message should include(detailed)
            le.level shouldBe level
          },
          "Expected warning message with more details",
        ),
      )
    )

  private def loadNamespaceDelegation(
      partyId: PartyId,
      partyNamespaceKey: SigningPublicKey,
      fingerprint: Fingerprint,
      participant: LocalParticipantReference,
      serial: PositiveInt = PositiveInt.one,
      topologyChangeOp: TopologyChangeOp = TopologyChangeOp.Replace,
  )(implicit env: FixtureParam): SignedTopologyTransaction[TopologyChangeOp, TopologyMapping] = {
    import env.*

    val signedNamespaceDelegation = signedTopologyTransaction(
      NamespaceDelegation.tryCreate(
        namespace = partyId.namespace,
        target = partyNamespaceKey,
        restriction = DelegationRestriction.CanSignAllMappings,
      ),
      Seq(fingerprint),
      serial = serial,
      topologyChangeOp = topologyChangeOp,
    )

    participant.topology.transactions.load(
      transactions = Seq(signedNamespaceDelegation),
      store = synchronizer1Id,
    )

    signedNamespaceDelegation
  }

  private def generateProtocolSigningKeys(numberOfKeys: Int)(implicit
      env: FixtureParam,
      traceContext: TraceContext,
  ): NonEmpty[Seq[SigningPublicKey]] = {
    val signingKeys = Seq.fill(numberOfKeys)(generateSigningKey(forNamespace = false))
    NonEmpty.from(signingKeys).value
  }

  private def generateSigningKey(forNamespace: Boolean)(implicit
      env: FixtureParam,
      traceContext: TraceContext,
  ): SigningPublicKey =
    env.tryGlobalCrypto
      .generateSigningKey(usage =
        NonEmpty(Set, SigningKeyUsage.Protocol) ++ Option.when(forNamespace)(
          SigningKeyUsage.Namespace
        )
      )
      .futureValueUS
      .value

  private def signedTopologyTransaction(
      topologyMapping: TopologyMapping,
      fingerprints: Seq[Fingerprint],
      serial: PositiveInt = PositiveInt.one,
      topologyChangeOp: TopologyChangeOp = TopologyChangeOp.Replace,
  )(implicit env: FixtureParam): SignedTopologyTransaction[TopologyChangeOp, TopologyMapping] = {
    val tx =
      topologyTransaction(topologyMapping, serial = serial, topologyChangeOp = topologyChangeOp)

    val topologySignatures = fingerprints.map { fingerprint =>
      val signature = sign(tx, fingerprint)
      SingleTransactionSignature(tx.hash, signature)
    }

    SignedTopologyTransaction.withTopologySignatures(
      tx,
      NonEmpty.mk(Seq, topologySignatures.head, topologySignatures.tail*),
      isProposal = false,
      protocolVersion = testedProtocolVersion,
    )
  }

  private def topologyTransaction[M <: TopologyMapping](
      mapping: M,
      serial: PositiveInt,
      topologyChangeOp: TopologyChangeOp,
  ): TopologyTransaction[TopologyChangeOp, M] =
    TopologyTransaction(
      topologyChangeOp,
      serial = serial,
      mapping = mapping,
      protocolVersion = testedProtocolVersion,
    )

  private def sign[Op <: TopologyChangeOp, M <: TopologyMapping](
      topologyTransaction: TopologyTransaction[Op, M],
      fingerprint: Fingerprint,
  )(implicit env: FixtureParam): Signature =
    signBytes(topologyTransaction.hash.hash.getCryptographicEvidence, fingerprint)

  private def signBytes(
      bytes: ByteString,
      fingerprint: Fingerprint,
  )(implicit env: FixtureParam): Signature =
    env.tryGlobalCrypto.privateCrypto
      .signBytes(
        bytes,
        fingerprint,
        SigningKeyUsage.NamespaceOrProofOfOwnership,
      )
      .futureValueUS
      .value

  private def createContractWithSigningKey(
      participant: ParticipantReference,
      partyId: PartyId,
      signingKey: SigningPublicKey,
  )(implicit env: FixtureParam): ExecuteSubmissionAndWaitResponse = {
    val prepared: PrepareSubmissionResponse =
      participant.ledger_api.javaapi.interactive_submission.prepare(
        Seq(partyId),
        Seq(
          new cycle.Cycle(
            UUID.randomUUID().toString,
            partyId.toProtoPrimitive,
          ).create.commands.loneElement
        ),
      )

    val signature = signBytes(prepared.preparedTransactionHash, signingKey.fingerprint)

    participant.ledger_api.interactive_submission.execute_and_wait(
      prepared.preparedTransaction.value,
      Map(partyId -> Seq(signature)),
      UUID.randomUUID().toString,
      HashingSchemeVersion.HASHING_SCHEME_VERSION_V2,
    )
  }

  private def hostingParticipant(participantId: ParticipantId, onboarding: Boolean = false) =
    HostingParticipant(
      participantId = participantId,
      permission = ParticipantPermission.Confirmation,
      onboarding = onboarding,
    )
}

class PartyToParticipantAuthIntegrationTestPostgres extends PartyToParticipantAuthIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
