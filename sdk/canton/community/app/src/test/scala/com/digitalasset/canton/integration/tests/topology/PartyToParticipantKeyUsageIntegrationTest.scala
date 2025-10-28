// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.{Fingerprint, SigningKeyUsage, SigningKeysWithThreshold}
import com.digitalasset.canton.examples.java.cycle
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  PartyToParticipant,
  SignedTopologyTransaction,
  SingleTransactionSignature,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{ExternalParty, PartyId}

import java.util.UUID

class PartyToParticipantKeyUsageIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
    }

  "PartyToParticipant signing keys" should {
    "require Namespace usage for the self-authorizing key" in { implicit env =>
      import env.*

      // Namespace key with ProtocolOnly usage, should not work
      val namespaceKey =
        global_secret.keys.secret
          .generate_keys(PositiveInt.one, usage = SigningKeyUsage.ProtocolOnly)
          .head1
      val ptpMapping = PartyToParticipant.tryCreate(
        partyId = PartyId.tryCreate("alice", namespaceKey.fingerprint),
        threshold = PositiveInt.one,
        participants = Seq(HostingParticipant(participant1, Confirmation)),
        partySigningKeysWithThreshold = Some(
          SigningKeysWithThreshold.tryCreate(
            NonEmpty.mk(Seq, namespaceKey),
            PositiveInt.one,
          )
        ),
      )
      val ptpTx = TopologyTransaction(Replace, PositiveInt.one, ptpMapping, testedProtocolVersion)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        loadTransaction(ptpTx, NonEmpty.mk(Set, namespaceKey.fingerprint)),
        { le =>
          le.warningMessage should include("Transaction signature verification failed")
          le.mdc("error") should include(
            "keyUsage = Set(signing, proof-of-ownership), expectedKeyUsage = namespace)"
          )
        },
        _.errorMessage should include("Request failed for participant1"),
      )
    }

    "work with (Namespace, Protocol) usage for the self-authorizing key, and Protocol for the others" in {
      implicit env =>
        import env.*

        val namespaceKey =
          global_secret.keys.secret
            .generate_keys(
              PositiveInt.one,
              usage = NonEmpty.mk(Set, SigningKeyUsage.Namespace, SigningKeyUsage.Protocol),
            )
            .head1
        val protocolKey =
          global_secret.keys.secret
            .generate_keys(PositiveInt.one, usage = NonEmpty.mk(Set, SigningKeyUsage.Protocol))
            .head1

        val partyId = PartyId.tryCreate("alice", namespaceKey.fingerprint)
        val ptpMapping = PartyToParticipant.tryCreate(
          partyId = partyId,
          threshold = PositiveInt.one,
          participants = Seq(HostingParticipant(participant1, Confirmation)),
          partySigningKeysWithThreshold = Some(
            SigningKeysWithThreshold.tryCreate(
              NonEmpty.mk(Seq, namespaceKey, protocolKey),
              PositiveInt.two,
            )
          ),
        )
        val ptpTx = TopologyTransaction(Replace, PositiveInt.one, ptpMapping, testedProtocolVersion)
        loadTransaction(ptpTx, NonEmpty.mk(Set, namespaceKey.fingerprint, protocolKey.fingerprint))

        // Creating a contract with alice should work
        val aliceE = ExternalParty(
          partyId,
          NonEmpty.mk(Seq, namespaceKey.fingerprint, protocolKey.fingerprint),
          PositiveInt.two,
        )
        participant1.ledger_api.javaapi.commands.submit(
          Seq(aliceE),
          Seq(
            new cycle.Cycle(
              UUID.randomUUID().toString,
              partyId.toProtoPrimitive,
            ).create.commands.loneElement
          ),
        )
    }
  }

  private def loadTransaction(
      tx: GenericTopologyTransaction,
      signWith: NonEmpty[Set[Fingerprint]],
  )(implicit env: TestConsoleEnvironment) = {
    import env.*
    val signatures = signWith.map(
      global_secret.sign(
        tx.hash.hash.getCryptographicEvidence,
        _,
        SigningKeyUsage.All,
      )
    )
    val signedByParty = Seq(
      SignedTopologyTransaction
        .create(
          tx,
          signatures.map(
            SingleTransactionSignature(
              tx.hash,
              _,
            )
          ),
          isProposal = false,
          testedProtocolVersion,
        )
        .value
    )
    val signedByParticipant =
      participant1.topology.transactions.sign(signedByParty, synchronizer1Id)
    participant1.topology.transactions.load(
      signedByParticipant,
      synchronizer1Id,
    )
  }

}
