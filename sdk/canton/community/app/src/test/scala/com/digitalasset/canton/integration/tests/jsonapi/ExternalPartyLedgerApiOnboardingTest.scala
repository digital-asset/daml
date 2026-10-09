// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.v2.admin.party_management_service.GenerateExternalPartyTopologyResponse
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{SignatureFormat, SigningAlgorithmSpec}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.google.protobuf.ByteString

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.KeyPairGenerator
import java.util.Base64

class ExternalPartyLedgerApiOnboardingTest extends CommunityIntegrationTest with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)

      }

  private lazy val keyGen = KeyPairGenerator.getInstance("Ed25519")
  private lazy val keyPair = keyGen.generateKeyPair()
  private lazy val pb = keyPair.getPublic

  private lazy val signingPublicKey = com.digitalasset.canton.crypto.SigningPublicKey
    .create(
      format = com.digitalasset.canton.crypto.CryptoKeyFormat.DerX509Spki,
      key = ByteString.copyFrom(pb.getEncoded),
      keySpec = com.digitalasset.canton.crypto.SigningKeySpec.EcCurve25519,
      usage = com.digitalasset.canton.crypto.SigningKeyUsage.All,
    )
    .valueOrFail("failed to generate pubkey")

  private def generateSignature(bytes: ByteString) = {
    val signing = java.security.Signature.getInstance("Ed25519")
    signing.initSign(keyPair.getPrivate)
    signing.update(bytes.toByteArray)
    com.digitalasset.canton.crypto.Signature.create(
      format = SignatureFormat.Concat,
      signature = ByteString.copyFrom(signing.sign()),
      signedBy = signingPublicKey.fingerprint,
      signingAlgorithmSpec = Some(SigningAlgorithmSpec.Ed25519),
      signatureDelegation = None,
    )
  }

  private def verifyPartyCanBeAddressed(
      name: String
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    val module = "Canton.Internal.Ping"
    val party = participant1.parties.list(name).loneElement.party
    val pkg = participant1.packages.find_by_module(module).loneElement.packageId
    participant1.ledger_api.commands.submit(
      actAs = Seq(participant1.adminParty),
      commands = Seq(
        ledger_api_utils.create(
          pkg,
          module,
          "Ping",
          Map[String, Any](
            "id" -> "HelloYellow",
            "initiator" -> participant1.adminParty,
            "responder" -> party,
          ),
        )
      ),
    )
  }

  "onboard new single node hosted party via JSON api" in { implicit env =>
    // Send a JSON API request via HttpClient to verify that the JSON API works
    import env.*
    val port =
      participant1.config.httpLedgerApi.internalPort.valueOrFail("JSON API must be enabled")

    val jsonBody =
      """{
        |  "synchronizer": "%s",
        |  "partyHint": "Alice",
        |  "publicKey": {
        |     "format" : "CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO",
        |     "keyData": "%s",
        |     "keySpec" : "SIGNING_KEY_SPEC_EC_CURVE25519"
        |  }
        |}
        |""".stripMargin.formatted(
        sequencer1.synchronizer_id.toProtoPrimitive,
        Base64.getEncoder.encodeToString(pb.getEncoded),
      )

    def sendRequest(jsonBody: String, url: String) = {
      val client = HttpClient.newHttpClient()
      val request = HttpRequest
        .newBuilder()
        .uri(new URI(url))
        .header("Content-Type", "application/json") // Set the header for JSON data
        .POST(HttpRequest.BodyPublishers.ofString(jsonBody)) // Specify the method and body
        .build();
      val ret = client.send(request, HttpResponse.BodyHandlers.ofString())
      if (ret.statusCode() != 200) {
        fail(s"Request failed with ${ret.statusCode()} " + ret.body())
      } else ret
    }

    val response =
      sendRequest(jsonBody, s"http://localhost:$port/v2/parties/external/generate-topology")

    // decode response
    import com.digitalasset.canton.http.json.v2.JsPartyManagementCodecs.*
    val decoded = io.circe.parser
      .decode[GenerateExternalPartyTopologyResponse](response.body())
      .valueOrFail("failed to decode response")

    // sign hash
    val signing = java.security.Signature.getInstance("Ed25519")
    signing.initSign(keyPair.getPrivate)
    signing.update(decoded.multiHash.toByteArray)
    val signature = Base64.getEncoder.encodeToString(signing.sign())

    val onboardingTxs =
      decoded.topologyTransactions.map(x =>
        """{ "transaction" : "%s" }""".format(Base64.getEncoder.encodeToString(x.toByteArray))
      )
    // TODO(#27556) prefix the signature below with an A to get a very ugly error message
    // TODO(#27556) change the string "multiHashSignature" into anything else: you don't get a "unknown field" error!
    val responseBody =
      """{
        |  "synchronizer": "%s",
        |  "onboardingTransactions" : [
        |     %s
        |   ],
        |  "multiHashSignatures": [{
        |     "format" : "SIGNATURE_FORMAT_CONCAT",
        |     "signature": "%s",
        |     "signedBy" : "%s",
        |     "signingAlgorithmSpec" : "SIGNING_ALGORITHM_SPEC_ED25519"
        |  }]
        |}
        |""".stripMargin.formatted(
        sequencer1.synchronizer_id.toProtoPrimitive,
        onboardingTxs.mkString(",\n     "),
        signature,
        decoded.publicKeyFingerprint,
      )

    sendRequest(responseBody, s"http://localhost:$port/v2/parties/external/allocate")

    eventually() {
      participant1.parties.hosted("Alice") should not be empty
    }
    verifyPartyCanBeAddressed("Alice")

  }

  "onboard new single node hosted party via GRPC api" in { implicit env =>
    // Send a JSON API request via HttpClient to verify that the JSON API works
    import env.*

    val syncId = SynchronizerId.tryFromString(
      participant1.ledger_api.state
        .connected_synchronizers()
        .connectedSynchronizers
        .loneElement
        .synchronizerId
    )
    val txs = participant1.ledger_api.parties.generate_topology(
      syncId,
      "Bob",
      signingPublicKey,
    )

    participant1.ledger_api.parties.allocate_external(
      syncId,
      txs.topologyTransactions.map((_, Seq.empty[com.digitalasset.canton.crypto.Signature])),
      multiSignatures = Seq(generateSignature(txs.multiHash.getCryptographicEvidence)),
    )
    verifyPartyCanBeAddressed("Bob")

  }

  "onboard a new multi-hosted party entirely using lapi " in { implicit env =>
    import env.*

    val txs = participant1.ledger_api.parties.generate_topology(
      sequencer1.synchronizer_id,
      "David",
      signingPublicKey,
      otherConfirmingParticipantIds = Seq(participant2.id),
      confirmationThreshold = NonNegativeInt.two,
    )

    participant1.ledger_api.parties.allocate_external(
      sequencer1.synchronizer_id,
      txs.topologyTransactions.map((_, Seq.empty[com.digitalasset.canton.crypto.Signature])),
      multiSignatures = Seq(generateSignature(txs.multiHash.getCryptographicEvidence)),
    )

    participant2.ledger_api.parties.allocate_external(
      sequencer1.synchronizer_id,
      txs.topologyTransactions.map(x => (x, Seq.empty)),
      multiSignatures = Seq(),
    )

    eventually() {
      Seq(participant1, participant2).foreach(_.parties.hosted("David") should not be empty)
    }
    verifyPartyCanBeAddressed("David")

  }

  "onboard a new multi-hosted party with lapi request, admin api response" in { implicit env =>
    import env.*

    val txs = participant1.ledger_api.parties.generate_topology(
      sequencer1.synchronizer_id,
      "Charlie",
      signingPublicKey,
      otherConfirmingParticipantIds = Seq(participant2.id),
      confirmationThreshold = NonNegativeInt.two,
    )

    participant1.ledger_api.parties.allocate_external(
      sequencer1.synchronizer_id,
      txs.topologyTransactions.map((_, Seq.empty[com.digitalasset.canton.crypto.Signature])),
      multiSignatures = Seq(generateSignature(txs.multiHash.getCryptographicEvidence)),
    )

    eventually() {
      participant2.topology.party_to_participant_mappings
        .list_hosting_proposals(sequencer1.synchronizer_id, participant2.id) should not be empty
    }

    participant2.topology.party_to_participant_mappings
      .list_hosting_proposals(sequencer1.synchronizer_id, participant2.id)
      .foreach { request =>
        request.threshold.value shouldBe 2
        participant2.topology.transactions.authorize(
          sequencer1.synchronizer_id,
          request.txHash,
        )
      }

    eventually() {
      Seq(participant1, participant2).foreach(_.parties.hosted("Charlie") should not be empty)
    }
    verifyPartyCanBeAddressed("Charlie")

  }

}
