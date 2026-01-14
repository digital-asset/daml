// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.BigDecimalImplicits.*
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CryptoConfig, CryptoProvider}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.crypto.{
  CryptoKeyPair,
  CryptoPureApi,
  EncryptionPrivateKey,
  EncryptionPublicKey,
  KeyPurpose,
  SecureRandomness,
}
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.config.SynchronizerParametersConfig
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

trait LedgerTransparencyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with SecurityTestHelpers
    with HasCycleUtils {

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()

  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*

        val staticSynchronizerParameters = StaticSynchronizerParameters.fromConfig(
          SynchronizerParametersConfig(),
          CryptoConfig(provider = CryptoProvider.Jce),
          testedProtocolVersion,
        )

        val network = NetworkTopologyDescription.createWithStaticSynchronizerParameters(
          daName,
          synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
          synchronizerThreshold = PositiveInt.two,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          staticSynchronizerParameters =
            staticSynchronizerParameters.copy(enableTransparencyChecks = true),
        )

        new NetworkBootstrapper(network)
      }
      .withSetup { implicit env =>
        import env.*
        pureCryptoRef.set(participant1.crypto.pureCrypto)
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
      }

  "Synchronizer with transparency checks enabled" should {
    "views are deterministically encrypted" in { implicit env =>
      import env.*

      // download public and private key for participants
      val p1EncId =
        participant1.keys.public.list(filterPurpose = Set(KeyPurpose.Encryption)).loneElement.id
      val p1EncKeyPair = CryptoKeyPair
        .fromTrustedByteString(participant1.keys.secret.download(p1EncId))
        .valueOrFail("parsing of participant1 encryption key pair")

      val p2EncId =
        participant2.keys.public.list(filterPurpose = Set(KeyPurpose.Encryption)).loneElement.id
      val p2EncKeyPair = CryptoKeyPair
        .fromTrustedByteString(participant2.keys.secret.download(p2EncId))
        .valueOrFail("parsing of participant2 encryption key pair")

      val createIouCmd = new Iou(
        participant1.adminParty.toProtoPrimitive,
        participant2.adminParty.toProtoPrimitive,
        new Amount(100.toBigDecimal, "USD"),
        List.empty.asJava,
      ).create.commands.asScala.toSeq

      // extract encrypted view messages
      checkingEncryptedViewMessages(pureCrypto, participant1, sequencer1) {
        participant1.ledger_api.javaapi.commands
          .submit(Seq(participant1.adminParty), createIouCmd)
      } { encryptedViewMessages =>
        // we only expect one encrypted view
        val encryptedViewMessage = encryptedViewMessages.loneElement
        val viewEncryptionKeyRandomness = encryptedViewMessage.viewEncryptionKeyRandomness

        // the session encryption key for the ping message must be encrypted for both `participant1`
        // and `participant2`
        viewEncryptionKeyRandomness
          .map(_.encryptedFor)
          .forgetNE should contain allOf (p1EncId, p2EncId)

        val p1Ciphertext = viewEncryptionKeyRandomness
          .find(_.encryptedFor == p1EncId)
          .valueOrFail("no encrypted randomness for participant1")

        val randomnessP1 = pureCrypto
          .decryptWith(
            p1Ciphertext,
            p1EncKeyPair.privateKey.asInstanceOf[EncryptionPrivateKey],
          )(
            SecureRandomness.fromByteString(
              encryptedViewMessage.viewEncryptionScheme.keySizeInBytes
            )(_)
          )
          .valueOrFail("decrypt encrypted randomness for participant1")

        val p2Ciphertext = encryptedViewMessage.viewEncryptionKeyRandomness
          .find(_.encryptedFor == p2EncId)
          .valueOrFail("no encrypted randomness for participant2")

        val randomnessP2 = pureCrypto
          .decryptWith(
            p2Ciphertext,
            p2EncKeyPair.privateKey.asInstanceOf[EncryptionPrivateKey],
          )(
            SecureRandomness.fromByteString(
              encryptedViewMessage.viewEncryptionScheme.keySizeInBytes
            )(_)
          )
          .valueOrFail("decrypt encrypted randomness for participant2")

        // sanity check: both ciphertexts decrypt to the same randomness,
        // even though each was encrypted with a different key
        randomnessP1 shouldBe randomnessP2

        // verify determinism: re-encrypting the same randomness with each participant’s public key
        // must reproduce the original ciphertexts
        val encryptedRandomnessP1 = pureCrypto
          .encryptDeterministicWith(
            randomnessP1,
            p1EncKeyPair.publicKey.asInstanceOf[EncryptionPublicKey],
          )
          .valueOrFail("re-encrypt randomness for participant1")

        val encryptedRandomnessP2 = pureCrypto
          .encryptDeterministicWith(
            randomnessP2,
            p2EncKeyPair.publicKey.asInstanceOf[EncryptionPublicKey],
          )
          .valueOrFail("re-encrypt randomness for participant2")

        p1Ciphertext shouldBe encryptedRandomnessP1
        p2Ciphertext shouldBe encryptedRandomnessP2

      }
    }
  }

}

class LedgerTransparencyBftOrderingIntegrationTestDefault
    extends LedgerTransparencyIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))
}
