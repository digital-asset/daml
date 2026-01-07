// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.config.{CacheConfig, CryptoConfig, SessionEncryptionKeyCacheConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.jce.JceCrypto
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullTransactionViewTree,
  LightTransactionViewTree,
}
import com.digitalasset.canton.participant.protocol.ProcessingSteps.DecryptedViews
import com.digitalasset.canton.participant.protocol.submission.EncryptedViewMessageFactory
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.protocol.messages.EncryptedViewMessage
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  ExampleTransactionFactory,
  ViewHash,
}
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients, WithRecipients}
import com.digitalasset.canton.store.SessionKeyStoreWithNoEviction
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{ParticipantId, TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*

class ViewMessageDecrypterTest extends BaseTestWordSpec with HasExecutionContext {

  private class Env(
      interceptEncryptedViewKeys: Seq[AsymmetricEncrypted[SecureRandomness]] => Seq[
        AsymmetricEncrypted[SecureRandomness]
      ] = identity,
      interceptSubviewKeyRandomness: Seq[Seq[SecureRandomness]] => Seq[Seq[SecureRandomness]] =
        identity,
      interceptEncryptedViewMessages: Seq[EncryptedViewMessage[TransactionViewType.type]] => Seq[
        EncryptedViewMessage[TransactionViewType.type]
      ] = identity,
      interceptFullTree: Seq[FullTransactionViewTree] => Seq[FullTransactionViewTree] = identity,
  ) {

    val participantId: ParticipantId = ParticipantId("participant")

    val jceCrypto: Crypto = {
      val config = CryptoConfig()
      JceCrypto
        .create(
          config,
          CryptoSchemes.tryFromConfig(config),
          SessionEncryptionKeyCacheConfig(),
          CacheConfig(PositiveNumeric.tryCreate(1)),
          new InMemoryCryptoPrivateStore(testedReleaseProtocolVersion, loggerFactory),
          new InMemoryCryptoPublicStore(loggerFactory),
          timeouts,
          loggerFactory,
        )
        .value
    }

    val pureCrypto: CryptoPureApi = jceCrypto.pureCrypto

    val snapshot: SynchronizerSnapshotSyncCryptoApi = {
      val identityFactory: TestingIdentityFactory = new TestingIdentityFactory(
        TestingTopology(participants = Map(participantId -> ParticipantAttributes(Submission))),
        jceCrypto,
        loggerFactory,
        List(
          WithValidity(
            CantonTimestamp.MinValue,
            None,
            DynamicSynchronizerParameters.defaultValues(testedProtocolVersion),
          )
        ),
      )

      identityFactory
        .forOwnerAndSynchronizer(participantId)
        .currentSnapshotApproximation
        .futureValueUS
    }

    val parent: Int = 0
    val child: Int = 1
    val allViewIndices: Seq[Int] = Seq(parent, child)

    val fullTree: Seq[FullTransactionViewTree] = {
      val exampleTransactionFactory: ExampleTransactionFactory = new ExampleTransactionFactory(
        pureCrypto
      )()

      val fullTree =
        exampleTransactionFactory.MultipleRootsAndSimpleViewNesting.transactionViewTrees.drop(1)
      fullTree(parent).subviewHashes.loneElement shouldBe fullTree(child).viewHash

      interceptFullTree(fullTree)
    }

    def mkRandomness(): SecureRandomness =
      pureCrypto.generateSecureRandomness(pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes)

    val randomness: Seq[SecureRandomness] = Seq(mkRandomness(), mkRandomness())
    val subviewKeyRandomness: Seq[Seq[SecureRandomness]] = interceptSubviewKeyRandomness(
      Seq(Seq(randomness(child)), Seq.empty)
    )

    def mkViewKeyData(
        viewKeyRandomness: SecureRandomness
    ): (SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]]) = {
      val viewKey: SymmetricKey = pureCrypto.createSymmetricKey(viewKeyRandomness).value
      val encryptedViewKeys: Seq[AsymmetricEncrypted[SecureRandomness]] =
        snapshot
          .encryptFor(viewKeyRandomness, Seq(participantId))
          .futureValueUS
          .value
          .values
          .toSeq
      val modifiedEncryptedViewKeys = interceptEncryptedViewKeys(encryptedViewKeys)
      (viewKey, modifiedEncryptedViewKeys)
    }

    val viewKeyData: Seq[(SymmetricKey, Seq[AsymmetricEncrypted[SecureRandomness]])] =
      randomness.map(mkViewKeyData)

    val lightTree: Seq[LightTransactionViewTree] = allViewIndices.map(i =>
      LightTransactionViewTree
        .fromTransactionViewTree(fullTree(i), subviewKeyRandomness(i), testedProtocolVersion)
        .value
    )

    val encryptedViewMessage: Seq[EncryptedViewMessage[TransactionViewType.type]] =
      interceptEncryptedViewMessages(
        allViewIndices.map(i =>
          EncryptedViewMessageFactory
            .create(TransactionViewType)(
              lightTree(i),
              viewKeyData(i),
              snapshot,
              None,
              testedProtocolVersion,
            )
            .futureValueUS
            .value
        )
      )

    val recipients: Recipients = Recipients.cc(participantId)
    val allEnvelopes: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]]] =
      NonEmpty
        .from(encryptedViewMessage.map(OpenEnvelope(_, recipients)(testedProtocolVersion)))
        .value
    val onlyChildEnvelopes
        : NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[TransactionViewType.type]]]] =
      NonEmpty(Seq, allEnvelopes(child))

    val decrypter: ViewMessageDecrypter = new ViewMessageDecrypter(
      participantId,
      testedProtocolVersion,
      new SessionKeyStoreWithNoEviction(),
      snapshot,
      futureSupervisor,
      loggerFactory,
    )
  }

  "ViewMessageDecrypter" can {

    "successfully decrypt all view messages" in {

      val env = new Env()
      import env.*

      val decryptedViews = decrypter.decryptViews(allEnvelopes).futureValueUS.value
      inside(decryptedViews) { case DecryptedViews(views, decryptionErrors) =>
        forEvery(views.zipWithIndex) {
          case ((WithRecipients(view, actualRecipients), optSignature), i) =>
            view shouldBe lightTree(i)
            actualRecipients shouldBe recipients
            optSignature shouldBe encryptedViewMessage(i).submittingParticipantSignature
        }
        views should have size allViewIndices.size.toLong

        decryptionErrors shouldBe empty
      }
    }

    "fail on decryption errors" in {
      // Note: it would be desirable to filter out envelopes with decryption errors instead of failing.

      val env = new Env(
        interceptEncryptedViewKeys = _.map(encryptedKey =>
          encryptedKey
            .focus(_.ciphertext)
            .replace(ByteString.fromHex("DEADBEEFDEADBEEFDEADBEEFDEADBEEF"))
        )
      )
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(onlyChildEnvelopes).value,
        _.getMessage should startWith(
          s"Can't decrypt the randomness of the view with hash ${encryptedViewMessage(child).viewHash} where I'm allegedly an informee. " +
            s"SyncCryptoDecryptError(\n  FailedToDecrypt(\n    org.bouncycastle.jcajce.provider.util.BadBlockException"
        ),
      )
    }

    "fail on with missing view keys" in {
      // Note: It would be desirable to filter out envelopes that use unknown keys (according to the topology state)

      val env = new Env(
        interceptEncryptedViewKeys = _.map(encryptedKey =>
          encryptedKey
            .focus(_.encryptedFor)
            .replace(Fingerprint.tryFromString("Nudelsuppe"))
        )
      )
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(onlyChildEnvelopes).value,
        _.getMessage shouldBe s"Can't decrypt the randomness of the view with hash ${encryptedViewMessage(child).viewHash} where I'm allegedly an informee. " +
          s"MissingParticipantKey(PAR::participant::default)",
      )
    }

    "crash on missing private keys" in {
      // Note: If the private key is missing, the participant needs to crash to avoid a ledger fork.
      //  The operator needs to upload the missing key and reconnect to the synchronizer.

      val env = new Env()
      import env.*

      val (_, encryptedViewKeys) = viewKeyData(child)
      // Remove the private key from the store
      val fingerprint = encryptedViewKeys.loneElement.encryptedFor
      jceCrypto.cryptoPrivateStore
        .existsPrivateKey(fingerprint, KeyPurpose.Encryption)
        .futureValueUS shouldBe Right(true)
      jceCrypto.cryptoPrivateStore.removePrivateKey(fingerprint).futureValueUS.value

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(onlyChildEnvelopes).value,
        _.getMessage shouldBe s"Can't decrypt the randomness of the view with hash ${encryptedViewMessage(child).viewHash} where I'm allegedly an informee. " +
          s"PrivateKeyStoreVerificationError(FailedToReadKey(matching private key does not exist))",
      )
    }

    "fail if the randomness of an EncryptedViewMessage does not match the randomness in the parent tree" in {
      // Note: It is desirable to keep the child view and discard the parent view in this case.

      val dummyRandomness = SecureRandomness.fromByteString(4)(ByteString.fromHex("DEADBEEF")).value
      val env = new Env(interceptSubviewKeyRandomness = _ => Seq(Seq(dummyRandomness), Seq.empty))
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(allEnvelopes).value,
        _.getMessage shouldBe s"View ${encryptedViewMessage(child).viewHash} has different encryption keys associated with it. " +
          s"(Previous: Some(Success(Outcome(${randomness(child)}))), new: $dummyRandomness)",
      )
    }

    "fail if different encrypted view messages contain the same view with different randomnesses" in {
      // Note: it would be desirable to keep the messages instead.

      val env = new Env(
        interceptFullTree = trees => Seq(trees(1), trees(1)),
        interceptSubviewKeyRandomness = _ => Seq(Seq.empty, Seq.empty),
      )
      import env.*

      loggerFactory.assertInternalErrorAsyncUS[IllegalArgumentException](
        decrypter.decryptViews(allEnvelopes).value,
        _.getMessage shouldBe s"View ${encryptedViewMessage(child).viewHash} has different encryption keys associated with it. " +
          s"(Previous: Some(Success(Outcome(${randomness(parent)}))), new: ${randomness(child)})",
      )
    }

    "successfully decrypt even if the view hash of an EncryptedViewMessage does not match the view hash of the contained tree" in {
      // Note: It is desirable to discard the envelope instead.

      val dummyViewHash = ViewHash(
        Hash.digest(
          HashPurpose.MerkleTreeInnerNode,
          ByteString.fromHex("DEADBEEF"),
          HashAlgorithm.Sha256,
        )
      )
      val env = new Env(
        interceptEncryptedViewMessages = _.map(
          _.focus(_.viewHash)
            .replace(
              dummyViewHash
            )
        )
      )
      import env.*

      val decryptedViews = decrypter.decryptViews(onlyChildEnvelopes).futureValueUS.value
      inside(decryptedViews) { case DecryptedViews(views, decryptionErrors) =>
        val (WithRecipients(view, outputRecipients), optSignature) = views.loneElement
        view shouldBe lightTree(child)
        outputRecipients shouldBe recipients
        optSignature shouldBe encryptedViewMessage(child).submittingParticipantSignature
        decryptionErrors shouldBe empty
      }
    }
  }
}
