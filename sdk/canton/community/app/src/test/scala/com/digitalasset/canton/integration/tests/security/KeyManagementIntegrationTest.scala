// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import cats.syntax.functorFilter.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.*
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{CommandFailure, InstanceReference, LocalInstanceReference}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.SigningKeyUsage.matchesRelevantUsages
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations
import com.digitalasset.canton.util.OptionUtil
import org.scalatest.Assertion

trait KeyManagementIntegrationTestHelper extends KeyManagementTestHelper {
  self: BaseIntegrationTest =>

  protected def waitForKeyTopologyUpdate(
      nodes: Seq[InstanceReference],
      owner: Member,
      keyId: Fingerprint,
      expectExistence: Boolean,
  ): Unit =
    eventually() {
      nodes.foreach { ref =>
        val listOfSigningKeys = ref.keys.public
          .list_by_owner(owner)
          .flatMap(_.keys(KeyPurpose.Signing).map(_.id))

        listOfSigningKeys.contains(keyId) shouldBe expectExistence
      }
    }

  protected def rotateNodeKeysTest(asset: String): SecurityTest = SecurityTest(
    property = Authenticity,
    asset = asset,
    Attack(
      actor = "a network participant",
      threat = s"impersonates a $asset with compromised signing keys",
      mitigation = "rotate the keys",
    ),
  )

  protected def rotateNamespaceKeysTest(asset: String): SecurityTest = SecurityTest(
    property = Authenticity,
    asset = s"$asset namespace",
    Attack(
      actor = "a network participant",
      threat =
        s"impersonates any entity under the namespace by authorizing topology transactions with the compromised namespace signing keys",
      mitigation = "rotate a namespace intermediate keys",
    ),
  )

  protected def setupNamespaceIntermediateKey(node: InstanceReference): SigningPublicKey = {

    val namespaceDelegations =
      node.topology.namespace_delegations.list(
        store = TopologyStoreId.Authorized,
        filterNamespace = node.namespace.toProtoPrimitive,
      )

    // architecture-handbook-entry-begin: CreateNamespaceIntermediateKey

    // create a new namespace intermediate key
    val intermediateKey = node.keys.secret.generate_signing_key(
      s"${node.name}-intermediate-${System.currentTimeMillis()}",
      SigningKeyUsage.NamespaceOnly,
    )

    // Create a namespace delegation for the intermediate key with the namespace root key
    node.topology.namespace_delegations.propose_delegation(
      node.namespace,
      intermediateKey,
      CanSignAllButNamespaceDelegations,
    )

    // architecture-handbook-entry-end: CreateNamespaceIntermediateKey

    // Check that the new namespace delegations appears
    eventually() {
      val updatedNamespaceDelegations = node.topology.namespace_delegations.list(
        store = TopologyStoreId.Authorized,
        filterNamespace = node.namespace.toProtoPrimitive,
      )
      assertResult(1, updatedNamespaceDelegations)(
        updatedNamespaceDelegations.length - namespaceDelegations.length
      )
    }

    intermediateKey
  }

  protected def rotateIntermediateNamespaceKeyAndPing(
      node: InstanceReference,
      kmsRotationKeyIdO: Option[String],
      setupIntermediateKey: InstanceReference => SigningPublicKey = setupNamespaceIntermediateKey,
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*

    assertPingSucceeds(participant2, participant1)

    // Create a namespace intermediate key for the participant namespace
    val rootKey =
      node.keys.secret
        .list(filterName = s"${node.name}-namespace")
        .headOption
        .getOrElse(fail("Cannot get namespace signing key"))
        .publicKey

    val intermediateKey = setupIntermediateKey(node)

    // To test the namespace key rotation, we assign a new node key to a node authorized with the new intermediate key
    // and remove the previous node key that was authorized by the previous intermediate key.

    // Find the current signing key
    val currentSigningKey =
      getCurrentKey(node, KeyPurpose.Signing, Some(SigningKeyUsage.ProtocolOnly))

    val newSigningKey = kmsRotationKeyIdO match {
      case Some(kmsKeyId) =>
        node.keys.secret.register_kms_signing_key(
          kmsKeyId,
          SigningKeyUsage.ProtocolOnly,
          s"${node.name}-signing-new",
        )
      case None =>
        node.keys.secret
          .generate_signing_key(s"${node.name}-signing-new", SigningKeyUsage.ProtocolOnly)
    }

    // Authorize the new key with the intermediate namespace key
    node.topology.owner_to_key_mappings.add_key(
      newSigningKey.fingerprint,
      KeyPurpose.Signing,
      signedBy = Seq(intermediateKey.fingerprint, newSigningKey.fingerprint),
    )

    waitForKeyTopologyUpdate(
      nodes.all,
      node.id.member,
      newSigningKey.fingerprint,
      expectExistence = true,
    )

    // Remove the previous signing key authorized by the namespace root key, now only a signing key authorized by the namespace intermediate key exists
    node.topology.owner_to_key_mappings.remove_key(
      currentSigningKey.fingerprint,
      currentSigningKey.purpose,
      signedBy = Seq(intermediateKey.fingerprint),
    )

    waitForKeyTopologyUpdate(
      nodes.all,
      node.id.member,
      currentSigningKey.fingerprint,
      expectExistence = false,
    )

    // Create a new namespace intermediate key
    val newIntermediateKey = setupIntermediateKey(node)

    // architecture-handbook-entry-begin: RotateNamespaceIntermediateKey

    // Remove the previous intermediate key
    node.topology.namespace_delegations.propose_revocation(
      node.namespace,
      intermediateKey,
    )

    // architecture-handbook-entry-end: RotateNamespaceIntermediateKey

    // ensure that all nodes now see the relevant delegations on the synchronizer
    eventually() {
      nodes.all.foreach { ref =>
        val delegations = ref.topology.namespace_delegations
          .list(
            store = daId,
            filterNamespace = rootKey.id.toProtoPrimitive,
          )
        val delegatedKeys = delegations.map(_.item.target.fingerprint)

        delegatedKeys shouldNot contain(intermediateKey.fingerprint)
        delegatedKeys shouldBe Seq(rootKey.fingerprint, newIntermediateKey.fingerprint)
      }
    }

    // We should still be able to ping with the participant1 signing key authorized by the newIntermediateKey
    assertPingSucceeds(participant2, participant1)
  }

  protected def rotateNodeKeyAndPing(
      node: InstanceReference,
      newKeyName: String,
      purpose: KeyPurpose,
  )(implicit
      env: TestConsoleEnvironment
  ): Assertion = {
    import env.*

    val defaultSigningKeyUsage: Set[SigningKeyUsage] = SigningKeyUsage.ProtocolOnly

    assertPingSucceeds(participant2, participant1)

    // Get the key owner id from the node id
    val owner = node.id match {
      case owner: Member => owner
      case unknown =>
        fail(s"Unknown key owner: $unknown")
    }

    // Find the current key in the node's store
    val currentKey = purpose match {
      case KeyPurpose.Signing =>
        getCurrentKey(node, purpose, Some(NonEmptyUtil.fromUnsafe(defaultSigningKeyUsage)))
      case KeyPurpose.Encryption => getCurrentKey(node, purpose, None)
    }

    // Generate a new key on the node
    val newKey = purpose match {
      case KeyPurpose.Signing =>
        node.keys.secret.generate_signing_key(newKeyName, defaultSigningKeyUsage)
      case KeyPurpose.Encryption => node.keys.secret.generate_encryption_key(newKeyName)
    }

    // Rotate the key for the node through the identity manager
    node.topology.owner_to_key_mappings.rotate_key(
      owner,
      currentKey,
      newKey,
    )

    // check that only the new key is listed before running a ping
    eventually() {
      nodes.all.foreach { ref =>
        val inspect = ref.keys.public
          .list_by_owner(owner)
          .flatMap(_.keys(purpose).mapFilter {
            case pubKey @ (_: EncryptionPublicKey) => Some(pubKey.fingerprint)
            case pubKey @ SigningPublicKey(_, _, _, usage, _) =>
              if (matchesRelevantUsages(usage, currentKey.asInstanceOf[SigningPublicKey].usage))
                Some(pubKey.fingerprint)
              else None
            case _ => fail("Unknown key type")
          })
        assertResult(Seq(newKey.fingerprint))(inspect)
      }
    }

    // check that ping still works
    assertPingSucceeds(participant2, participant1)
  }
}

/** Tests the management (specifically rotation) of various cryptographic keys. */
sealed trait KeyManagementIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with KeyManagementIntegrationTestHelper
    with HasCycleUtils
    with SecurityTestSuite
    with AccessTestScenario
    with KeyManagementTestHelper {

  // TODO(test-coverage): disable participant / roll keys while the affected nodes are busy

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableRemoteMediators("mediator1", "remoteMediator1"),
        // Disable ACS commitment processing, so that the sequencer is idle when keys get deleted
        ConfigTransforms.useStaticTime,
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      }

  "A Canton operator" can {

    "rotate mediator signing key" taggedAs rotateNodeKeysTest("mediator node") in { implicit env =>
      import env.*
      rotateNodeKeyAndPing(
        mediator1,
        "mediator1-signing-rotated",
        purpose = KeyPurpose.Signing,
      )
    }

    "rotate sequencer signing key" taggedAs rotateNodeKeysTest("sequencer node") in {
      implicit env =>
        import env.*
        rotateNodeKeyAndPing(
          sequencer1,
          "sequencer1-signing-rotated",
          purpose = KeyPurpose.Signing,
        )
    }

    "rotate participant signing keys" taggedAs rotateNodeKeysTest("participant node") in {
      implicit env =>
        import env.*
        rotateNodeKeyAndPing(
          participant1,
          "participant1-signing-rotated",
          purpose = KeyPurpose.Signing,
        )
    }

    "rotate participant encryption keys" taggedAs rotateNodeKeysTest("participant node") in {
      implicit env =>
        import env.*
        rotateNodeKeyAndPing(
          participant1,
          "participant1-encryption-rotated",
          purpose = KeyPurpose.Encryption,
        )
    }

    "create participant namespace intermediate key and rotate the key" taggedAs rotateNamespaceKeysTest(
      "participant"
    ) in { implicit env =>
      import env.*

      rotateIntermediateNamespaceKeyAndPing(participant1, None)
    }

    "create sequencer namespace intermediate key and rotate the key" taggedAs rotateNamespaceKeysTest(
      "sequencer"
    ) in { implicit env =>
      import env.*

      rotateIntermediateNamespaceKeyAndPing(sequencer1, None)
    }

    def downloadUploadTest(
        node: LocalInstanceReference,
        keyNameO: Option[String],
        usageO: Option[NonEmpty[Set[SigningKeyUsage]]] = None,
    ) = {

      val privateKey = node.keys.secret
        .list(
          filterName = OptionUtil.noneAsEmptyString(keyNameO),
          filterPurpose = Set(KeyPurpose.Signing),
        )
        .head
      val privateKeyName = privateKey.name
      val privateKeyId = privateKey.publicKey.id
      val privateKeyNew = node.keys.secret.download(privateKeyId)

      // If the usage is set, we forcefully replace the keys' usage.
      val keyPair = usageO match {
        case Some(usage) =>
          val ckp = CryptoKeyPair
            .fromTrustedByteString(privateKeyNew)
            .valueOrFail("parsing of crypto key pair")
            .asInstanceOf[SigningKeyPair]
            .replaceUsage(usage)
          ckp.privateKey.usage shouldBe usage
          ckp
        case None =>
          CryptoKeyPair
            .fromTrustedByteString(privateKeyNew)
            .valueOrFail("parsing of crypto key pair")
      }

      // The old format was just the protobuf message serialized to bytestring
      val privateKeyOld = keyPair.toProtoCryptoKeyPairV30.toByteString

      val force = true
      // Delete the secret key before re-uploading
      // user-manual-entry-begin: DeletePrivateKey
      node.keys.secret.delete(privateKeyId, force)
      // user-manual-entry-end: DeletePrivateKey
      node.keys.secret.list(filterFingerprint = privateKeyId.unwrap) should have size 0

      node.keys.secret.upload(privateKeyOld, privateKeyName.map(_.unwrap))

      node.keys.secret.list(filterFingerprint = privateKeyId.unwrap) should have size 1

    }

    "download and upload private keys locally" in { implicit env =>
      import env.*
      downloadUploadTest(sequencer1, None)
    }

    // This test covers a scenario where an older key, exported from a different Canton version,
    // is missing certain key usages (i.e., `ProofOfOwnership`).
    // It checks that the key is updated with the correct usage when uploaded.
    "upload old private keys adds ProofOfOwnership if missing" in { implicit env =>
      import env.*

      val keyUsageNoPoO = NonEmpty.mk(
        Set,
        SigningKeyUsage.Namespace,
        SigningKeyUsage.SequencerAuthentication,
        SigningKeyUsage.Protocol,
      )

      val keyName = "key-with-no-PoO"

      participant1.crypto
        .generateSigningKey(
          name = Some(KeyName.tryCreate(keyName)),
          usage = SigningKeyUsage.ProtocolOnly,
        )
        .futureValueUS
        .valueOrFail("generate signing key")

      downloadUploadTest(participant1, Some(keyName), Some(keyUsageNoPoO))

      val privateKeyMetadata = participant1.keys.secret.list(filterName = keyName).head

      // Uploading the key will trigger an update of its usage, i.e. adding `ProofOfOwnership`.
      privateKeyMetadata.publicKey.asSigningKey.valueOrFail("not a signing key").usage shouldBe
        keyUsageNoPoO ++ SigningKeyUsage.ProofOfOwnershipOnly

      // To add a key to the OwnerToKeyMappings, the key must be able to sign itself to prove ownership.
      participant1.topology.owner_to_key_mappings
        .add_key(
          privateKeyMetadata.publicKey.id,
          KeyPurpose.Signing,
          participant1.member,
        )

      waitForKeyTopologyUpdate(
        nodes.all,
        participant1,
        privateKeyMetadata.publicKey.fingerprint,
        expectExistence = true,
      )

      // Delete key
      participant1.topology.owner_to_key_mappings.remove_key(
        privateKeyMetadata.publicKey.fingerprint,
        KeyPurpose.Signing,
        participant1.member,
      )

      waitForKeyTopologyUpdate(
        nodes.all,
        participant1,
        privateKeyMetadata.publicKey.fingerprint,
        expectExistence = false,
      )

      participant1.keys.secret.delete(privateKeyMetadata.id, force = true)
    }

    "download and upload private keys remotely" in { implicit env =>
      import env.*

      val remoteMediator1 = rm("remoteMediator1")

      val privateKey = remoteMediator1.keys.secret.list().head
      val privateKeyName = privateKey.name
      val privateKeyId = privateKey.publicKey.id
      val privateKeyNew = remoteMediator1.keys.secret.download(privateKeyId)

      // Delete the secret key before re-uploading
      remoteMediator1.keys.secret.delete(privateKeyId, force = true)
      remoteMediator1.keys.secret
        .list(filterFingerprint = privateKeyId.unwrap) should have size 0

      remoteMediator1.keys.secret.upload(privateKeyNew, privateKeyName.map(_.unwrap))

      remoteMediator1.keys.secret
        .list(filterFingerprint = privateKeyId.unwrap) should have size 1
    }

    "rotate a node's keys using the rotate_node_key macro" in { implicit env =>
      import env.*

      val currentKeys = participant1.keys.secret.list()
      val p2Key =
        // user-manual-entry-begin: ListPublicKeys
        participant2.keys.secret
          .list()
          // user-manual-entry-end: ListPublicKeys
          .find(_.publicKey.fingerprint != participant2.fingerprint)
          .valueOrFail("could not find a valid key to rotate")

      // Rotate a participant's keys
      // user-manual-entry-begin: RotateNodeKeys
      participant1.keys.secret.rotate_node_keys()
      // user-manual-entry-end: RotateNodeKeys

      // user-manual-entry-begin: RotateNodeKey
      val p2KeyNew = participant2.keys.secret.rotate_node_key(
        p2Key.publicKey.fingerprint.unwrap,
        "key_rotated",
      )
      // user-manual-entry-end: RotateNodeKey

      val (p2StoredPubKey, p2StoredPubKeyName) = participant2.keys.secret
        .list(filterFingerprint = p2KeyNew.fingerprint.unwrap)
        .map(key => (key.publicKey, key.name))
        .loneElement

      p2StoredPubKey shouldBe p2KeyNew
      p2StoredPubKeyName shouldBe Some("key_rotated")

      p2KeyNew should not be p2Key.publicKey

      val newKeys = participant1.keys.secret.list()
      currentKeys should not equal newKeys

      rotateAndTest(sequencer1, environment.clock.now)
      rotateAndTest(mediator1, environment.clock.now)

      // Rotate the synchronizer keys again (and use snippet for documentation)
      // user-manual-entry-begin: RotateNodeKeys2
      sequencer1.keys.secret.rotate_node_keys()
      mediator1.keys.secret.rotate_node_keys()
      // user-manual-entry-end: RotateNodeKeys2

      participant1.health.ping(participant2)
    }

    "list with correct filter parameters" in { implicit env =>
      import env.*

      participant1.keys.secret.list(
        filterFingerprint = participant1.fingerprint.unwrap
      ) should have size 1

      val signingKeys = participant1.keys.secret.list(filterPurpose = Set(KeyPurpose.Signing))
      signingKeys.forall(_.purpose == KeyPurpose.Signing) shouldBe true
      val encryptionKeys = participant1.keys.secret.list(filterPurpose = Set(KeyPurpose.Encryption))
      encryptionKeys.forall(_.purpose == KeyPurpose.Encryption) shouldBe true

      participant1.keys.secret
        .list(filterPurpose = Set(KeyPurpose.Encryption, KeyPurpose.Signing))
        .map(_.id)
        .toSet shouldBe
        encryptionKeys.map(_.id).toSet ++ signingKeys.map(_.id).toSet

      val newKey = participant1.keys.secret
        .generate_signing_key(usage = SigningKeyUsage.ProtocolOnly ++ SigningKeyUsage.NamespaceOnly)

      participant1.keys.secret
        .list(filterUsage = SigningKeyUsage.ProtocolOnly)
        .map(_.id) should contain(newKey.id)
      participant1.keys.secret
        .list(filterUsage = SigningKeyUsage.SequencerAuthenticationOnly)
        .map(_.id) should not contain newKey.id

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.keys.secret.list(
          filterPurpose = Set(KeyPurpose.Encryption),
          filterUsage = SigningKeyUsage.ProtocolOnly,
        ),
        _.errorMessage should include("Cannot specify a usage when listing encryption keys"),
      )
    }
  }
}

class KeyManagementReferenceIntegrationTestPostgres extends KeyManagementIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}

class KeyManagementBftOrderingIntegrationTestPostgres extends KeyManagementIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
