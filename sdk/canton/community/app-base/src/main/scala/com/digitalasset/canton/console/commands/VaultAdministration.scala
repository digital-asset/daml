// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.admin.api.client.commands.{TopologyAdminCommands, VaultAdminCommands}
import com.digitalasset.canton.admin.api.client.data.ListKeyOwnersResult
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleCommandResult,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReference,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.PrivateKeyMetadata
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{ExternalParty, Member, MemberCode, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

import java.time.Instant
import scala.concurrent.ExecutionContext

class SecretKeyAdministration(
    instance: InstanceReference,
    runner: AdminCommandRunner,
    override protected val consoleEnvironment: ConsoleEnvironment,
    override protected val loggerFactory: NamedLoggerFactory,
) extends Helpful
    with FeatureFlagFilter {

  import runner.*

  private def regenerateKey(currentKey: PublicKey, name: String): PublicKey =
    currentKey match {
      case encKey: EncryptionPublicKey =>
        instance.keys.secret.generate_encryption_key(
          keySpec = Some(encKey.keySpec),
          name = name,
        )
      case signKey: SigningPublicKey =>
        instance.keys.secret.generate_signing_key(
          usage = signKey.usage,
          keySpec = Some(signKey.keySpec),
          name = name,
        )
      case unknown => throw new IllegalArgumentException(s"Invalid public key type: $unknown")
    }

  @Help.Summary("List keys in private vault")
  @Help.Description("""Returns all public keys to the corresponding private keys in the key vault.
                      |Optional arguments can be used for filtering.""")
  def list(
      filterFingerprint: String = "",
      filterName: String = "",
      filterPurpose: Set[KeyPurpose] = Set.empty,
      filterUsage: Set[SigningKeyUsage] = Set.empty,
  ): Seq[PrivateKeyMetadata] =
    consoleEnvironment.run {
      adminCommand(
        VaultAdminCommands.ListMyKeys(filterFingerprint, filterName, filterPurpose, filterUsage)
      )
    }

  @Help.Summary("Generate new public/private key pair for signing and store it in the vault")
  @Help.Description(
    """
      |The optional name argument allows you to store an associated string for your convenience.
      |The usage specifies the intended use for the signing key that can be:
      | - Namespace: for the root namespace key that defines a node's identity and signs topology requests;
      | - SequencerAuthentication: for a signing key that authenticates members of the network towards a sequencer;
      | - Protocol: for a signing key that deals with all the signing that happens as part of the protocol.
      |The keySpec can be used to select a key specification, e.g., which elliptic curve to use, and the default spec is used if left unspecified."""
  )
  def generate_signing_key(
      name: String = "",
      usage: Set[SigningKeyUsage],
      keySpec: Option[SigningKeySpec] = None,
  ): SigningPublicKey =
    NonEmpty.from(usage) match {
      case Some(usageNE) =>
        consoleEnvironment.run {
          adminCommand(VaultAdminCommands.GenerateSigningKey(name, usageNE, keySpec))
        }
      case None => throw new IllegalArgumentException("no signing key usage specified")
    }

  @Help.Summary("Generate new public/private key pair for encryption and store it in the vault")
  @Help.Description(
    """
      |The optional name argument allows you to store an associated string for your convenience.
      |The keySpec can be used to select a key specification, e.g., which elliptic curve to use, and the default spec is used if left unspecified."""
  )
  def generate_encryption_key(
      name: String = "",
      keySpec: Option[EncryptionKeySpec] = None,
  ): EncryptionPublicKey =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.GenerateEncryptionKey(name, keySpec))
    }

  @Help.Summary(
    "Register the specified KMS signing key in canton storing its public information in the vault"
  )
  @Help.Description(
    """
      |The id for the KMS signing key.
      |The usage specifies the intended use for the signing key that can be:
      | - Namespace: for the root namespace key that defines a node's identity and signs topology requests;
      | - SequencerAuthentication: for a signing key that authenticates members of the network towards a sequencer;
      | - Protocol: for a signing key that deals with all the signing that happens as part of the protocol.
      |The optional name argument allows you to store an associated string for your convenience."""
  )
  def register_kms_signing_key(
      kmsKeyId: String,
      usage: Set[SigningKeyUsage],
      name: String = "",
  ): SigningPublicKey =
    NonEmpty.from(usage) match {
      case Some(usageNE) =>
        consoleEnvironment.run {
          adminCommand(VaultAdminCommands.RegisterKmsSigningKey(kmsKeyId, usageNE, name))
        }
      case None => throw new IllegalArgumentException("no signing key usage specified")
    }

  @Help.Summary(
    "Register the specified KMS encryption key in canton storing its public information in the vault"
  )
  @Help.Description(
    """
      |The id for the KMS encryption key.
      |The optional name argument allows you to store an associated string for your convenience."""
  )
  def register_kms_encryption_key(
      kmsKeyId: String,
      name: String = "",
  ): EncryptionPublicKey =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.RegisterKmsEncryptionKey(kmsKeyId, name))
    }

  private def findPublicKey(
      fingerprint: String,
      topologyAdmin: TopologyAdministrationGroup,
      owner: Member,
  ): PublicKey =
    findPublicKeys(topologyAdmin, owner).find(_.fingerprint.unwrap == fingerprint) match {
      case Some(key) => key
      case None =>
        throw new IllegalStateException(
          s"The key $fingerprint does not exist"
        )
    }

  @Help.Summary("Rotate a given node's keypair with a new pre-generated KMS keypair")
  @Help.Description(
    """Rotates an existing encryption or signing key stored externally in a KMS with a pre-generated
      key. NOTE: A namespace root signing key CANNOT be rotated by this command.
      |The fingerprint of the key we want to rotate.
      |The id of the new KMS key (e.g. Resource Name).
      |An optional name for the new key."""
  )
  def rotate_kms_node_key(
      fingerprint: String,
      newKmsKeyId: String,
      name: String = "",
  ): PublicKey = {

    val owner = instance.id.member

    val currentKey = findPublicKey(fingerprint, instance.topology, owner)
    val newKey = currentKey match {
      case SigningPublicKey(_, _, _, usage, _) =>
        instance.keys.secret.register_kms_signing_key(newKmsKeyId, usage, name)
      case _: EncryptionPublicKey =>
        instance.keys.secret.register_kms_encryption_key(newKmsKeyId, name)
      case _ =>
        throw new IllegalStateException("Unsupported key type")
    }

    // Rotate the key for the node in the topology management
    instance.topology.owner_to_key_mappings.rotate_key(
      owner,
      currentKey,
      newKey,
    )
    newKey
  }

  @Help.Summary("Rotate a node's public/private key pair")
  @Help.Description(
    """Rotates an existing encryption or signing key. NOTE: A namespace root or intermediate
      signing key CANNOT be rotated by this command.
      |The fingerprint of the key we want to rotate.
      |An optional name for the new key."""
  )
  def rotate_node_key(fingerprint: String, name: String = ""): PublicKey = {
    val owner = instance.id.member

    val currentKey = findPublicKey(fingerprint, instance.topology, owner)

    val newName =
      if (name.isEmpty)
        generateNewNameForRotatedKey(fingerprint, consoleEnvironment.environment.clock)
      else name

    val newKey = regenerateKey(currentKey, newName)

    // Backup should be performed here if necessary

    // Rotate the key for the node in the topology management
    instance.topology.owner_to_key_mappings.rotate_key(
      owner,
      currentKey,
      newKey,
    )
    newKey
  }

  @Help.Summary("Rotate the node's public/private key pairs")
  @Help.Description(
    """
      |For a participant node it rotates the signing and encryption key pair.
      |For a sequencer or mediator node it rotates the signing key pair as those nodes do not have an encryption key pair.
      |NOTE: Namespace root or intermediate signing keys are NOT rotated by this command."""
  )
  def rotate_node_keys(): Unit = {

    val owner = instance.id.member

    // Find the current keys
    val currentKeys = findPublicKeys(instance.topology, owner)

    val replacements = currentKeys.map { currentKey =>
      val newKey =
        regenerateKey(
          currentKey,
          generateNewNameForRotatedKey(
            currentKey.fingerprint.unwrap,
            consoleEnvironment.environment.clock,
          ),
        )
      (currentKey, newKey)
    }

    // Perform a backup here if necessary

    replacements.foreach { case (currentKey, newKey) =>
      // Rotate the key for the node in the topology management
      instance.topology.owner_to_key_mappings.rotate_key(
        owner,
        currentKey,
        newKey,
      )
    }
  }

  /** Helper to find public keys for topology shared between community and enterprise
    */
  private def findPublicKeys(
      topologyAdmin: TopologyAdministrationGroup,
      owner: Member,
  ): Seq[PublicKey] =
    topologyAdmin.owner_to_key_mappings
      .list(
        store = Some(TopologyStoreId.Authorized),
        filterKeyOwnerUid = owner.filterString,
        filterKeyOwnerType = Some(owner.code),
      )
      .flatMap(_.item.keys)

  /** Helper to name new keys generated during a rotation with a ...-rotated-<timestamp> tag to
    * better identify the new keys after a rotation
    */
  private def generateNewNameForRotatedKey(
      currentKeyId: String,
      clock: Clock,
  ): String = {
    val keyName = instance.keys.secret
      .list()
      .find(_.publicKey.fingerprint.unwrap == currentKeyId)
      .flatMap(_.name)

    val rotatedKeyRegExp = "(.*-rotated).*".r

    keyName.map(_.unwrap) match {
      case Some(rotatedKeyRegExp(currentName)) =>
        s"$currentName-${clock.now.show}"
      case Some(currentName) =>
        s"$currentName-rotated-${clock.now.show}"
      case None => ""
    }
  }

  @Help.Summary("Change the wrapper key for encrypted private keys store")
  @Help.Description(
    """Change the wrapper key (e.g. AWS KMS key) being used to encrypt the private keys in the store.
      |newWrapperKeyId: The optional new wrapper key id to be used. If the wrapper key id is empty Canton will generate a new key based on the current configuration."""
  )
  def rotate_wrapper_key(
      newWrapperKeyId: String = ""
  ): Unit =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.RotateWrapperKey(newWrapperKeyId))
    }

  @Help.Summary("Get the wrapper key id that is used for the encrypted private keys store")
  def get_wrapper_key_id(): String =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.GetWrapperKeyId())
    }

  @Help.Summary("Upload (load and import) a key pair from file")
  @Help.Description(
    """Upload the previously downloaded key pair from a file.
      |filename: The name of the file holding the key pair
      |name: The (optional) descriptive name of the key pair
      |password: Optional password to decrypt an encrypted key pair"""
  )
  def upload_from(filename: String, name: Option[String], password: Option[String] = None): Unit = {
    val keyPair = BinaryFileUtil.tryReadByteStringFromFile(filename)
    upload(keyPair, name, password)
  }

  @Help.Summary("Upload a key pair")
  @Help.Description(
    """Upload the previously downloaded key pair.
      |pairBytes: The binary representation of a previously downloaded key pair
      |name: The (optional) descriptive name of the key pair
      |password: Optional password to decrypt an encrypted key pair"""
  )
  def upload(
      pairBytes: ByteString,
      name: Option[String],
      password: Option[String] = None,
  ): Unit =
    consoleEnvironment.run {
      adminCommand(
        VaultAdminCommands.ImportKeyPair(pairBytes, name, password)
      )
    }

  // TODO(i13613): Remove feature flag
  @Help.Summary("Download key pair", FeatureFlag.Preview)
  @Help.Description(
    """Download the key pair with the private and public key in its binary representation.
      |fingerprint: The identifier of the key pair to download
      |protocolVersion: The (optional) protocol version that defines the serialization of the key pair
      |password: Optional password to encrypt the exported key pair with"""
  )
  def download(
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
      password: Option[String] = None,
  ): ByteString =
    check(FeatureFlag.Preview) {
      consoleEnvironment.run {
        adminCommand(
          VaultAdminCommands.ExportKeyPair(fingerprint, protocolVersion, password)
        )
      }
    }

  @Help.Summary("Download key pair and save it to a file", FeatureFlag.Preview)
  @Help.Description(
    """Download the key pair with the private and public key in its binary representation and store it in a file.
      |fingerprint: The identifier of the key pair to download
      |outputFile: The name of the file to store the key pair in
      |protocolVersion: The (optional) protocol version that defines the serialization of the key pair
      |password: Optional password to encrypt the exported key pair with"""
  )
  def download_to(
      fingerprint: Fingerprint,
      outputFile: String,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
      password: Option[String] = None,
  ): Unit =
    writeToFile(outputFile, download(fingerprint, protocolVersion, password))

  @Help.Summary("Delete private key")
  def delete(fingerprint: Fingerprint, force: Boolean = false): Unit = {
    def deleteKey(): Unit =
      consoleEnvironment.run {
        adminCommand(
          VaultAdminCommands.DeleteKeyPair(fingerprint)
        )
      }

    if (force)
      deleteKey()
    else {
      println(
        s"Are you sure you want to delete the private key with fingerprint $fingerprint? yes/no"
      )
      println(s"This action is irreversible and can have undesired effects if done carelessly.")
      print("> ")
      val answer = Option(scala.io.StdIn.readLine())
      if (answer.exists(_.toLowerCase == "yes")) deleteKey()
    }
  }
}

/** Global keys operations (e.g., for external parties in tests)
  */
class GlobalSecretKeyAdministration(
    override protected val consoleEnvironment: ConsoleEnvironment,
    override protected val loggerFactory: NamedLoggerFactory,
) extends Helpful
    with FeatureFlagFilter {

  private implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext
  private implicit val tc: TraceContext = TraceContext.empty
  private implicit val ce: ConsoleEnvironment = consoleEnvironment

  @Help.Summary("Get the SigningPublicKey for a fingerprint")
  @Help.Description(
    """Get the SigningPublicKey for a fingerprint.
      |
      |The arguments are:
      |  - fingerprint: Fingerprint of the key to lookup
      |
      |Fails if the corresponding keys are not in the global crypto.
      |"""
  )
  def get_signing_key(fingerprint: Fingerprint): SigningPublicKey =
    consoleEnvironment.run(
      ConsoleCommandResult.fromEitherTUS(
        consoleEnvironment.tryGlobalCrypto.cryptoPublicStore
          .signingKey(fingerprint)
          .toRight(s"Signing key not found for fingerprint $fingerprint")
      )
    )

  @Help.Summary("Sign the given hash on behalf of the external party")
  @Help.Description(
    """Sign the given hash on behalf of the external party with signingThreshold keys.
      |
      |The arguments are:
      |  - hash: Hash to be signed
      |  - party: Party who should sign
      |  - useAllKeys: If true, all signing keys will be used to sign instead of just signingThreshold many
      |
      |Fails if the corresponding keys are not in the global crypto.
      |"""
  )
  def sign(hash: ByteString, party: ExternalParty, useAllKeys: Boolean = false): Seq[Signature] =
    consoleEnvironment.run(
      ConsoleCommandResult.fromEitherTUS(
        party.signingFingerprints
          .take(if (useAllKeys) party.signingFingerprints.size else party.signingThreshold.value)
          .parTraverse(
            consoleEnvironment.tryGlobalCrypto.privateCrypto
              .signBytes(hash, _, SigningKeyUsage.ProtocolOnly)
          )
          .leftMap(_.toString)
      )
    )

  @Help.Summary("Sign the given hash on behalf of the external party")
  @Help.Description(
    """Sign the given hash on behalf of the external party
      |
      |The arguments are:
      |  - hash: Hash to be signed
      |  - key: Fingerprint of the key that should sign
      |  - Usage: Usage of the key (e.g., Protocol or Namespace)
      |
      |Fails if the corresponding keys are not in the global crypto.
      |"""
  )
  def sign(
      hash: ByteString,
      key: Fingerprint,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Signature = consoleEnvironment.run(
    ConsoleCommandResult.fromEitherTUS(
      consoleEnvironment.tryGlobalCrypto.privateCrypto
        .signBytes(hash, key, usage)
        .leftMap(_.toString)
    )
  )

  @Help.Summary("Sign the given topology transaction on behalf of the external party")
  @Help.Description(
    """Sign the given topology transaction on behalf of the external party
      |
      |The arguments are:
      |  - tx: Transaction to be signed
      |  - party: Party who should sign
      |  - protocolVersion: Protocol version of the synchronizer
      |
      |Fails if the corresponding keys are not in the global crypto.
      |"""
  )
  def sign[Op <: TopologyChangeOp, M <: TopologyMapping](
      tx: TopologyTransaction[Op, M],
      party: ExternalParty,
      protocolVersion: ProtocolVersion,
  ): SignedTopologyTransaction[Op, M] = {
    val signature: Signature = consoleEnvironment.run(
      ConsoleCommandResult.fromEitherTUS(
        consoleEnvironment.tryGlobalCrypto.privateCrypto
          .sign(tx.hash.hash, party.fingerprint, SigningKeyUsage.NamespaceOnly)
          .leftMap(_.toString)
      )
    )

    SignedTopologyTransaction
      .withSignature(
        tx,
        signature,
        isProposal = true,
        protocolVersion,
      )
  }

  object keys {
    object secret {
      @Help.Summary("Download key pair")
      @Help.Description("Can be deserialized as a CryptoKeyPair")
      def download(
          fingerprint: Fingerprint,
          protocolVersion: ProtocolVersion = ProtocolVersion.latest,
          password: Option[String] = None,
      ): ByteString = {
        val cmd =
          LocalSecretKeyAdministration.download(
            consoleEnvironment.tryGlobalCrypto,
            fingerprint,
            protocolVersion,
            password,
          )

        consoleEnvironment.run(ConsoleCommandResult.fromEitherTUS(cmd))
      }

      @Help.Summary("Store a key into the global store")
      def store(keys: Seq[PrivateKey]): Unit = {
        val extendedCryptoStore =
          consoleEnvironment.tryGlobalCrypto.cryptoPrivateStore.toExtended.getOrElse {
            consoleEnvironment.raiseError("The global crypto does not support importing keys")
          }

        val cmd = keys
          .parTraverse_(
            extendedCryptoStore.storePrivateKey(_, None)
          )
          .leftMap(_.toString)

        consoleEnvironment.run(ConsoleCommandResult.fromEitherTUS(cmd))
      }

      @Help.Summary("Generate a key")
      @Help.Description("Generate a key and store it into the global store")
      def generate_key(
          keySpec: SigningKeySpec =
            consoleEnvironment.tryGlobalCrypto.privateCrypto.signingSchemes.keySpecs.default,
          usage: NonEmpty[Set[SigningKeyUsage]],
          name: Option[KeyName] = None,
      ): SigningPublicKey = consoleEnvironment.run(
        ConsoleCommandResult.fromEitherTUS(
          consoleEnvironment.tryGlobalCrypto
            .generateSigningKey(keySpec, usage, name)
            .leftMap(_.toString)
        )
      )

      @Help.Summary("Generate keys")
      @Help.Description("Generate keys and store them into the global store")
      def generate_keys(
          count: PositiveInt,
          keySpec: SigningKeySpec =
            consoleEnvironment.tryGlobalCrypto.privateCrypto.signingSchemes.keySpecs.default,
          usage: NonEmpty[Set[SigningKeyUsage]],
          name: Option[KeyName] = None,
      ): NonEmpty[Seq[SigningPublicKey]] = {
        val keys = Seq.fill(count.value)(
          consoleEnvironment.run(
            ConsoleCommandResult.fromEitherTUS(
              consoleEnvironment.tryGlobalCrypto
                .generateSigningKey(keySpec, usage, name)
                .leftMap(_.toString)
            )
          )
        )

        NonEmptyUtil.fromUnsafe(checked(keys))
      }
    }
  }
}

class PublicKeyAdministration(
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {

  import runner.*

  private def defaultLimit: PositiveInt =
    consoleEnvironment.environment.config.parameters.console.defaultLimit

  @Help.Summary("Upload public key")
  @Help.Description(
    """Import a public key and store it together with a name used to provide some context to that key."""
  )
  def upload(keyBytes: ByteString, name: Option[String]): Fingerprint = consoleEnvironment.run {
    adminCommand(
      VaultAdminCommands.ImportPublicKey(keyBytes, name)
    )
  }

  @Help.Summary("Upload public key")
  @Help.Summary(
    "Load a public key from a file and store it together with a name used to provide some context to that key."
  )
  def upload_from(filename: String, name: Option[String]): Fingerprint =
    BinaryFileUtil.readByteStringFromFile(filename).map(upload(_, name)).valueOr { err =>
      throw new IllegalArgumentException(err)
    }

  @Help.Summary("Download public key")
  def download(
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
  ): ByteString = {
    val keys = list(fingerprint.unwrap)
    if (keys.sizeCompare(1) == 0) { // vector doesn't like matching on Nil
      val key = keys.headOption.getOrElse(sys.error("no key"))
      key.publicKey.toByteString(protocolVersion)
    } else {
      if (keys.isEmpty) throw new IllegalArgumentException(s"no key found for [$fingerprint]")
      else
        throw new IllegalArgumentException(
          s"found multiple results for [$fingerprint]: ${keys.map(_.publicKey.fingerprint)}"
        )
    }
  }

  @Help.Summary("Download public key and save it to a file")
  def download_to(
      fingerprint: Fingerprint,
      outputFile: String,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
  ): Unit =
    BinaryFileUtil.writeByteStringToFile(
      outputFile,
      download(fingerprint, protocolVersion),
    )

  @Help.Summary("List public keys in registry")
  @Help.Description("""Returns all public keys that have been added to the key registry.
    Optional arguments can be used for filtering.""")
  def list(
      filterFingerprint: String = "",
      filterContext: String = "",
      filterPurpose: Set[KeyPurpose] = Set.empty,
      filterUsage: Set[SigningKeyUsage] = Set.empty,
  ): Seq[PublicKeyWithName] =
    consoleEnvironment.run {
      adminCommand(
        VaultAdminCommands.ListPublicKeys(
          filterFingerprint,
          filterContext,
          filterPurpose,
          filterUsage,
        )
      )
    }

  @Help.Summary("List active owners with keys for given search arguments.")
  @Help.Description("""This command allows deep inspection of the topology state.
      |The response includes the public keys.
      |Optional filterKeyOwnerType type can be 'ParticipantId.Code' , 'MediatorId.Code','SequencerId.Code'.
      |""")
  def list_owners(
      filterKeyOwnerUid: String = "",
      filterKeyOwnerType: Option[MemberCode] = None,
      synchronizerIds: Set[SynchronizerId] = Set.empty,
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListKeyOwnersResult] = consoleEnvironment.run {
    adminCommand(
      TopologyAdminCommands.Aggregation
        .ListKeyOwners(synchronizerIds, filterKeyOwnerType, filterKeyOwnerUid, asOf, limit)
    )
  }

  @Help.Summary("List keys for given keyOwner.")
  @Help.Description(
    """This command is a convenience wrapper for `list_key_owners`, taking an explicit keyOwner as search argument.
      |The response includes the public keys."""
  )
  def list_by_owner(
      keyOwner: Member,
      synchronizerIds: Set[SynchronizerId] = Set.empty,
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListKeyOwnersResult] =
    consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Aggregation.ListKeyOwners(
          synchronizerIds = synchronizerIds,
          filterKeyOwnerType = Some(keyOwner.code),
          filterKeyOwnerUid = keyOwner.uid.toProtoPrimitive,
          asOf,
          limit,
        )
      )
    }
}

class KeyAdministrationGroup(
    instance: InstanceReference,
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    loggerFactory: NamedLoggerFactory,
) extends Helpful {

  private lazy val publicAdmin =
    new PublicKeyAdministration(runner, consoleEnvironment)
  private lazy val secretAdmin =
    new SecretKeyAdministration(instance, runner, consoleEnvironment, loggerFactory)

  @Help.Summary("Manage public keys")
  @Help.Group("Public keys")
  def public: PublicKeyAdministration = publicAdmin

  @Help.Summary("Manage secret keys")
  @Help.Group("Secret keys")
  def secret: SecretKeyAdministration = secretAdmin
}

class LocalSecretKeyAdministration(
    instance: InstanceReference,
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    crypto: => Crypto,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SecretKeyAdministration(instance, runner, consoleEnvironment, loggerFactory) {

  @Help.Summary("Download key pair")
  override def download(
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
      password: Option[String] = None,
  ): ByteString =
    TraceContext.withNewTraceContext("download_key_pair") { implicit traceContext =>
      val cmd =
        LocalSecretKeyAdministration.download(crypto, fingerprint, protocolVersion, password)

      consoleEnvironment.run(ConsoleCommandResult.fromEitherTUS(cmd)(consoleEnvironment))
    }

  @Help.Summary("Download key pair and save it to a file", FeatureFlag.Preview)
  override def download_to(
      fingerprint: Fingerprint,
      outputFile: String,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
      password: Option[String] = None,
  ): Unit = writeToFile(outputFile, download(fingerprint, protocolVersion, password))
}

object LocalSecretKeyAdministration {
  def download(
      crypto: Crypto,
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
      password: Option[String] = None,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, ByteString] =
    for {
      cryptoPrivateStore <- crypto.cryptoPrivateStore.toExtended
        .toRight(
          "The selected crypto provider does not support exporting of private keys."
        )
        .toEitherT[FutureUnlessShutdown]
      privateKey <- cryptoPrivateStore
        .exportPrivateKey(fingerprint)
        .leftMap(_.toString)
        .subflatMap(_.toRight(s"no private key found for [$fingerprint]"))
        .leftMap(err => s"Error retrieving private key [$fingerprint] $err")
      publicKey <- crypto.cryptoPublicStore
        .publicKey(fingerprint)
        .toRight(s"Error retrieving public key [$fingerprint]: no public key found")
      keyPair: CryptoKeyPair[PublicKey, PrivateKey] = (publicKey, privateKey) match {
        case (pub: SigningPublicKey, pkey: SigningPrivateKey) =>
          SigningKeyPair.create(pub, pkey)
        case (pub: EncryptionPublicKey, pkey: EncryptionPrivateKey) =>
          EncryptionKeyPair.create(pub, pkey)
        case _ => sys.error("public and private keys must have same purpose")
      }

      // Encrypt the keypair if a password is provided
      keyPairBytes = password match {
        case Some(password) =>
          crypto.pureCrypto
            .encryptWithPassword(keyPair.toByteString(protocolVersion), password)
            .fold(
              err => sys.error(s"Failed to encrypt key pair for export: $err"),
              _.toByteString(protocolVersion),
            )
        case None => keyPair.toByteString(protocolVersion)
      }
    } yield keyPairBytes
}

class LocalKeyAdministrationGroup(
    instance: InstanceReference,
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    crypto: => Crypto,
    loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends KeyAdministrationGroup(instance, runner, consoleEnvironment, loggerFactory) {

  private lazy val localSecretAdmin: LocalSecretKeyAdministration =
    new LocalSecretKeyAdministration(instance, runner, consoleEnvironment, crypto, loggerFactory)

  @Help.Summary("Manage secret keys")
  @Help.Group("Secret keys")
  override def secret: LocalSecretKeyAdministration = localSecretAdmin
}
