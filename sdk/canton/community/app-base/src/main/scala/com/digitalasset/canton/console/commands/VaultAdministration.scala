// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.{TopologyAdminCommands, VaultAdminCommands}
import com.digitalasset.canton.admin.api.client.data.ListKeyOwnersResult
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{
  AdminCommandRunner,
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
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.{Member, MemberCode}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{BinaryFileUtil, OptionUtil}
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

  protected def regenerateKey(currentKey: PublicKey, name: Option[String]): PublicKey =
    currentKey match {
      case encKey: EncryptionPublicKey =>
        instance.keys.secret.generate_encryption_key(
          keySpec = Some(encKey.keySpec),
          name = OptionUtil.noneAsEmptyString(name),
        )
      case signKey: SigningPublicKey =>
        instance.keys.secret.generate_signing_key(
          name = OptionUtil.noneAsEmptyString(name),
          usage = signKey.usage,
          scheme = Some(signKey.scheme),
        )
      case unknown => throw new IllegalArgumentException(s"Invalid public key type: $unknown")
    }

  @Help.Summary("List keys in private vault")
  @Help.Description("""Returns all public keys to the corresponding private keys in the key vault.
                      |Optional arguments can be used for filtering.""")
  def list(
      filterFingerprint: String = "",
      filterName: String = "",
      purpose: Set[KeyPurpose] = Set.empty,
  ): Seq[PrivateKeyMetadata] =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.ListMyKeys(filterFingerprint, filterName, purpose))
    }

  @Help.Summary("Generate new public/private key pair for signing and store it in the vault")
  @Help.Description(
    """
      |The optional name argument allows you to store an associated string for your convenience.
      |The usage specifies the intended use for the signing key that can be:
      | - Namespace: for the root namespace key that defines a node's identity and signs topology requests;
      | - IdentityDelegation: for a signing key that acts as a delegation key for the root namespace and that can also be used to sign topology requests;
      | - SequencerAuthentication: for a signing key that authenticates members of the network towards a sequencer;
      | - Protocol: for a signing key that deals with all the signing that happens as part of the protocol.
      |The scheme can be used to select a key scheme and the default scheme is used if left unspecified."""
  )
  def generate_signing_key(
      name: String = "",
      usage: Set[SigningKeyUsage] = SigningKeyUsage.All,
      scheme: Option[SigningKeyScheme] = None,
  ): SigningPublicKey =
    NonEmpty.from(usage) match {
      case Some(usageNE) =>
        consoleEnvironment.run {
          adminCommand(VaultAdminCommands.GenerateSigningKey(name, usageNE, scheme))
        }
      case None => throw new IllegalArgumentException("no signing key usage specified")
    }

  @Help.Summary("Generate new public/private key pair for encryption and store it in the vault")
  @Help.Description(
    """
      |The optional name argument allows you to store an associated string for your convenience.
      |The scheme can be used to select a key scheme and the default scheme is used if left unspecified."""
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
      | - IdentityDelegation: for a signing key that acts as a delegation key for the root namespace and that can also be used to sign topology requests;
      | - SequencerAuthentication: for a signing key that authenticates members of the network towards a sequencer;
      | - Protocol: for a signing key that deals with all the signing that happens as part of the protocol.
      |The optional name argument allows you to store an associated string for your convenience."""
  )
  def register_kms_signing_key(
      kmsKeyId: String,
      usage: Set[SigningKeyUsage] = SigningKeyUsage.All,
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
      |The id of the new KMS key (e.g. Resource Name)."""
  )
  def rotate_kms_node_key(fingerprint: String, newKmsKeyId: String): PublicKey = {

    val owner = instance.id.member

    val currentKey = findPublicKey(fingerprint, instance.topology, owner)
    val newKey = currentKey match {
      case SigningPublicKey(_, _, _, usage) =>
        instance.keys.secret.register_kms_signing_key(newKmsKeyId, usage)
      case _: EncryptionPublicKey => instance.keys.secret.register_kms_encryption_key(newKmsKeyId)
      case _ => throw new IllegalStateException("Unsupported key type")
    }

    // Rotate the key for the node in the topology management
    instance.topology.owner_to_key_mappings.rotate_key(
      instance,
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
      |The fingerprint of the key we want to rotate."""
  )
  def rotate_node_key(fingerprint: String, name: Option[String] = None): PublicKey = {
    val owner = instance.id.member

    val currentKey = findPublicKey(fingerprint, instance.topology, owner)

    val newKey = name match {
      case Some(_) => regenerateKey(currentKey, name)
      case None =>
        regenerateKey(
          currentKey,
          generateNewNameForRotatedKey(fingerprint, consoleEnvironment.environment.clock),
        )
    }

    // Rotate the key for the node in the topology management
    instance.topology.owner_to_key_mappings.rotate_key(
      instance,
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
      |For a domain or domain manager node it rotates the signing key pair as those nodes do not have an encryption key pair.
      |For a sequencer or mediator node use `rotate_node_keys` with a domain manager reference as an argument.
      |NOTE: Namespace root or intermediate signing keys are NOT rotated by this command."""
  )
  def rotate_node_keys(): Unit = {

    val owner = instance.id.member

    // Find the current keys
    val currentKeys = findPublicKeys(instance.topology, owner)

    currentKeys.foreach { currentKey =>
      val newKey =
        regenerateKey(
          currentKey,
          generateNewNameForRotatedKey(
            currentKey.fingerprint.unwrap,
            consoleEnvironment.environment.clock,
          ),
        )

      // Rotate the key for the node in the topology management
      instance.topology.owner_to_key_mappings.rotate_key(
        instance,
        owner,
        currentKey,
        newKey,
      )
    }
  }

  /** Helper to find public keys for topology/x shared between community and enterprise
    */
  protected def findPublicKeys(
      topologyAdmin: TopologyAdministrationGroup,
      owner: Member,
  ): Seq[PublicKey] =
    topologyAdmin.owner_to_key_mappings
      .list(
        filterStore = AuthorizedStore.filterName,
        filterKeyOwnerUid = owner.filterString,
        filterKeyOwnerType = Some(owner.code),
      )
      .flatMap(_.item.keys)

  /** Helper to name new keys generated during a rotation with a ...-rotated-<timestamp> tag to better identify
    * the new keys after a rotation
    */
  protected def generateNewNameForRotatedKey(
      currentKeyId: String,
      clock: Clock,
  ): Option[String] = {
    val keyName = instance.keys.secret
      .list()
      .find(_.publicKey.fingerprint.unwrap == currentKeyId)
      .flatMap(_.name)

    val rotatedKeyRegExp = "(.*-rotated).*".r

    keyName.map(_.unwrap) match {
      case Some(rotatedKeyRegExp(currentName)) =>
        Some(s"$currentName-${clock.now.show}")
      case Some(currentName) =>
        Some(s"$currentName-rotated-${clock.now.show}")
      case None => None
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

  @Help.Summary("Download key pair and save it to a file")
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
  def list(filterFingerprint: String = "", filterContext: String = ""): Seq[PublicKeyWithName] =
    consoleEnvironment.run {
      adminCommand(VaultAdminCommands.ListPublicKeys(filterFingerprint, filterContext))
    }

  @Help.Summary("List active owners with keys for given search arguments.")
  @Help.Description("""This command allows deep inspection of the topology state.
      |The response includes the public keys.
      |Optional filterKeyOwnerType type can be 'ParticipantId.Code' , 'MediatorId.Code','SequencerId.Code'.
      |""")
  def list_owners(
      filterKeyOwnerUid: String = "",
      filterKeyOwnerType: Option[MemberCode] = None,
      filterDomain: String = "",
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListKeyOwnersResult] = consoleEnvironment.run {
    adminCommand(
      TopologyAdminCommands.Aggregation
        .ListKeyOwners(filterDomain, filterKeyOwnerType, filterKeyOwnerUid, asOf, limit)
    )
  }

  @Help.Summary("List keys for given keyOwner.")
  @Help.Description(
    """This command is a convenience wrapper for `list_key_owners`, taking an explicit keyOwner as search argument.
      |The response includes the public keys."""
  )
  def list_by_owner(
      keyOwner: Member,
      filterDomain: String = "",
      asOf: Option[Instant] = None,
      limit: PositiveInt = defaultLimit,
  ): Seq[ListKeyOwnersResult] =
    consoleEnvironment.run {
      adminCommand(
        TopologyAdminCommands.Aggregation.ListKeyOwners(
          filterDomain = filterDomain,
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

  private def run[V](eitherT: EitherT[FutureUnlessShutdown, String, V], action: String): V = {
    import TraceContext.Implicits.Empty.*
    consoleEnvironment.environment.config.parameters.timeouts.processing.default
      .await(action)(
        eitherT.onShutdown(throw new RuntimeException("aborted due to shutdown.")).value
      ) match {
      case Left(error) =>
        throw new IllegalArgumentException(s"Problem while $action. Error: $error")
      case Right(value) => value
    }
  }

  @Help.Summary("Download key pair")
  override def download(
      fingerprint: Fingerprint,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
      password: Option[String] = None,
  ): ByteString =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val cmd = for {
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
            new SigningKeyPair(pub, pkey)
          case (pub: EncryptionPublicKey, pkey: EncryptionPrivateKey) =>
            new EncryptionKeyPair(pub, pkey)
          case _ => sys.error("public and private keys must have same purpose")
        }

        // Encrypt the keypair if a password is provided
        keyPairBytes = password match {
          case Some(password) =>
            crypto.pureCrypto
              .encryptWithPassword(keyPair, password, protocolVersion)
              .fold(
                err => sys.error(s"Failed to encrypt key pair for export: $err"),
                _.toByteString(protocolVersion),
              )
          case None => keyPair.toByteString(protocolVersion)
        }
      } yield keyPairBytes
      run(cmd, "exporting key pair")
    }

  @Help.Summary("Download key pair and save it to a file")
  override def download_to(
      fingerprint: Fingerprint,
      outputFile: String,
      protocolVersion: ProtocolVersion = ProtocolVersion.latest,
      password: Option[String] = None,
  ): Unit =
    run(
      EitherT.rightT(writeToFile(outputFile, download(fingerprint, protocolVersion, password))),
      "saving key pair to file",
    )

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
