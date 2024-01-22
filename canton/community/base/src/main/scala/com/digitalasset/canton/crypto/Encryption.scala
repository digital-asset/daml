// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.data.EitherT
import cats.instances.future.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStoreError,
  CryptoPrivateStoreExtended,
  CryptoPublicStoreError,
}
import com.digitalasset.canton.error.{BaseCantonError, CantonErrorGroups}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DeserializationError, ProtoConverter}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedToByteString,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}

/** Encryption operations that do not require access to a private key store but operates with provided keys. */
trait EncryptionOps {

  protected def decryptWithInternal[M](
      encrypted: AsymmetricEncrypted[M],
      privateKey: EncryptionPrivateKey,
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M]

  def defaultSymmetricKeyScheme: SymmetricKeyScheme

  /** Generates and returns a random symmetric key using the specified scheme. */
  def generateSymmetricKey(
      scheme: SymmetricKeyScheme = defaultSymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey]

  /** Creates a symmetric key with the specified scheme for the given randomness. */
  def createSymmetricKey(
      bytes: SecureRandomness,
      scheme: SymmetricKeyScheme = defaultSymmetricKeyScheme,
  ): Either[EncryptionKeyCreationError, SymmetricKey]

  /** Encrypts the given bytes using the given public key */
  def encryptWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, AsymmetricEncrypted[M]]

  /** Deterministically encrypts the given bytes using the given public key.
    * This is unsafe for general use and it's only used to encrypt the decryption key of each view
    */
  def encryptDeterministicWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](encrypted: AsymmetricEncrypted[M], privateKey: EncryptionPrivateKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] = for {
    _ <- Either.cond(
      encrypted.encryptedFor == privateKey.id,
      (),
      DecryptionError.InvalidEncryptionKey(
        s"Private key ${privateKey.id} does not match the used encryption key ${encrypted.encryptedFor}"
      ),
    )
    message <- decryptWithInternal(encrypted, privateKey)(deserialize)
  } yield message

  /** Encrypts the given message with the given symmetric key */
  def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M]

}

/** Encryption operations that require access to stored private keys. */
trait EncryptionPrivateOps {

  def defaultEncryptionKeyScheme: EncryptionKeyScheme

  /** Decrypts an encrypted message using the referenced private encryption key */
  def decrypt[M](encrypted: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, DecryptionError, M]

  /** Generates a new encryption key pair with the given scheme and optional name, stores the private key and returns the public key. */
  def generateEncryptionKey(
      scheme: EncryptionKeyScheme = defaultEncryptionKeyScheme,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionPublicKey]
}

/** A default implementation with a private key store */
trait EncryptionPrivateStoreOps extends EncryptionPrivateOps {

  implicit val ec: ExecutionContext

  protected def store: CryptoPrivateStoreExtended

  protected val encryptionOps: EncryptionOps

  /** Decrypts an encrypted message using the referenced private encryption key */
  def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit tc: TraceContext): EitherT[Future, DecryptionError, M] =
    store
      .decryptionKey(encryptedMessage.encryptedFor)(TraceContext.todo)
      .leftMap(storeError => DecryptionError.KeyStoreError(storeError.show))
      .subflatMap(_.toRight(DecryptionError.UnknownEncryptionKey(encryptedMessage.encryptedFor)))
      .subflatMap(encryptionKey =>
        encryptionOps.decryptWith(encryptedMessage, encryptionKey)(deserialize)
      )

  /** Internal method to generate and return the entire encryption key pair */
  protected[crypto] def generateEncryptionKeypair(scheme: EncryptionKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionKeyPair]

  def generateEncryptionKey(
      scheme: EncryptionKeyScheme = defaultEncryptionKeyScheme,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      keypair <- generateEncryptionKeypair(scheme)
      _ <- store
        .storeDecryptionKey(keypair.privateKey, name)
        .leftMap[EncryptionKeyGenerationError](
          EncryptionKeyGenerationError.EncryptionPrivateStoreError
        )
    } yield keypair.publicKey

}

/** A tag to denote encrypted data. */
final case class Encrypted[+M] private[crypto] (ciphertext: ByteString) extends NoCopy

object Encrypted {
  private[this] def apply[M](ciphertext: ByteString): Encrypted[M] =
    throw new UnsupportedOperationException("Use encryption methods instead")

  def fromByteString[M](byteString: ByteString): Either[DeserializationError, Encrypted[M]] =
    Right(new Encrypted[M](byteString))
}

final case class AsymmetricEncrypted[+M](
    ciphertext: ByteString,
    encryptedFor: Fingerprint,
) extends NoCopy {
  def encrypted: Encrypted[M] = new Encrypted(ciphertext)
}

object AsymmetricEncrypted {
  val noEncryptionFingerprint = Fingerprint(String68.tryCreate("no-encryption"))
}

/** Key schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  // TODO(#12757): once we decouple the key scheme from the actual encryption algorithm this will move to the algorithm
  def supportDeterministicEncryption: Boolean
  def toProtoEnum: v0.EncryptionKeyScheme
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionKeyScheme {

  implicit val encryptionKeySchemeOrder: Order[EncryptionKeyScheme] =
    Order.by[EncryptionKeyScheme, String](_.name)

  case object EciesP256HkdfHmacSha256Aes128Gcm extends EncryptionKeyScheme {
    override val name: String = "ECIES-P256_HMAC256_AES128-GCM"
    override val supportDeterministicEncryption: Boolean = false
    override def toProtoEnum: v0.EncryptionKeyScheme =
      v0.EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm
  }

  /* This hybrid scheme from JCE/Bouncy Castle is intended to be used to encrypt the key for the view payload data
   * and can be made deterministic (e.g. using the hash(message ++ public key) as our source of randomness).
   * This way, every recipient of the view message can check that every other recipient can decrypt it
   * (i.e. transparency).
   */
  case object EciesP256HmacSha256Aes128Cbc extends EncryptionKeyScheme {
    override val name: String = "ECIES-P256_HMAC256_AES128-CBC"
    override val supportDeterministicEncryption: Boolean = true
    override def toProtoEnum: v0.EncryptionKeyScheme =
      v0.EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc
  }

  case object Rsa2048OaepSha256 extends EncryptionKeyScheme {
    override val name: String = "RSA2048-OAEP-SHA256"
    override val supportDeterministicEncryption: Boolean = true
    override def toProtoEnum: v0.EncryptionKeyScheme =
      v0.EncryptionKeyScheme.Rsa2048OaepSha256
  }

  val allSchemes: NonEmpty[Set[EncryptionKeyScheme]] = NonEmpty.mk(
    Set,
    EciesP256HkdfHmacSha256Aes128Gcm,
    EciesP256HmacSha256Aes128Cbc,
    Rsa2048OaepSha256,
  )

  def fromProtoEnum(
      field: String,
      schemeP: v0.EncryptionKeyScheme,
  ): ParsingResult[EncryptionKeyScheme] =
    schemeP match {
      case v0.EncryptionKeyScheme.MissingEncryptionKeyScheme =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.EncryptionKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
        Right(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm)
      case v0.EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc =>
        Right(EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc)
      case v0.EncryptionKeyScheme.Rsa2048OaepSha256 =>
        Right(EncryptionKeyScheme.Rsa2048OaepSha256)
    }
}

/** Key schemes for symmetric encryption. */
sealed trait SymmetricKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v0.SymmetricKeyScheme
  def keySizeInBytes: Int
  override def pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SymmetricKeyScheme {

  implicit val symmetricKeySchemeOrder: Order[SymmetricKeyScheme] =
    Order.by[SymmetricKeyScheme, String](_.name)

  /** AES with 128bit key in GCM */
  case object Aes128Gcm extends SymmetricKeyScheme {
    override def name: String = "AES128-GCM"
    override def toProtoEnum: v0.SymmetricKeyScheme = v0.SymmetricKeyScheme.Aes128Gcm
    override def keySizeInBytes: Int = 16
  }

  def fromProtoEnum(
      field: String,
      schemeP: v0.SymmetricKeyScheme,
  ): ParsingResult[SymmetricKeyScheme] =
    schemeP match {
      case v0.SymmetricKeyScheme.MissingSymmetricKeyScheme =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.SymmetricKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.SymmetricKeyScheme.Aes128Gcm => Right(SymmetricKeyScheme.Aes128Gcm)
    }
}

final case class SymmetricKey(
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: SymmetricKeyScheme,
) extends CryptoKey
    with HasVersionedWrapper[SymmetricKey]
    with NoCopy {
  override protected def companionObj = SymmetricKey

  protected def toProtoV0: v0.SymmetricKey =
    v0.SymmetricKey(format = format.toProtoEnum, key = key, scheme = scheme.toProtoEnum)
}

object SymmetricKey extends HasVersionedMessageCompanion[SymmetricKey] {
  override val name: String = "SymmetricKey"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v0.SymmetricKey)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  private def fromProtoV0(keyP: v0.SymmetricKey): ParsingResult[SymmetricKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", keyP.format)
      scheme <- SymmetricKeyScheme.fromProtoEnum("scheme", keyP.scheme)
    } yield new SymmetricKey(format, keyP.key, scheme)
}

final case class EncryptionKeyPair(publicKey: EncryptionPublicKey, privateKey: EncryptionPrivateKey)
    extends CryptoKeyPair[EncryptionPublicKey, EncryptionPrivateKey]
    with NoCopy {

  def toProtoV0: v0.EncryptionKeyPair =
    v0.EncryptionKeyPair(Some(publicKey.toProtoV0), Some(privateKey.toProtoV0))

  protected def toProtoCryptoKeyPairPairV0: v0.CryptoKeyPair.Pair =
    v0.CryptoKeyPair.Pair.EncryptionKeyPair(toProtoV0)
}

object EncryptionKeyPair {

  private[this] def apply(
      publicKey: EncryptionPublicKey,
      privateKey: EncryptionPrivateKey,
  ): EncryptionKeyPair =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  private[crypto] def create(
      id: Fingerprint,
      format: CryptoKeyFormat,
      publicKeyBytes: ByteString,
      privateKeyBytes: ByteString,
      scheme: EncryptionKeyScheme,
  ): EncryptionKeyPair = {
    val publicKey = new EncryptionPublicKey(id, format, publicKeyBytes, scheme)
    val privateKey = new EncryptionPrivateKey(publicKey.id, format, privateKeyBytes, scheme)
    new EncryptionKeyPair(publicKey, privateKey)
  }

  def fromProtoV0(
      encryptionKeyPairP: v0.EncryptionKeyPair
  ): ParsingResult[EncryptionKeyPair] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        EncryptionPublicKey.fromProtoV0,
        "public_key",
        encryptionKeyPairP.publicKey,
      )
      privateKey <- ProtoConverter.parseRequired(
        EncryptionPrivateKey.fromProtoV0,
        "private_key",
        encryptionKeyPairP.privateKey,
      )
    } yield new EncryptionKeyPair(publicKey, privateKey)
}

final case class EncryptionPublicKey private[crypto] (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: EncryptionKeyScheme,
) extends PublicKey
    with PrettyPrinting
    with HasVersionedWrapper[EncryptionPublicKey]
    with NoCopy {

  override protected def companionObj = EncryptionPublicKey

  val purpose: KeyPurpose = KeyPurpose.Encryption

  def toProtoV0: v0.EncryptionPublicKey =
    v0.EncryptionPublicKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      publicKey = key,
      scheme = scheme.toProtoEnum,
    )

  override protected def toProtoPublicKeyKeyV0: v0.PublicKey.Key =
    v0.PublicKey.Key.EncryptionPublicKey(toProtoV0)

  override val pretty: Pretty[EncryptionPublicKey] =
    prettyOfClass(param("id", _.id), param("format", _.format), param("scheme", _.scheme))
}

object EncryptionPublicKey
    extends HasVersionedMessageCompanion[EncryptionPublicKey]
    with HasVersionedMessageCompanionDbHelpers[EncryptionPublicKey] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v0.EncryptionPublicKey)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  override def name: String = "encryption public key"

  private[this] def apply(
      id: Fingerprint,
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: EncryptionKeyScheme,
  ): EncryptionPrivateKey =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  def fromProtoV0(
      publicKeyP: v0.EncryptionPublicKey
  ): ParsingResult[EncryptionPublicKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(publicKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", publicKeyP.format)
      scheme <- EncryptionKeyScheme.fromProtoEnum("scheme", publicKeyP.scheme)
    } yield new EncryptionPublicKey(id, format, publicKeyP.publicKey, scheme)
}

final case class EncryptionPublicKeyWithName(
    override val publicKey: EncryptionPublicKey,
    override val name: Option[KeyName],
) extends PublicKeyWithName {
  type K = EncryptionPublicKey
}

object EncryptionPublicKeyWithName {
  implicit def getResultEncryptionPublicKeyWithName(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[EncryptionPublicKeyWithName] =
    GetResult { r =>
      EncryptionPublicKeyWithName(r.<<, r.<<)
    }
}

final case class EncryptionPrivateKey private[crypto] (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: EncryptionKeyScheme,
) extends PrivateKey
    with HasVersionedWrapper[EncryptionPrivateKey]
    with NoCopy {

  override protected def companionObj = EncryptionPrivateKey

  override def purpose: KeyPurpose = KeyPurpose.Encryption

  def toProtoV0: v0.EncryptionPrivateKey =
    v0.EncryptionPrivateKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      privateKey = key,
      scheme = scheme.toProtoEnum,
    )

  override protected def toProtoPrivateKeyKeyV0: v0.PrivateKey.Key =
    v0.PrivateKey.Key.EncryptionPrivateKey(toProtoV0)
}

object EncryptionPrivateKey extends HasVersionedMessageCompanion[EncryptionPrivateKey] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v0.EncryptionPrivateKey)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  override def name: String = "encryption private key"

  private[this] def apply(
      id: Fingerprint,
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: EncryptionKeyScheme,
  ): EncryptionPrivateKey =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  def fromProtoV0(
      privateKeyP: v0.EncryptionPrivateKey
  ): ParsingResult[EncryptionPrivateKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(privateKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", privateKeyP.format)
      scheme <- EncryptionKeyScheme.fromProtoEnum("scheme", privateKeyP.scheme)
    } yield new EncryptionPrivateKey(id, format, privateKeyP.privateKey, scheme)
}

sealed trait EncryptionError extends Product with Serializable with PrettyPrinting
object EncryptionError {
  final case class UnsupportedSchemeForDeterministicEncryption(error: String)
      extends EncryptionError {
    override def pretty: Pretty[UnsupportedSchemeForDeterministicEncryption] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class FailedToEncrypt(error: String) extends EncryptionError {
    override def pretty: Pretty[FailedToEncrypt] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  final case class InvalidSymmetricKey(error: String) extends EncryptionError {
    override def pretty: Pretty[InvalidSymmetricKey] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  final case class InvalidEncryptionKey(error: String) extends EncryptionError {
    override def pretty: Pretty[InvalidEncryptionKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}

sealed trait DecryptionError extends Product with Serializable with PrettyPrinting
object DecryptionError {
  final case class FailedToDecrypt(error: String) extends DecryptionError {
    override def pretty: Pretty[FailedToDecrypt] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  final case class InvalidSymmetricKey(error: String) extends DecryptionError {
    override def pretty: Pretty[InvalidSymmetricKey] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  final case class InvariantViolation(error: String) extends DecryptionError {
    override def pretty: Pretty[InvariantViolation] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  final case class InvalidEncryptionKey(error: String) extends DecryptionError {
    override def pretty: Pretty[InvalidEncryptionKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class UnknownEncryptionKey(keyId: Fingerprint) extends DecryptionError {
    override def pretty: Pretty[UnknownEncryptionKey] = prettyOfClass(param("keyId", _.keyId))
  }
  final case class DecryptionKeyError(error: CryptoPrivateStoreError) extends DecryptionError {
    override def pretty: Pretty[DecryptionKeyError] = prettyOfClass(unnamedParam(_.error))
  }
  final case class FailedToDeserialize(error: DeserializationError) extends DecryptionError {
    override def pretty: Pretty[FailedToDeserialize] = prettyOfClass(unnamedParam(_.error))
  }
  final case class KeyStoreError(error: String) extends DecryptionError {
    override def pretty: Pretty[KeyStoreError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
}

sealed trait EncryptionKeyGenerationError extends Product with Serializable with PrettyPrinting
object EncryptionKeyGenerationError extends CantonErrorGroups.CommandErrorGroup {

  @Explanation("This error indicates that an encryption key could not be created.")
  @Resolution("Inspect the error details")
  object ErrorCode
      extends ErrorCode(
        id = "ENCRYPTION_KEY_GENERATION_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Wrap(reason: EncryptionKeyGenerationError)
        extends BaseCantonError.Impl(cause = "Unable to create encryption key")
  }

  final case class GeneralError(error: Exception) extends EncryptionKeyGenerationError {
    override def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class GeneralKmsError(error: String) extends EncryptionKeyGenerationError {
    override def pretty: Pretty[GeneralKmsError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class NameInvalidError(error: String) extends EncryptionKeyGenerationError {
    override def pretty: Pretty[NameInvalidError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class FingerprintError(error: String) extends EncryptionKeyGenerationError {
    override def pretty: Pretty[FingerprintError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class UnsupportedKeyScheme(scheme: EncryptionKeyScheme)
      extends EncryptionKeyGenerationError {
    override def pretty: Pretty[UnsupportedKeyScheme] = prettyOfClass(param("scheme", _.scheme))
  }

  final case class EncryptionPrivateStoreError(error: CryptoPrivateStoreError)
      extends EncryptionKeyGenerationError {
    override def pretty: Pretty[EncryptionPrivateStoreError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class EncryptionPublicStoreError(error: CryptoPublicStoreError)
      extends EncryptionKeyGenerationError {
    override def pretty: Pretty[EncryptionPublicStoreError] = prettyOfClass(unnamedParam(_.error))
  }
}

sealed trait EncryptionKeyCreationError extends Product with Serializable with PrettyPrinting
object EncryptionKeyCreationError {

  final case class InvalidRandomnessLength(randomnessLength: Int, expectedKeyLength: Int)
      extends EncryptionKeyCreationError {
    override def pretty: Pretty[InvalidRandomnessLength] = prettyOfClass(
      param("provided randomness length", _.randomnessLength),
      param("expected key length", _.expectedKeyLength),
    )
  }

  final case class InternalConversionError(error: String) extends EncryptionKeyCreationError {
    override def pretty: Pretty[InternalConversionError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

}
