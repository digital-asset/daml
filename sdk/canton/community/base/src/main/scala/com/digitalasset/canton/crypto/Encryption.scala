// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStoreError,
  CryptoPrivateStoreExtended,
  CryptoPublicStoreError,
}
import com.digitalasset.canton.error.{BaseCantonError, CantonErrorGroups}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  ProtoConverter,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.{
  HasToByteString,
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedToByteString,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

/** Encryption operations that do not require access to a private key store but operates with provided keys. */
trait EncryptionOps {

  protected[crypto] def decryptWithInternal[M](
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

  def defaultEncryptionAlgorithmSpec: EncryptionAlgorithmSpec
  def supportedEncryptionAlgorithmSpecs: NonEmpty[Set[EncryptionAlgorithmSpec]]

  /** Encrypts the bytes of the serialized message using the given public key.
    * The given protocol version determines the message serialization.
    */
  def encryptWithVersion[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = defaultEncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]]

  /** Encrypts the bytes of the serialized message using the given public key.
    * Where the message embedded protocol version determines the message serialization.
    */
  def encryptWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = defaultEncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]]

  /** Deterministically encrypts the given bytes using the given public key.
    * This is unsafe for general use and it's only used to encrypt the decryption key of each view
    */
  def encryptDeterministicWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = defaultEncryptionAlgorithmSpec,
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

  /** Encrypts the bytes of the serialized message using the given symmetric key.
    * The given protocol version determines the message serialization.
    */
  def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]]

  /** Encrypts the bytes of the serialized message using the given symmetric key.
    * Where the message embedded protocol version determines the message serialization.
    */
  def encryptWith[M <: HasToByteString](
      message: M,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, Encrypted[M]]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M]

}

/** Encryption operations that require access to stored private keys. */
trait EncryptionPrivateOps {

  def defaultEncryptionKeySpec: EncryptionKeySpec

  /** Decrypts an encrypted message using the referenced private encryption key */
  def decrypt[M](encrypted: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DecryptionError, M]

  /** Generates a new encryption key pair with the given scheme and optional name, stores the private key and returns the public key. */
  def generateEncryptionKey(
      keySpec: EncryptionKeySpec = defaultEncryptionKeySpec,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey]
}

/** A default implementation with a private key store */
trait EncryptionPrivateStoreOps extends EncryptionPrivateOps {

  implicit val ec: ExecutionContext

  protected def store: CryptoPrivateStoreExtended

  protected val encryptionOps: EncryptionOps

  /** Decrypts an encrypted message using the referenced private encryption key */
  override def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, DecryptionError, M] =
    store
      .decryptionKey(encryptedMessage.encryptedFor)
      .leftMap(storeError => DecryptionError.KeyStoreError(storeError.show))
      .subflatMap(_.toRight(DecryptionError.UnknownEncryptionKey(encryptedMessage.encryptedFor)))
      .subflatMap(encryptionKey =>
        encryptionOps.decryptWith(encryptedMessage, encryptionKey)(deserialize)
      )

  /** Internal method to generate and return the entire encryption key pair */
  protected[crypto] def generateEncryptionKeypair(keySpec: EncryptionKeySpec)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionKeyPair]

  override def generateEncryptionKey(
      keySpec: EncryptionKeySpec = defaultEncryptionKeySpec,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      keypair <- generateEncryptionKeypair(keySpec)
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

  def fromByteString[M](byteString: ByteString): Encrypted[M] =
    new Encrypted[M](byteString)
}

/** Represents an asymmetric encrypted message.
  *
  * @param ciphertext the encrypted message
  * @param encryptionAlgorithmSpec the encryption algorithm specification (e.g. RSA OAEP)
  * @param encryptedFor the public key of the recipient
  */
final case class AsymmetricEncrypted[+M](
    ciphertext: ByteString,
    encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
    encryptedFor: Fingerprint,
) extends NoCopy {
  def encrypted: Encrypted[M] = new Encrypted(ciphertext)
}

/** An encryption key specification. */
sealed trait EncryptionKeySpec extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v30.EncryptionKeySpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionKeySpec {

  implicit val encryptionKeySpecOrder: Order[EncryptionKeySpec] =
    Order.by[EncryptionKeySpec, String](_.name)

  /** Elliptic Curve Key from the P-256 curve (aka Secp256r1)
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP256 extends EncryptionKeySpec {
    override val name: String = "EC-P256"
    override def toProtoEnum: v30.EncryptionKeySpec =
      v30.EncryptionKeySpec.ENCRYPTION_KEY_SPEC_EC_P256
  }

  /** RSA key with 2048 bits */
  case object Rsa2048 extends EncryptionKeySpec {
    override val name: String = "RSA-2048"
    // the key size in bits for RSA2048
    val keySizeInBits: Int = 2048
    override def toProtoEnum: v30.EncryptionKeySpec =
      v30.EncryptionKeySpec.ENCRYPTION_KEY_SPEC_RSA_2048
  }

  def fromProtoEnum(
      field: String,
      schemeP: v30.EncryptionKeySpec,
  ): ParsingResult[EncryptionKeySpec] =
    schemeP match {
      case v30.EncryptionKeySpec.ENCRYPTION_KEY_SPEC_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.EncryptionKeySpec.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.EncryptionKeySpec.ENCRYPTION_KEY_SPEC_EC_P256 =>
        Right(EncryptionKeySpec.EcP256)
      case v30.EncryptionKeySpec.ENCRYPTION_KEY_SPEC_RSA_2048 =>
        Right(EncryptionKeySpec.Rsa2048)
    }
}

/** Key schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionAlgorithmSpec extends Product with Serializable with PrettyPrinting {
  def name: String
  def supportDeterministicEncryption: Boolean
  def supportedEncryptionKeySpecs: NonEmpty[Set[EncryptionKeySpec]]
  def toProtoEnum: v30.EncryptionAlgorithmSpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionAlgorithmSpec {

  implicit val encryptionAlgorithmSpecOrder: Order[EncryptionAlgorithmSpec] =
    Order.by[EncryptionAlgorithmSpec, String](_.name)

  /* This hybrid scheme (https://www.secg.org/sec1-v2.pdf) from JCE/Bouncy Castle is intended to be used to
   * encrypt the key for the view payload data.
   */
  case object EciesHkdfHmacSha256Aes128Gcm extends EncryptionAlgorithmSpec {
    override val name: String = "ECIES_HMAC256_AES128-GCM"
    override val supportDeterministicEncryption: Boolean = false
    override val supportedEncryptionKeySpecs: NonEmpty[Set[EncryptionKeySpec]] =
      NonEmpty.mk(Set, EncryptionKeySpec.EcP256)
    override def toProtoEnum: v30.EncryptionAlgorithmSpec =
      v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_ECIES_HKDF_HMAC_SHA256_AES128GCM
  }

  /* This hybrid scheme (https://www.secg.org/sec1-v2.pdf) from JCE/Bouncy Castle is intended to be used to encrypt
   * the key for the view payload data and can be made deterministic (e.g. using the hash(message ++ public key)
   * as our source of randomness). This way, every recipient of the view message can check that every other recipient
   * can decrypt it (i.e. transparency).
   */
  case object EciesHkdfHmacSha256Aes128Cbc extends EncryptionAlgorithmSpec {
    override val name: String = "ECIES_HMAC256_AES128-CBC"
    override val supportDeterministicEncryption: Boolean = true
    override val supportedEncryptionKeySpecs: NonEmpty[Set[EncryptionKeySpec]] =
      NonEmpty.mk(Set, EncryptionKeySpec.EcP256)
    override def toProtoEnum: v30.EncryptionAlgorithmSpec =
      v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_ECIES_HKDF_HMAC_SHA256_AES128CBC
  }

  /* This public encryption scheme (https://datatracker.ietf.org/doc/html/rfc8017#section-7.1) is
   * intended to be used to encrypt the key for the view payload data. It can also be made deterministic
   * (see details above). It was chosen because it is supported by most Key Management Service (KMS) providers
   * (e.g. AWS KMS, GCP KMS).
   */
  case object RsaOaepSha256 extends EncryptionAlgorithmSpec {
    override val name: String = "RSA-OAEP-SHA256"
    override val supportDeterministicEncryption: Boolean = true
    override val supportedEncryptionKeySpecs: NonEmpty[Set[EncryptionKeySpec]] =
      NonEmpty.mk(Set, EncryptionKeySpec.Rsa2048)
    override def toProtoEnum: v30.EncryptionAlgorithmSpec =
      v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_RSA_OAEP_SHA256
  }

  val allSchemes: NonEmpty[Set[EncryptionAlgorithmSpec]] = NonEmpty.mk(
    Set,
    EciesHkdfHmacSha256Aes128Gcm,
    EciesHkdfHmacSha256Aes128Cbc,
    RsaOaepSha256,
  )

  def fromProtoEnum(
      field: String,
      schemeP: v30.EncryptionAlgorithmSpec,
  ): ParsingResult[EncryptionAlgorithmSpec] =
    schemeP match {
      case v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.EncryptionAlgorithmSpec.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_ECIES_HKDF_HMAC_SHA256_AES128GCM =>
        Right(EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm)
      case v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_ECIES_HKDF_HMAC_SHA256_AES128CBC =>
        Right(EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc)
      case v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_RSA_OAEP_SHA256 =>
        Right(EncryptionAlgorithmSpec.RsaOaepSha256)
    }

}

/** Required encryption algorithms and keys for asymmetric/hybrid encryption to be listed in the domain.
  *
  * @param algorithms list of required encryption algorithm specifications
  * @param keys list of required encryption key specifications
  */
final case class RequiredEncryptionSpecs(
    algorithms: NonEmpty[Set[EncryptionAlgorithmSpec]],
    keys: NonEmpty[Set[EncryptionKeySpec]],
) extends Product
    with Serializable
    with PrettyPrinting {
  def toProtoV30: v30.RequiredEncryptionSpecs =
    v30.RequiredEncryptionSpecs(
      algorithms.forgetNE.map(_.toProtoEnum).toSeq,
      keys.forgetNE.map(_.toProtoEnum).toSeq,
    )
  override val pretty: Pretty[this.type] = prettyOfClass(
    param("algorithms", _.algorithms),
    param("keys", _.keys),
  )
}

object RequiredEncryptionSpecs {
  def fromProtoV30(
      requiredEncryptionSpecsP: v30.RequiredEncryptionSpecs
  ): ParsingResult[RequiredEncryptionSpecs] =
    for {
      keySpecs <- requiredEncryptionSpecsP.keys.traverse(keySpec =>
        EncryptionKeySpec.fromProtoEnum("keys", keySpec)
      )
      algorithmSpecs <- requiredEncryptionSpecsP.algorithms
        .traverse(algorithmSpec =>
          EncryptionAlgorithmSpec.fromProtoEnum("algorithms", algorithmSpec)
        )
      keySpecsNE <- NonEmpty
        .from(keySpecs.toSet)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            "keys",
            "no required encryption algorithm specification",
          )
        )
      algorithmSpecsNE <- NonEmpty
        .from(algorithmSpecs.toSet)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            "algorithms",
            "no required encryption key specification",
          )
        )
    } yield RequiredEncryptionSpecs(algorithmSpecsNE, keySpecsNE)
}

sealed trait EncryptionKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionKeyScheme {

  implicit val encryptionKeySchemeOrder: Order[EncryptionKeyScheme] =
    Order.by[EncryptionKeyScheme, String](_.name)

  case object EciesP256HkdfHmacSha256Aes128Gcm extends EncryptionKeyScheme {
    override val name: String = "ECIES-P256_HMAC256_AES128-GCM"
  }

  /* This hybrid scheme from JCE/Bouncy Castle is intended to be used to encrypt the key for the view payload data
   * and can be made deterministic (e.g. using the hash(message ++ public key) as our source of randomness).
   * This way, every recipient of the view message can check that every other recipient can decrypt it
   * (i.e. transparency).
   */
  case object EciesP256HmacSha256Aes128Cbc extends EncryptionKeyScheme {
    override val name: String = "ECIES-P256_HMAC256_AES128-CBC"
  }

  case object Rsa2048OaepSha256 extends EncryptionKeyScheme {
    override val name: String = "RSA2048-OAEP-SHA256"
  }

  def fromProtoEnum(
      field: String,
      schemeP: v30.EncryptionKeyScheme,
  ): ParsingResult[EncryptionKeyScheme] =
    schemeP match {
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.EncryptionKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_ECIES_P256_HKDF_HMAC_SHA256_AES128GCM =>
        Right(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm)
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_ECIES_P256_HMAC_SHA256A_ES128CBC =>
        Right(EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc)
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_RSA2048_OAEP_SHA256 =>
        Right(EncryptionKeyScheme.Rsa2048OaepSha256)
    }

  def fromProtoEnumToEncryptionKeySpec(
      field: String,
      schemeP: v30.EncryptionKeyScheme,
  ): ParsingResult[EncryptionKeySpec] =
    schemeP match {
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.EncryptionKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_ECIES_P256_HKDF_HMAC_SHA256_AES128GCM =>
        Right(EncryptionKeySpec.EcP256)
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_ECIES_P256_HMAC_SHA256A_ES128CBC =>
        Right(EncryptionKeySpec.EcP256)
      case v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_RSA2048_OAEP_SHA256 =>
        Right(EncryptionKeySpec.Rsa2048)
    }
}

/** Key schemes for symmetric encryption. */
sealed trait SymmetricKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v30.SymmetricKeyScheme
  def keySizeInBytes: Int
  override def pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SymmetricKeyScheme {

  implicit val symmetricKeySchemeOrder: Order[SymmetricKeyScheme] =
    Order.by[SymmetricKeyScheme, String](_.name)

  /** AES with 128bit key in GCM */
  case object Aes128Gcm extends SymmetricKeyScheme {
    override def name: String = "AES128-GCM"
    override def toProtoEnum: v30.SymmetricKeyScheme =
      v30.SymmetricKeyScheme.SYMMETRIC_KEY_SCHEME_AES128GCM
    override def keySizeInBytes: Int = 16
  }

  def fromProtoEnum(
      field: String,
      schemeP: v30.SymmetricKeyScheme,
  ): ParsingResult[SymmetricKeyScheme] =
    schemeP match {
      case v30.SymmetricKeyScheme.SYMMETRIC_KEY_SCHEME_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.SymmetricKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SymmetricKeyScheme.SYMMETRIC_KEY_SCHEME_AES128GCM =>
        Right(SymmetricKeyScheme.Aes128Gcm)
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

  protected def toProtoV30: v30.SymmetricKey =
    v30.SymmetricKey(format = format.toProtoEnum, key = key, scheme = scheme.toProtoEnum)
}

object SymmetricKey extends HasVersionedMessageCompanion[SymmetricKey] {
  override val name: String = "SymmetricKey"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.SymmetricKey)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def fromProtoV30(keyP: v30.SymmetricKey): ParsingResult[SymmetricKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", keyP.format)
      scheme <- SymmetricKeyScheme.fromProtoEnum("scheme", keyP.scheme)
    } yield new SymmetricKey(format, keyP.key, scheme)
}

final case class EncryptionKeyPair(publicKey: EncryptionPublicKey, privateKey: EncryptionPrivateKey)
    extends CryptoKeyPair[EncryptionPublicKey, EncryptionPrivateKey]
    with NoCopy {

  def toProtoV30: v30.EncryptionKeyPair =
    v30.EncryptionKeyPair(Some(publicKey.toProtoV30), Some(privateKey.toProtoV30))

  protected def toProtoCryptoKeyPairPairV30: v30.CryptoKeyPair.Pair =
    v30.CryptoKeyPair.Pair.EncryptionKeyPair(toProtoV30)
}

object EncryptionKeyPair {

  private[this] def apply(
      publicKey: EncryptionPublicKey,
      privateKey: EncryptionPrivateKey,
  ): EncryptionKeyPair =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  private[crypto] def create(
      format: CryptoKeyFormat,
      publicKeyBytes: ByteString,
      privateKeyBytes: ByteString,
      keySpec: EncryptionKeySpec,
  ): EncryptionKeyPair = {
    val publicKey = new EncryptionPublicKey(format, publicKeyBytes, keySpec)
    val privateKey = new EncryptionPrivateKey(publicKey.id, format, privateKeyBytes, keySpec)
    new EncryptionKeyPair(publicKey, privateKey)
  }

  def fromProtoV30(
      encryptionKeyPairP: v30.EncryptionKeyPair
  ): ParsingResult[EncryptionKeyPair] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        EncryptionPublicKey.fromProtoV30,
        "public_key",
        encryptionKeyPairP.publicKey,
      )
      privateKey <- ProtoConverter.parseRequired(
        EncryptionPrivateKey.fromProtoV30,
        "private_key",
        encryptionKeyPairP.privateKey,
      )
    } yield new EncryptionKeyPair(publicKey, privateKey)
}

final case class EncryptionPublicKey private[crypto] (
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    keySpec: EncryptionKeySpec,
) extends PublicKey
    with PrettyPrinting
    with HasVersionedWrapper[EncryptionPublicKey] {

  override protected def companionObj: EncryptionPublicKey.type = EncryptionPublicKey

  // TODO(#15649): Make EncryptionPublicKey object invariant
  protected def validated: Either[ProtoDeserializationError.CryptoDeserializationError, this.type] =
    CryptoKeyValidation
      .parseAndValidatePublicKey(
        this,
        errMsg =>
          ProtoDeserializationError.CryptoDeserializationError(DefaultDeserializationError(errMsg)),
      )
      .map(_ => this)

  val purpose: KeyPurpose = KeyPurpose.Encryption

  def toProtoV30: v30.EncryptionPublicKey =
    v30.EncryptionPublicKey(
      format = format.toProtoEnum,
      publicKey = key,
      // we no longer use this field so we set this scheme as unspecified
      scheme = v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_UNSPECIFIED,
      keySpec = keySpec.toProtoEnum,
    )

  override protected def toProtoPublicKeyKeyV30: v30.PublicKey.Key =
    v30.PublicKey.Key.EncryptionPublicKey(toProtoV30)

  override val pretty: Pretty[EncryptionPublicKey] =
    prettyOfClass(param("id", _.id), param("format", _.format), param("keySpec", _.keySpec))
}

object EncryptionPublicKey
    extends HasVersionedMessageCompanion[EncryptionPublicKey]
    with HasVersionedMessageCompanionDbHelpers[EncryptionPublicKey] {
  override def name: String = "encryption public key"
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.EncryptionPublicKey)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[crypto] def create(
      format: CryptoKeyFormat,
      key: ByteString,
      keySpec: EncryptionKeySpec,
  ): Either[ProtoDeserializationError.CryptoDeserializationError, EncryptionPublicKey] =
    new EncryptionPublicKey(format, key, keySpec).validated

  @nowarn("cat=deprecation")
  def fromProtoV30(
      publicKeyP: v30.EncryptionPublicKey
  ): ParsingResult[EncryptionPublicKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", publicKeyP.format)
      // if keySpec is unspecified, use the old [[EncryptionKeyScheme]] from the public key
      keySpec <- EncryptionKeySpec
        .fromProtoEnum("keySpec", publicKeyP.keySpec)
        .leftFlatMap {
          case ProtoDeserializationError.FieldNotSet(_) =>
            EncryptionKeyScheme.fromProtoEnumToEncryptionKeySpec("scheme", publicKeyP.scheme)
          case err => Left(err)
        }
      encryptionPublicKey <- EncryptionPublicKey.create(
        format,
        publicKeyP.publicKey,
        keySpec,
      )
    } yield encryptionPublicKey
}

final case class EncryptionPublicKeyWithName(
    override val publicKey: EncryptionPublicKey,
    override val name: Option[KeyName],
) extends PublicKeyWithName
    with PrettyPrinting {

  type K = EncryptionPublicKey

  override val id: Fingerprint = publicKey.id

  override def pretty: Pretty[EncryptionPublicKeyWithName] =
    prettyOfClass(param("publicKey", _.publicKey), param("name", _.name))
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
    keySpec: EncryptionKeySpec,
) extends PrivateKey
    with HasVersionedWrapper[EncryptionPrivateKey]
    with NoCopy {

  override protected def companionObj = EncryptionPrivateKey

  override def purpose: KeyPurpose = KeyPurpose.Encryption

  def toProtoV30: v30.EncryptionPrivateKey =
    v30.EncryptionPrivateKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      privateKey = key,
      // we no longer use this field so we set this scheme as unspecified
      scheme = v30.EncryptionKeyScheme.ENCRYPTION_KEY_SCHEME_UNSPECIFIED,
      keySpec = keySpec.toProtoEnum,
    )

  override protected def toProtoPrivateKeyKeyV30: v30.PrivateKey.Key =
    v30.PrivateKey.Key.EncryptionPrivateKey(toProtoV30)
}

object EncryptionPrivateKey extends HasVersionedMessageCompanion[EncryptionPrivateKey] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.EncryptionPrivateKey)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  override def name: String = "encryption private key"

  @nowarn("cat=deprecation")
  def fromProtoV30(
      privateKeyP: v30.EncryptionPrivateKey
  ): ParsingResult[EncryptionPrivateKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(privateKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", privateKeyP.format)
      keySpecET = EncryptionKeySpec.fromProtoEnum("keySpec", privateKeyP.keySpec)
      // if keySpec is unspecified, use the old [[EncryptionKeyScheme]] from the private key
      keySpec <- keySpecET match {
        case Left(_: ProtoDeserializationError.FieldNotSet) =>
          EncryptionKeyScheme.fromProtoEnumToEncryptionKeySpec("scheme", privateKeyP.scheme)
        case value => value
      }
    } yield new EncryptionPrivateKey(id, format, privateKeyP.privateKey, keySpec)
}

sealed trait EncryptionError extends Product with Serializable with PrettyPrinting
object EncryptionError {
  final case class UnsupportedAlgorithmSpec(
      algorithmSpec: EncryptionAlgorithmSpec,
      supportedAlgorithmSpec: Set[EncryptionAlgorithmSpec],
  ) extends EncryptionError {
    override def pretty: Pretty[UnsupportedAlgorithmSpec] = prettyOfClass(
      param("algorithmSpec", _.algorithmSpec),
      param("supportedAlgorithmSpec", _.supportedAlgorithmSpec),
    )
  }
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
  final case class UnsupportedAlgorithmSpec(
      algorithmSpec: EncryptionAlgorithmSpec,
      supportedAlgorithmSpecs: Set[EncryptionAlgorithmSpec],
  ) extends DecryptionError {
    override def pretty: Pretty[UnsupportedAlgorithmSpec] = prettyOfClass(
      param("algorithmSpec", _.algorithmSpec),
      param("supportedAlgorithmSpecs", _.supportedAlgorithmSpecs),
    )
  }
  final case class UnsupportedKeySpec(
      encryptionKeySpec: EncryptionKeySpec,
      supportedKeySpecs: Set[EncryptionKeySpec],
  ) extends DecryptionError {
    override def pretty: Pretty[UnsupportedKeySpec] = prettyOfClass(
      param("encryptionKeySpec", _.encryptionKeySpec),
      param("supportedKeySpecs", _.supportedKeySpecs),
    )
  }
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
