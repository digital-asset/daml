// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{CantonConfigValidator, UniformCantonConfigValidation}
import com.digitalasset.canton.crypto.provider.jce.JcePrivateCrypto
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPrivateStoreExtended}
import com.digitalasset.canton.error.{CantonBaseError, CantonErrorGroups}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.sequencing.protocol.MaxRequestSizeToDeserialize
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  CryptoParseAndValidationError,
  DeserializationError,
  ProtoConverter,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

/** Encryption operations that do not require access to a private key store but operates with
  * provided keys.
  */
trait EncryptionOps {

  private[crypto] def decryptWithInternal[M](
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

  def encryptionAlgorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec]

  /** Encrypts the bytes of the serialized message using the given public key. */
  def encryptWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = encryptionAlgorithmSpecs.default,
  ): Either[EncryptionError, AsymmetricEncrypted[M]]

  /** Deterministically encrypts the given bytes using the given public key. This is unsafe for
    * general use, and it's only used to encrypt the decryption key of each view
    */
  def encryptDeterministicWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = encryptionAlgorithmSpecs.default,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](encrypted: AsymmetricEncrypted[M], privateKey: EncryptionPrivateKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] = decryptWithInternal(encrypted, privateKey)(deserialize)

  /** Encrypts the bytes of the serialized message using the given symmetric key. Where the message
    * embedded protocol version determines the message serialization.
    */
  def encryptSymmetricWith[M <: HasToByteString](
      message: M,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, Encrypted[M]] =
    encryptSymmetricWith(message.toByteString, symmetricKey).map { ciphertext =>
      new Encrypted[M](ciphertext)
    }

  private[crypto] def encryptSymmetricWith(
      data: ByteString,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, ByteString]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M]

}

/** Encryption operations that require access to stored private keys. */
trait EncryptionPrivateOps {

  def encryptionAlgorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec]
  def encryptionKeySpecs: CryptoScheme[EncryptionKeySpec]

  /** Decrypts an encrypted message using the referenced private encryption key */
  def decrypt[M](encrypted: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DecryptionError, M]

  /** Generates a new encryption key pair with the given scheme and optional name, stores the
    * private key and returns the public key.
    */
  def generateEncryptionKey(
      keySpec: EncryptionKeySpec = encryptionKeySpecs.default,
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
      keySpec: EncryptionKeySpec = encryptionKeySpecs.default,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      keypair <- generateEncryptionKeypair(keySpec)
      _ <- store
        .storeDecryptionKey(keypair.privateKey, name)
        .leftMap[EncryptionKeyGenerationError](
          EncryptionKeyGenerationError.EncryptionPrivateStoreError.apply
        )
    } yield keypair.publicKey

}

/** A tag to denote encrypted data. */
final case class Encrypted[+M] private[crypto] (ciphertext: ByteString)

object Encrypted {

  def fromByteString[M](byteString: ByteString): Encrypted[M] =
    new Encrypted[M](byteString)
}

/** Represents an asymmetric encrypted message.
  *
  * @param ciphertext
  *   the encrypted message
  * @param encryptionAlgorithmSpec
  *   the encryption algorithm specification (e.g. RSA OAEP)
  * @param encryptedFor
  *   the public key of the recipient
  */
final case class AsymmetricEncrypted[+M](
    ciphertext: ByteString,
    encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
    encryptedFor: Fingerprint,
) extends NoCopy
    with HasVersionedWrapper[AsymmetricEncrypted[?]] {
  def encrypted: Encrypted[M] = new Encrypted(ciphertext)

  override protected def companionObj: AsymmetricEncrypted.type = AsymmetricEncrypted

  def toProtoV30: v30.AsymmetricEncrypted = v30.AsymmetricEncrypted(
    ciphertext,
    encryptionAlgorithmSpec.toProtoEnum,
    fingerprint = encryptedFor.toProtoPrimitive,
  )
}

object AsymmetricEncrypted extends HasVersionedMessageCompanion[AsymmetricEncrypted[?]] {
  override val name: String = "AsymmetricEncrypted"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v34,
      supportedProtoVersion(v30.AsymmetricEncrypted)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def fromProtoV30[T](
      encryptedP: v30.AsymmetricEncrypted
  ): ParsingResult[AsymmetricEncrypted[T]] =
    for {
      fingerprint <- Fingerprint.fromProtoPrimitive(encryptedP.fingerprint)
      encryptionAlgorithmSpec <- EncryptionAlgorithmSpec.fromProtoEnum(
        "encryption_algorithm_spec",
        encryptedP.encryptionAlgorithmSpec,
      )
      ciphertext = encryptedP.ciphertext
    } yield AsymmetricEncrypted(ciphertext, encryptionAlgorithmSpec, fingerprint)
}

/** An encryption key specification. */
sealed trait EncryptionKeySpec
    extends Product
    with Serializable
    with PrettyPrinting
    with UniformCantonConfigValidation {
  def name: String
  def toProtoEnum: v30.EncryptionKeySpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionKeySpec {

  implicit val encryptionKeySpecOrder: Order[EncryptionKeySpec] =
    Order.by[EncryptionKeySpec, String](_.name)

  implicit val encryptionKeySpecCantonConfigValidator: CantonConfigValidator[EncryptionKeySpec] =
    CantonConfigValidatorDerivation[EncryptionKeySpec]

  /** Elliptic Curve Key from the P-256 curve (aka Secp256r1) as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP256 extends EncryptionKeySpec with EcKeySpec {
    override val name: String = "EC-P256"
    override def toProtoEnum: v30.EncryptionKeySpec =
      v30.EncryptionKeySpec.ENCRYPTION_KEY_SPEC_EC_P256
    // Name of the elliptic curve as expected by Java's ECGenParameterSpec (JCA standard name)
    override val jcaCurveName: String = "secp256r1"
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

  /** If keySpec is unspecified, use the old EncryptionKeyScheme from the key */
  def fromProtoEnumWithDefaultScheme(
      keySpecP: v30.EncryptionKeySpec,
      keySchemeP: v30.EncryptionKeyScheme,
  ): ParsingResult[EncryptionKeySpec] =
    EncryptionKeySpec
      .fromProtoEnum("key_spec", keySpecP)
      .leftFlatMap {
        case ProtoDeserializationError.FieldNotSet(_) =>
          EncryptionKeySpec.fromProtoEnumEncryptionKeyScheme("scheme", keySchemeP)
        case err => Left(err)
      }

  /** Converts an old EncryptionKeyScheme enum to the new key scheme, ensuring backward
    * compatibility with existing data.
    */
  private def fromProtoEnumEncryptionKeyScheme(
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

/** Algorithm schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionAlgorithmSpec
    extends Product
    with Serializable
    with PrettyPrinting
    with UniformCantonConfigValidation {
  def name: String
  def supportDeterministicEncryption: Boolean
  def supportedEncryptionKeySpecs: NonEmpty[Set[EncryptionKeySpec]]
  def toProtoEnum: v30.EncryptionAlgorithmSpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionAlgorithmSpec {

  implicit val encryptionAlgorithmSpecOrder: Order[EncryptionAlgorithmSpec] =
    Order.by[EncryptionAlgorithmSpec, String](_.name)

  implicit val encryptionAlgorithmSpecCantonConfigValidator
      : CantonConfigValidator[EncryptionAlgorithmSpec] =
    CantonConfigValidatorDerivation[EncryptionAlgorithmSpec]

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

  def fromProtoEnum(
      field: String,
      schemeP: v30.EncryptionAlgorithmSpec,
  ): ParsingResult[EncryptionAlgorithmSpec] =
    schemeP match {
      case v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.EncryptionAlgorithmSpec.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_ECIES_HKDF_HMAC_SHA256_AES128CBC =>
        Right(EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc)
      case v30.EncryptionAlgorithmSpec.ENCRYPTION_ALGORITHM_SPEC_RSA_OAEP_SHA256 =>
        Right(EncryptionAlgorithmSpec.RsaOaepSha256)
    }
}

/** Required encryption algorithms and keys for asymmetric/hybrid encryption to be listed in the
  * synchronizer.
  *
  * @param algorithms
  *   list of required encryption algorithm specifications
  * @param keys
  *   list of required encryption key specifications
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

/** Key schemes for symmetric encryption. */
sealed trait SymmetricKeyScheme
    extends Product
    with Serializable
    with PrettyPrinting
    with UniformCantonConfigValidation {
  def name: String
  def toProtoEnum: v30.SymmetricKeyScheme
  def keySizeInBytes: Int
  override protected def pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SymmetricKeyScheme {

  implicit val symmetricKeySchemeOrder: Order[SymmetricKeyScheme] =
    Order.by[SymmetricKeyScheme, String](_.name)

  implicit val symmetricKeySchemeCantonConfigValidator: CantonConfigValidator[SymmetricKeyScheme] =
    CantonConfigValidatorDerivation[SymmetricKeyScheme]

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

final case class SymmetricKey private (
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: SymmetricKeyScheme,
) extends CryptoKey
    with HasVersionedWrapper[SymmetricKey] {
  override protected def companionObj: SymmetricKey.type = SymmetricKey

  protected def validated: Either[EncryptionKeyCreationError, this.type] =
    CryptoKeyValidation
      .validateSymmetricKey(
        this,
        errMsg => EncryptionKeyCreationError.KeyParseAndValidateError(errMsg),
      )
      .map(_ => this)

  def toProtoV30: v30.SymmetricKey =
    v30.SymmetricKey(format = format.toProtoEnum, key = key, scheme = scheme.toProtoEnum)
}

object SymmetricKey extends HasVersionedMessageCompanion[SymmetricKey] {
  override val name: String = "SymmetricKey"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v34,
      supportedProtoVersion(v30.SymmetricKey)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def create(
      format: CryptoKeyFormat,
      key: ByteString,
      keyScheme: SymmetricKeyScheme,
  ): Either[EncryptionKeyCreationError, SymmetricKey] =
    SymmetricKey(format, key, keyScheme).validated

  def fromProtoV30(keyP: v30.SymmetricKey): ParsingResult[SymmetricKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", keyP.format)
      scheme <- SymmetricKeyScheme.fromProtoEnum("scheme", keyP.scheme)
      key <- SymmetricKey
        .create(format, keyP.key, scheme)
        .leftMap(err =>
          ProtoDeserializationError.CryptoDeserializationError(
            CryptoParseAndValidationError(err.show)
          )
        )
    } yield key
}

final case class EncryptionKeyPair private (
    publicKey: EncryptionPublicKey,
    privateKey: EncryptionPrivateKey,
) extends CryptoKeyPair[EncryptionPublicKey, EncryptionPrivateKey] {

  require(
    publicKey.keySpec == privateKey.keySpec,
    s"Public [${publicKey.keySpec}] and private[${privateKey.keySpec}] key must have the same key spec",
  )

  def toProtoV30: v30.EncryptionKeyPair =
    v30.EncryptionKeyPair(Some(privateKey.toProtoV30))

  protected def toProtoCryptoKeyPairPairV30: v30.CryptoKeyPair.Pair =
    v30.CryptoKeyPair.Pair.EncryptionKeyPair(toProtoV30)
}

object EncryptionKeyPair {

  private[crypto] def create(
      publicFormat: CryptoKeyFormat,
      publicKeyBytes: ByteString,
      privateFormat: CryptoKeyFormat,
      privateKeyBytes: ByteString,
      keySpec: EncryptionKeySpec,
  ): Either[EncryptionKeyCreationError, EncryptionKeyPair] =
    for {
      publicKey <- EncryptionPublicKey
        .create(publicFormat, publicKeyBytes, keySpec)
      privateKey <- EncryptionPrivateKey
        .create(publicKey.id, privateFormat, privateKeyBytes, keySpec)
    } yield EncryptionKeyPair.create(publicKey, privateKey)

  def create(
      privateKey: EncryptionPrivateKey
  ): Either[EncryptionKeyCreationError, EncryptionKeyPair] =
    JcePrivateCrypto
      .derivePublicKey(privateKey)
      .leftMap(EncryptionKeyCreationError.DerivePublicKeyError.apply)
      .flatMap {
        case publicKey: EncryptionPublicKey =>
          // The ID in the private key might not match the ID of the derived public key,
          // so we regenerate the private key with the correct ID. This can only happen if a malicious operator
          // tampers with the imported private key and assigns it an incorrect ID.
          EncryptionPrivateKey
            .create(publicKey.id, privateKey.format, privateKey.key, privateKey.keySpec)
            .map(EncryptionKeyPair.create(publicKey, _))
        case _ =>
          Left(
            EncryptionKeyCreationError.DerivePublicKeyError(
              "Expected an encryption public key but got a signing public key"
            )
          )
      }

  def create(
      publicKey: EncryptionPublicKey,
      privateKey: EncryptionPrivateKey,
  ): EncryptionKeyPair = EncryptionKeyPair(publicKey, privateKey)

  def fromProtoV30(
      encryptionKeyPairP: v30.EncryptionKeyPair
  ): ParsingResult[EncryptionKeyPair] =
    for {
      privateKey <- ProtoConverter.parseRequired(
        EncryptionPrivateKey.fromProtoV30,
        "private_key",
        encryptionKeyPairP.privateKey,
      )
      keyPair <- EncryptionKeyPair
        .create(privateKey)
        .leftMap(err => ProtoDeserializationError.CryptoParseAndValidationError(err.toString))
    } yield keyPair
}

final case class EncryptionPublicKey private (
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    keySpec: EncryptionKeySpec,
)(
    override val migrated: Boolean = false
) extends PublicKey
    with PrettyPrinting
    with HasVersionedWrapper[EncryptionPublicKey] {

  override type K = EncryptionPublicKey

  override protected def companionObj: EncryptionPublicKey.type = EncryptionPublicKey

  protected override val dataForFingerprintO: Option[ByteString] = None

  protected def validated: Either[EncryptionKeyCreationError.KeyParseAndValidateError, this.type] =
    CryptoKeyValidation
      .parseAndValidatePublicKey(
        this,
        errMsg => EncryptionKeyCreationError.KeyParseAndValidateError(errMsg),
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

  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  private def migrate(): Option[EncryptionPublicKey] =
    (keySpec, format) match {
      case (EncryptionKeySpec.EcP256, CryptoKeyFormat.Der) |
          (EncryptionKeySpec.Rsa2048, CryptoKeyFormat.Der) =>
        Some(EncryptionPublicKey(CryptoKeyFormat.DerX509Spki, key, keySpec)(migrated = true))

      case _ => None
    }

  @VisibleForTesting
  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  override private[canton] def reverseMigrate(): Option[K] =
    (keySpec, format) match {
      case (EncryptionKeySpec.EcP256, CryptoKeyFormat.DerX509Spki) |
          (EncryptionKeySpec.Rsa2048, CryptoKeyFormat.DerX509Spki) =>
        Some(
          EncryptionPublicKey(CryptoKeyFormat.Der, key, keySpec)()
        )

      case _ => None
    }

  @VisibleForTesting
  private[crypto] def replaceFormat(format: CryptoKeyFormat): EncryptionPublicKey =
    this.copy(format = format)(migrated)

  @VisibleForTesting
  private[crypto] def replaceKeySpec(keySpec: EncryptionKeySpec): EncryptionPublicKey =
    this.copy(keySpec = keySpec)(migrated)
}

object EncryptionPublicKey
    extends HasVersionedMessageCompanion[EncryptionPublicKey]
    with HasVersionedMessageCompanionDbHelpers[EncryptionPublicKey] {
  override def name: String = "encryption public key"
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v34,
      supportedProtoVersion(v30.EncryptionPublicKey)(fromProtoV30),
      _.toProtoV30,
    )
  )

  /** Creates a [[EncryptionPublicKey]] from the given parameters. Performs validations on usage and
    * format. If the [[EncryptionKeySpec]] is EC-based, it also validates that the public key lies
    * on the expected curve.
    */
  private[crypto] def create(
      format: CryptoKeyFormat,
      key: ByteString,
      keySpec: EncryptionKeySpec,
  ): Either[EncryptionKeyCreationError, EncryptionPublicKey] = {
    val keyBeforeMigration = EncryptionPublicKey(format, key, keySpec)()
    val keyAfterMigration = keyBeforeMigration.migrate().getOrElse(keyBeforeMigration)

    keyAfterMigration.validated
  }

  @nowarn("cat=deprecation")
  def fromProtoV30(
      publicKeyP: v30.EncryptionPublicKey
  ): ParsingResult[EncryptionPublicKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", publicKeyP.format)
      keySpec <- EncryptionKeySpec.fromProtoEnumWithDefaultScheme(
        publicKeyP.keySpec,
        publicKeyP.scheme,
      )
      encryptionPublicKey <- EncryptionPublicKey
        .create(
          format,
          publicKeyP.publicKey,
          keySpec,
        )
        .leftMap(err =>
          ProtoDeserializationError.CryptoDeserializationError(
            CryptoParseAndValidationError(err.show)
          )
        )
    } yield encryptionPublicKey
}

final case class EncryptionPublicKeyWithName(
    override val publicKey: EncryptionPublicKey,
    override val name: Option[KeyName],
) extends PublicKeyWithName
    with PrettyPrinting {

  type PK = EncryptionPublicKey

  override val id: Fingerprint = publicKey.id

  override protected def pretty: Pretty[EncryptionPublicKeyWithName] =
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

final case class EncryptionPrivateKey private (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    keySpec: EncryptionKeySpec,
)(
    override val migrated: Boolean = false
) extends PrivateKey
    with HasVersionedWrapper[EncryptionPrivateKey] {

  override type K = EncryptionPrivateKey

  override protected def companionObj: EncryptionPrivateKey.type = EncryptionPrivateKey

  override def purpose: KeyPurpose = KeyPurpose.Encryption

  protected def validated: Either[EncryptionKeyCreationError.KeyParseAndValidateError, this.type] =
    CryptoKeyValidation
      .parseAndValidatePrivateKey(
        this,
        errMsg => EncryptionKeyCreationError.KeyParseAndValidateError(errMsg),
      )
      .map(_ => this)

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

  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  private def migrate(): Option[EncryptionPrivateKey] =
    (keySpec, format) match {
      case (EncryptionKeySpec.EcP256, CryptoKeyFormat.Der) |
          (EncryptionKeySpec.Rsa2048, CryptoKeyFormat.Der) =>
        Some(EncryptionPrivateKey(id, CryptoKeyFormat.DerPkcs8Pki, key, keySpec)(migrated = true))

      case _ => None
    }

  @VisibleForTesting
  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  override private[canton] def reverseMigrate(): Option[K] =
    (keySpec, format) match {
      case (EncryptionKeySpec.EcP256, CryptoKeyFormat.DerPkcs8Pki) |
          (EncryptionKeySpec.Rsa2048, CryptoKeyFormat.DerPkcs8Pki) =>
        Some(
          EncryptionPrivateKey(id, CryptoKeyFormat.Der, key, keySpec)()
        )

      case _ => None
    }

  @VisibleForTesting
  private[crypto] def replaceFormat(format: CryptoKeyFormat): EncryptionPrivateKey =
    EncryptionPrivateKey(this.id, format, this.key, this.keySpec)(migrated)

}

object EncryptionPrivateKey extends HasVersionedMessageCompanion[EncryptionPrivateKey] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v34,
      supportedProtoVersion(v30.EncryptionPrivateKey)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "encryption private key"

  private[crypto] def create(
      id: Fingerprint,
      format: CryptoKeyFormat,
      key: ByteString,
      keySpec: EncryptionKeySpec,
  ): Either[EncryptionKeyCreationError, EncryptionPrivateKey] = {
    val keyBeforeMigration = EncryptionPrivateKey(id, format, key, keySpec)()
    val keyAfterMigration = keyBeforeMigration.migrate().getOrElse(keyBeforeMigration)
    keyAfterMigration.validated
  }

  @nowarn("cat=deprecation")
  def fromProtoV30(
      privateKeyP: v30.EncryptionPrivateKey
  ): ParsingResult[EncryptionPrivateKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(privateKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", privateKeyP.format)
      keySpec <- EncryptionKeySpec.fromProtoEnumWithDefaultScheme(
        privateKeyP.keySpec,
        privateKeyP.scheme,
      )
      epk <- EncryptionPrivateKey
        .create(id, format, privateKeyP.privateKey, keySpec)
        .leftMap(err =>
          ProtoDeserializationError.CryptoDeserializationError(
            CryptoParseAndValidationError(err.show)
          )
        )
    } yield epk
}

sealed trait EncryptionError extends Product with Serializable with PrettyPrinting
object EncryptionError {
  final case class UnsupportedAlgorithmSpec(
      algorithmSpec: EncryptionAlgorithmSpec,
      supportedAlgorithmSpec: Set[EncryptionAlgorithmSpec],
  ) extends EncryptionError {
    override protected def pretty: Pretty[UnsupportedAlgorithmSpec] = prettyOfClass(
      param("algorithmSpec", _.algorithmSpec),
      param("supportedAlgorithmSpec", _.supportedAlgorithmSpec),
    )
  }
  final case class UnsupportedKeyFormat(
      keyFormat: CryptoKeyFormat,
      supportedKeyFormats: Set[CryptoKeyFormat],
  ) extends EncryptionError {
    override protected def pretty: Pretty[UnsupportedKeyFormat] = prettyOfClass(
      param("format", _.keyFormat),
      param("supportedKeyFormats", _.supportedKeyFormats),
    )
  }
  final case class UnsupportedSchemeForDeterministicEncryption(error: String)
      extends EncryptionError {
    override protected def pretty: Pretty[UnsupportedSchemeForDeterministicEncryption] =
      prettyOfClass(
        unnamedParam(_.error.unquoted)
      )
  }
  final case class NoMatchingAlgorithmSpec(message: String) extends EncryptionError {
    override protected def pretty: Pretty[NoMatchingAlgorithmSpec] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }
  final case class FailedToEncrypt(error: String) extends EncryptionError {
    override protected def pretty: Pretty[FailedToEncrypt] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class InvalidSymmetricKey(error: String) extends EncryptionError {
    override protected def pretty: Pretty[InvalidSymmetricKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class InvalidEncryptionKey(error: String) extends EncryptionError {
    override protected def pretty: Pretty[InvalidEncryptionKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  @Explanation(
    """This error means that the view size has exceeded the configured value for its size."""
  )
  @Resolution(
    """Reduce the size of the payloads or increase the max request size in the dynamic synchronizer parameters."""
  )
  final case class MaxViewSizeExceeded(
      viewSize: Int,
      maxRequestSize: MaxRequestSizeToDeserialize.Limit,
  ) extends EncryptionError {
    override protected def pretty: Pretty[MaxViewSizeExceeded] = prettyOfClass(
      param("view size", _.viewSize),
      param("max request size configured", _.maxRequestSize.value),
    )
  }
}

sealed trait DecryptionError extends Product with Serializable with PrettyPrinting
object DecryptionError {
  final case class UnsupportedAlgorithmSpec(
      algorithmSpec: EncryptionAlgorithmSpec,
      supportedAlgorithmSpecs: Set[EncryptionAlgorithmSpec],
  ) extends DecryptionError {
    override protected def pretty: Pretty[UnsupportedAlgorithmSpec] = prettyOfClass(
      param("algorithmSpec", _.algorithmSpec),
      param("supportedAlgorithmSpecs", _.supportedAlgorithmSpecs),
    )
  }
  final case class KeyAlgoSpecsMismatch(
      encryptionKeySpec: EncryptionKeySpec,
      algorithmSpec: EncryptionAlgorithmSpec,
      supportedKeySpecsByAlgo: Set[EncryptionKeySpec],
  ) extends DecryptionError {
    override def pretty: Pretty[KeyAlgoSpecsMismatch] = prettyOfClass(
      param("encryptionKeySpec", _.encryptionKeySpec),
      param("algorithmSpec", _.algorithmSpec),
      param("supportedKeySpecsByAlgo", _.supportedKeySpecsByAlgo),
    )
  }
  final case class UnsupportedKeySpec(
      encryptionKeySpec: EncryptionKeySpec,
      supportedKeySpecs: Set[EncryptionKeySpec],
  ) extends DecryptionError {
    override protected def pretty: Pretty[UnsupportedKeySpec] = prettyOfClass(
      param("encryptionKeySpec", _.encryptionKeySpec),
      param("supportedKeySpecs", _.supportedKeySpecs),
    )
  }
  final case class UnsupportedKeyFormat(
      keyFormat: CryptoKeyFormat,
      supportedKeyFormats: Set[CryptoKeyFormat],
  ) extends DecryptionError {
    override protected def pretty: Pretty[UnsupportedKeyFormat] = prettyOfClass(
      param("format", _.keyFormat),
      param("supportedKeyFormats", _.supportedKeyFormats),
    )
  }
  final case class UnsupportedSymmetricKeySpec(
      symmetricKeySpec: SymmetricKeyScheme,
      supportedKeySpecs: Set[SymmetricKeyScheme],
  ) extends DecryptionError {
    override protected def pretty: Pretty[UnsupportedSymmetricKeySpec] = prettyOfClass(
      param("symmetricKeySpec", _.symmetricKeySpec),
      param("supportedKeySpecs", _.supportedKeySpecs),
    )
  }
  final case class FailedToDecrypt(error: String) extends DecryptionError {
    override protected def pretty: Pretty[FailedToDecrypt] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class InvalidSymmetricKey(error: String) extends DecryptionError {
    override protected def pretty: Pretty[InvalidSymmetricKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class InvariantViolation(error: String) extends DecryptionError {
    override protected def pretty: Pretty[InvariantViolation] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class InvalidEncryptionKey(error: String) extends DecryptionError {
    override protected def pretty: Pretty[InvalidEncryptionKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class UnknownEncryptionKey(keyId: Fingerprint) extends DecryptionError {
    override protected def pretty: Pretty[UnknownEncryptionKey] = prettyOfClass(
      param("keyId", _.keyId)
    )
  }
  final case class DecryptionWithWrongKey(error: String) extends DecryptionError {
    override protected def pretty: Pretty[DecryptionWithWrongKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class FailedToDeserialize(error: DeserializationError) extends DecryptionError {
    override protected def pretty: Pretty[FailedToDeserialize] = prettyOfClass(
      unnamedParam(_.error)
    )
  }
  final case class KeyStoreError(error: String) extends DecryptionError {
    override protected def pretty: Pretty[KeyStoreError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}

/** Errors that happen when generating new encryption keys.
  *
  * This means creating key material from scratch. Different from errors that happen when creating
  * keys from existing key material.
  */
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
        extends CantonBaseError.Impl(cause = "Unable to create encryption key")
  }

  final case class GeneralError(error: Exception) extends EncryptionKeyGenerationError {
    override protected def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class GeneralKmsError(error: String) extends EncryptionKeyGenerationError {
    override protected def pretty: Pretty[GeneralKmsError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class KeyCreationError(error: EncryptionKeyCreationError)
      extends EncryptionKeyGenerationError {
    override protected def pretty: Pretty[KeyCreationError] = prettyOfParam(
      _.error
    )
  }

  final case class FingerprintError(error: String) extends EncryptionKeyGenerationError {
    override protected def pretty: Pretty[FingerprintError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class UnsupportedKeySpec(
      keySpec: EncryptionKeySpec,
      supportedKeySpecs: Set[EncryptionKeySpec],
  ) extends EncryptionKeyGenerationError {
    override protected def pretty: Pretty[UnsupportedKeySpec] = prettyOfClass(
      param("keySpec", _.keySpec),
      param("supportedKeySpecs", _.supportedKeySpecs),
    )
  }

  final case class EncryptionPrivateStoreError(error: CryptoPrivateStoreError)
      extends EncryptionKeyGenerationError {
    override protected def pretty: Pretty[EncryptionPrivateStoreError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

}

/** Errors that happen when creating encryption keys from existing key material.
  *
  * This includes parsing, validating, or checking the key data. Different from errors that happen
  * during key generation (creating new key material).
  */
sealed trait EncryptionKeyCreationError extends Product with Serializable with PrettyPrinting
object EncryptionKeyCreationError extends CantonErrorGroups.CommandErrorGroup {

  @Explanation("This error indicates that an encryption key could not be created.")
  @Resolution("Inspect the error details")
  object ErrorCode
      extends ErrorCode(
        id = "ENCRYPTION_KEY_CREATION_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Wrap(reason: EncryptionKeyCreationError)
        extends CantonBaseError.Impl(cause = "An error occurred during encryption key creation")

    final case class WrapStr(reason: String)
        extends CantonBaseError.Impl(cause = "An error occurred during encryption key creation")
  }

  final case class KeyParseAndValidateError(error: String) extends EncryptionKeyCreationError {
    override protected def pretty: Pretty[KeyParseAndValidateError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class DerivePublicKeyError(error: String) extends EncryptionKeyCreationError {
    override protected def pretty: Pretty[DerivePublicKeyError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}
