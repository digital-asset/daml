// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStoreError,
  CryptoPrivateStoreExtended,
  CryptoPublicStoreError,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DefaultDeserializationError, ProtoConverter}
import com.digitalasset.canton.topology.KeyOwner
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}

/** Signing operations that do not require access to a private key store but operates with provided keys. */
trait SigningOps {

  /** Signs the given hash using the private signing key. */
  def sign(hash: Hash, signingKey: SigningPrivateKey): Either[SigningError, Signature] =
    sign(hash.getCryptographicEvidence, signingKey)

  protected[crypto] def sign(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
  ): Either[SigningError, Signature]

  /** Confirms if the provided signature is a valid signature of the payload using the public key */
  def verifySignature(
      hash: Hash,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit] =
    verifySignature(hash.getCryptographicEvidence, publicKey, signature)

  protected[crypto] def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit]
}

/** Signing operations that require access to stored private keys. */
trait SigningPrivateOps {

  def defaultSigningKeyScheme: SigningKeyScheme

  /** Signs the given hash using the referenced private signing key. */
  def sign(hash: Hash, signingKeyId: Fingerprint)(implicit
      tc: TraceContext
  ): EitherT[Future, SigningError, Signature] =
    sign(hash.getCryptographicEvidence, signingKeyId)

  /** Signs the byte string directly, however it is encouraged to sign a hash. */
  protected[crypto] def sign(
      bytes: ByteString,
      signingKeyId: Fingerprint,
  )(implicit tc: TraceContext): EitherT[Future, SigningError, Signature]

  /** Generates a new signing key pair with the given scheme and optional name, stores the private key and returns the public key. */
  def generateSigningKey(
      scheme: SigningKeyScheme = defaultSigningKeyScheme,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningPublicKey]

}

/** A default implementation with a private key store */
trait SigningPrivateStoreOps extends SigningPrivateOps {

  implicit val ec: ExecutionContext

  protected val store: CryptoPrivateStoreExtended

  protected val signingOps: SigningOps

  protected[crypto] def sign(
      bytes: ByteString,
      signingKeyId: Fingerprint,
  )(implicit tc: TraceContext): EitherT[Future, SigningError, Signature] =
    store
      .signingKey(signingKeyId)(TraceContext.todo)
      .leftMap(storeError => SigningError.KeyStoreError(storeError.show))
      .subflatMap(_.toRight(SigningError.UnknownSigningKey(signingKeyId)))
      .subflatMap(signingKey => signingOps.sign(bytes, signingKey))

  /** Internal method to generate and return the entire signing key pair */
  protected[crypto] def generateSigningKeypair(scheme: SigningKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningKeyPair]

  def generateSigningKey(
      scheme: SigningKeyScheme,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningPublicKey] =
    for {
      keypair <- generateSigningKeypair(scheme)
      _ <- store
        .storeSigningKey(keypair.privateKey, name)
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.SigningPrivateStoreError)
    } yield keypair.publicKey

}

final case class Signature private[crypto] (
    format: SignatureFormat,
    private val signature: ByteString,
    signedBy: Fingerprint,
) extends HasVersionedWrapper[Signature]
    with PrettyPrinting
    with NoCopy {

  override protected def companionObj = Signature

  def toProtoV0: v0.Signature =
    v0.Signature(
      format = format.toProtoEnum,
      signature = signature,
      signedBy = signedBy.toProtoPrimitive,
    )

  override def pretty: Pretty[Signature] =
    prettyOfClass(param("signature", _.signature), param("signedBy", _.signedBy))

  /** Access to the raw signature, must NOT be used for serialization */
  private[crypto] def unwrap: ByteString = signature
}

object Signature
    extends HasVersionedMessageCompanion[Signature]
    with HasVersionedMessageCompanionDbHelpers[Signature] {
  val noSignature =
    new Signature(
      SignatureFormat.Raw,
      ByteString.EMPTY,
      Fingerprint.tryCreate("no-fingerprint"),
    )
  val noSignatures = NonEmpty(Set, noSignature)

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.Signature)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  override def name: String = "signature"

  private[this] def apply(
      format: SignatureFormat,
      signature: ByteString,
      signedBy: Fingerprint,
  ): Signature =
    throw new UnsupportedOperationException("Use deserialization method instead")

  def fromProtoV0(signatureP: v0.Signature): ParsingResult[Signature] =
    for {
      format <- SignatureFormat.fromProtoEnum("format", signatureP.format)
      signature = signatureP.signature
      signedBy <- Fingerprint.fromProtoPrimitive(signatureP.signedBy)
    } yield new Signature(format, signature, signedBy)
}

sealed trait SignatureFormat extends Product with Serializable {
  def toProtoEnum: v0.SignatureFormat
}

object SignatureFormat {
  case object Raw extends SignatureFormat {
    override def toProtoEnum: v0.SignatureFormat = v0.SignatureFormat.RawSignatureFormat
  }

  def fromProtoEnum(
      field: String,
      formatP: v0.SignatureFormat,
  ): ParsingResult[SignatureFormat] =
    formatP match {
      case v0.SignatureFormat.MissingSignatureFormat =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.SignatureFormat.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.SignatureFormat.RawSignatureFormat => Right(SignatureFormat.Raw)
    }
}

sealed trait SigningKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v0.SigningKeyScheme
  def pretty: Pretty[this.type] = prettyOfString(_.name)
}

/** Schemes for signature keys.
  *
  * Ed25519 is the best performing curve and should be the default.
  * EC-DSA is slower than Ed25519 but has better compatibility with other systems (such as CCF).
  */
object SigningKeyScheme {
  implicit val signingKeySchemeOrder: Order[SigningKeyScheme] =
    Order.by[SigningKeyScheme, String](_.name)

  case object Ed25519 extends SigningKeyScheme {
    override val name: String = "Ed25519"
    override def toProtoEnum: v0.SigningKeyScheme = v0.SigningKeyScheme.Ed25519
  }

  case object EcDsaP256 extends SigningKeyScheme {
    override def name: String = "ECDSA-P256"
    override def toProtoEnum: v0.SigningKeyScheme = v0.SigningKeyScheme.EcDsaP256
  }

  case object EcDsaP384 extends SigningKeyScheme {
    override def name: String = "ECDSA-P384"
    override def toProtoEnum: v0.SigningKeyScheme = v0.SigningKeyScheme.EcDsaP384
  }

  val EdDsaSchemes: NonEmpty[Set[SigningKeyScheme]] = NonEmpty.mk(Set, Ed25519)
  val EcDsaSchemes: NonEmpty[Set[SigningKeyScheme]] = NonEmpty.mk(Set, EcDsaP256, EcDsaP384)

  val allSchemes: NonEmpty[Set[SigningKeyScheme]] = NonEmpty.mk(Set, Ed25519, EcDsaP256, EcDsaP384)

  def fromProtoEnum(
      field: String,
      schemeP: v0.SigningKeyScheme,
  ): ParsingResult[SigningKeyScheme] =
    schemeP match {
      case v0.SigningKeyScheme.MissingSigningKeyScheme =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.SigningKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.SigningKeyScheme.Ed25519 => Right(SigningKeyScheme.Ed25519)
      case v0.SigningKeyScheme.EcDsaP256 => Right(SigningKeyScheme.EcDsaP256)
      case v0.SigningKeyScheme.EcDsaP384 => Right(SigningKeyScheme.EcDsaP384)
      case v0.SigningKeyScheme.Sm2 =>
        Left(ProtoDeserializationError.OtherError("Support for SM2 has been removed since v2.7"))
    }
}

final case class SigningKeyPair(publicKey: SigningPublicKey, privateKey: SigningPrivateKey)
    extends CryptoKeyPair[SigningPublicKey, SigningPrivateKey]
    with NoCopy {

  protected def toProtoV0: v0.SigningKeyPair =
    v0.SigningKeyPair(Some(publicKey.toProtoV0), Some(privateKey.toProtoV0))

  protected def toProtoCryptoKeyPairPairV0: v0.CryptoKeyPair.Pair =
    v0.CryptoKeyPair.Pair.SigningKeyPair(toProtoV0)
}

object SigningKeyPair {

  private[this] def apply(
      publicKey: SigningPublicKey,
      privateKey: SigningPrivateKey,
  ): SigningKeyPair =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  private[crypto] def create(
      id: Fingerprint,
      format: CryptoKeyFormat,
      publicKeyBytes: ByteString,
      privateKeyBytes: ByteString,
      scheme: SigningKeyScheme,
  ): SigningKeyPair = {
    val publicKey = new SigningPublicKey(id, format, publicKeyBytes, scheme)
    val privateKey = new SigningPrivateKey(publicKey.id, format, privateKeyBytes, scheme)
    new SigningKeyPair(publicKey, privateKey)
  }

  @VisibleForTesting
  def wrongSigningKeyPairWithPublicKeyUnsafe(publicKey: SigningPublicKey): SigningKeyPair = {
    val privateKey =
      new SigningPrivateKey(publicKey.id, publicKey.format, publicKey.key, publicKey.scheme)
    new SigningKeyPair(publicKey, privateKey)
  }

  def fromProtoV0(
      signingKeyPairP: v0.SigningKeyPair
  ): ParsingResult[SigningKeyPair] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV0,
        "public_key",
        signingKeyPairP.publicKey,
      )
      privateKey <- ProtoConverter.parseRequired(
        SigningPrivateKey.fromProtoV0,
        "private_key",
        signingKeyPairP.privateKey,
      )
    } yield new SigningKeyPair(publicKey, privateKey)
}

@SuppressWarnings(Array("org.wartremover.warts.FinalCaseClass")) // This class is mocked in tests
case class SigningPublicKey private[crypto] (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: SigningKeyScheme,
) extends PublicKey
    with PrettyPrinting
    with HasVersionedWrapper[SigningPublicKey] {
  override val purpose: KeyPurpose = KeyPurpose.Signing

  override protected def companionObj = SigningPublicKey

  // TODO(#15649): Make SigningPublicKey object invariant
  protected def validated: Either[ProtoDeserializationError.CryptoDeserializationError, this.type] =
    CryptoKeyValidation
      .parseAndValidatePublicKey(
        this,
        errMsg =>
          ProtoDeserializationError.CryptoDeserializationError(DefaultDeserializationError(errMsg)),
      )
      .map(_ => this)

  def toProtoV0: v0.SigningPublicKey =
    v0.SigningPublicKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      publicKey = key,
      scheme = scheme.toProtoEnum,
    )

  override protected def toProtoPublicKeyKeyV0: v0.PublicKey.Key =
    v0.PublicKey.Key.SigningPublicKey(toProtoV0)

  override def pretty: Pretty[SigningPublicKey] =
    prettyOfClass(param("id", _.id), param("format", _.format), param("scheme", _.scheme))
}

object SigningPublicKey
    extends HasVersionedMessageCompanion[SigningPublicKey]
    with HasVersionedMessageCompanionDbHelpers[SigningPublicKey] {
  override def name: String = "signing public key"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.SigningPublicKey)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  private[crypto] def create(
      id: Fingerprint,
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: SigningKeyScheme,
  ): Either[ProtoDeserializationError.CryptoDeserializationError, SigningPublicKey] =
    new SigningPublicKey(id, format, key, scheme).validated

  @VisibleForTesting
  val idUnsafe: Lens[SigningPublicKey, Fingerprint] =
    GenLens[SigningPublicKey](_.id)

  def fromProtoV0(
      publicKeyP: v0.SigningPublicKey
  ): ParsingResult[SigningPublicKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(publicKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", publicKeyP.format)
      scheme <- SigningKeyScheme.fromProtoEnum("scheme", publicKeyP.scheme)
      signingPublicKey <- SigningPublicKey.create(
        id,
        format,
        publicKeyP.publicKey,
        scheme,
      )
    } yield signingPublicKey

  def collect(initialKeys: Map[KeyOwner, Seq[PublicKey]]): Map[KeyOwner, Seq[SigningPublicKey]] =
    initialKeys.map { case (k, v) =>
      (k, v.collect { case x: SigningPublicKey => x })
    }

}

final case class SigningPublicKeyWithName(
    override val publicKey: SigningPublicKey,
    override val name: Option[KeyName],
) extends PublicKeyWithName
    with PrettyPrinting {

  type K = SigningPublicKey

  override def pretty: Pretty[SigningPublicKeyWithName] =
    prettyOfClass(param("publicKey", _.publicKey), param("name", _.name))
}

object SigningPublicKeyWithName {
  implicit def getResultSigningPublicKeyWithName(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[SigningPublicKeyWithName] = GetResult { r =>
    SigningPublicKeyWithName(r.<<, r.<<)
  }
}

final case class SigningPrivateKey private[crypto] (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: SigningKeyScheme,
) extends PrivateKey
    with HasVersionedWrapper[SigningPrivateKey]
    with NoCopy {

  override protected def companionObj = SigningPrivateKey

  def toProtoV0: v0.SigningPrivateKey =
    v0.SigningPrivateKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      privateKey = key,
      scheme = scheme.toProtoEnum,
    )

  override def purpose: KeyPurpose = KeyPurpose.Signing

  override protected def toProtoPrivateKeyKeyV0: v0.PrivateKey.Key =
    v0.PrivateKey.Key.SigningPrivateKey(toProtoV0)
}

object SigningPrivateKey extends HasVersionedMessageCompanion[SigningPrivateKey] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.SigningPrivateKey)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  override def name: String = "signing private key"

  def fromProtoV0(
      privateKeyP: v0.SigningPrivateKey
  ): ParsingResult[SigningPrivateKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(privateKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", privateKeyP.format)
      scheme <- SigningKeyScheme.fromProtoEnum("scheme", privateKeyP.scheme)
    } yield new SigningPrivateKey(id, format, privateKeyP.privateKey, scheme)

}

sealed trait SigningError extends Product with Serializable with PrettyPrinting
object SigningError {

  final case class GeneralError(error: Exception) extends SigningError {
    override def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class InvariantViolation(error: String) extends SigningError {
    override def pretty: Pretty[InvariantViolation] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class InvalidSigningKey(error: String) extends SigningError {
    override def pretty: Pretty[InvalidSigningKey] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class UnknownSigningKey(keyId: Fingerprint) extends SigningError {
    override def pretty: Pretty[UnknownSigningKey] = prettyOfClass(param("keyId", _.keyId))
  }

  final case class FailedToSign(error: String) extends SigningError {
    override def pretty: Pretty[FailedToSign] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class KeyStoreError(error: String) extends SigningError {
    override def pretty: Pretty[KeyStoreError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
}

sealed trait SigningKeyGenerationError extends Product with Serializable with PrettyPrinting
object SigningKeyGenerationError {

  final case class GeneralError(error: Exception) extends SigningKeyGenerationError {
    override def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class GeneralKmsError(error: String) extends SigningKeyGenerationError {
    override def pretty: Pretty[GeneralKmsError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class NameInvalidError(error: String) extends SigningKeyGenerationError {
    override def pretty: Pretty[NameInvalidError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class FingerprintError(error: String) extends SigningKeyGenerationError {
    override def pretty: Pretty[FingerprintError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class UnsupportedKeyScheme(scheme: SigningKeyScheme)
      extends SigningKeyGenerationError {
    override def pretty: Pretty[UnsupportedKeyScheme] = prettyOfClass(param("scheme", _.scheme))
  }

  final case class SigningPrivateStoreError(error: CryptoPrivateStoreError)
      extends SigningKeyGenerationError {
    override def pretty: Pretty[SigningPrivateStoreError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class SigningPublicStoreError(error: CryptoPublicStoreError)
      extends SigningKeyGenerationError {
    override def pretty: Pretty[SigningPublicStoreError] = prettyOfClass(unnamedParam(_.error))
  }
}

sealed trait SignatureCheckError extends Product with Serializable with PrettyPrinting
object SignatureCheckError {

  final case class MultipleErrors(errors: Seq[SignatureCheckError], message: Option[String] = None)
      extends SignatureCheckError {
    override def pretty: Pretty[MultipleErrors] = prettyOfClass[MultipleErrors](
      paramIfDefined("message", _.message.map(_.unquoted)),
      param("errors", _.errors),
    )
  }

  final case class InvalidSignature(signature: Signature, bytes: ByteString, error: String)
      extends SignatureCheckError {
    override def pretty: Pretty[InvalidSignature] =
      prettyOfClass(
        param("signature", _.signature),
        param("bytes", _.bytes),
        param("error", _.error.doubleQuoted),
      )
  }
  final case class InvalidCryptoScheme(message: String) extends SignatureCheckError {
    override def pretty: Pretty[InvalidCryptoScheme] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }
  final case class InvalidKeyError(message: String) extends SignatureCheckError {
    override def pretty: Pretty[InvalidKeyError] = prettyOfClass(unnamedParam(_.message.unquoted))
  }
  final case class GeneralError(error: Exception) extends SignatureCheckError {
    override def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }
  final case class SignatureWithWrongKey(message: String) extends SignatureCheckError {
    override def pretty: Pretty[SignatureWithWrongKey] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }
  final case class SignerHasNoValidKeys(message: String) extends SignatureCheckError {
    override def pretty: Pretty[SignerHasNoValidKeys] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }
}
