// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.data.EitherT
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
import com.digitalasset.canton.serialization.{DefaultDeserializationError, ProtoConverter}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

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
  ): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    sign(hash.getCryptographicEvidence, signingKeyId)

  /** Signs the byte string directly, however it is encouraged to sign a hash. */
  protected[crypto] def sign(
      bytes: ByteString,
      signingKeyId: Fingerprint,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature]

  /** Generates a new signing key pair with the given scheme and optional name, stores the private key and returns the public key. */
  def generateSigningKey(
      scheme: SigningKeyScheme = defaultSigningKeyScheme,
      usage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey]

}

/** A default implementation with a private key store */
trait SigningPrivateStoreOps extends SigningPrivateOps {

  implicit val ec: ExecutionContext

  protected val store: CryptoPrivateStoreExtended

  protected val signingOps: SigningOps

  override protected[crypto] def sign(
      bytes: ByteString,
      signingKeyId: Fingerprint,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    store
      .signingKey(signingKeyId)
      .leftMap(storeError => SigningError.KeyStoreError(storeError.show))
      .subflatMap(_.toRight(SigningError.UnknownSigningKey(signingKeyId)))
      .subflatMap(signingKey => signingOps.sign(bytes, signingKey))

  /** Internal method to generate and return the entire signing key pair */
  protected[crypto] def generateSigningKeypair(
      scheme: SigningKeyScheme,
      usage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningKeyPair]

  override def generateSigningKey(
      scheme: SigningKeyScheme,
      usage: NonEmpty[Set[SigningKeyUsage]],
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    for {
      keypair <- generateSigningKeypair(scheme, usage)
      _ <- store
        .storeSigningKey(keypair.privateKey, name)
        .leftMap[SigningKeyGenerationError](
          SigningKeyGenerationError.SigningPrivateStoreError.apply
        )
    } yield keypair.publicKey

}

final case class Signature private[crypto] (
    format: SignatureFormat,
    private val signature: ByteString,
    signedBy: Fingerprint,
) extends HasVersionedWrapper[Signature]
    with PrettyPrinting
    with NoCopy {

  override protected def companionObj: Signature.type = Signature

  def toProtoV30: v30.Signature =
    v30.Signature(
      format = format.toProtoEnum,
      signature = signature,
      signedBy = signedBy.toProtoPrimitive,
    )

  override protected def pretty: Pretty[Signature] =
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
  val noSignatures: NonEmpty[Set[Signature]] = NonEmpty(Set, noSignature)

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.Signature)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  override def name: String = "signature"

  private[this] def apply(
      format: SignatureFormat,
      signature: ByteString,
      signedBy: Fingerprint,
  ): Signature =
    throw new UnsupportedOperationException("Use deserialization method instead")

  def fromProtoV30(signatureP: v30.Signature): ParsingResult[Signature] =
    for {
      format <- SignatureFormat.fromProtoEnum("format", signatureP.format)
      signature = signatureP.signature
      signedBy <- Fingerprint.fromProtoPrimitive(signatureP.signedBy)
    } yield new Signature(format, signature, signedBy)
}

sealed trait SignatureFormat extends Product with Serializable {
  def toProtoEnum: v30.SignatureFormat
}

object SignatureFormat {
  case object Raw extends SignatureFormat {
    override def toProtoEnum: v30.SignatureFormat = v30.SignatureFormat.SIGNATURE_FORMAT_RAW
  }

  def fromProtoEnum(
      field: String,
      formatP: v30.SignatureFormat,
  ): ParsingResult[SignatureFormat] =
    formatP match {
      case v30.SignatureFormat.SIGNATURE_FORMAT_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.SignatureFormat.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SignatureFormat.SIGNATURE_FORMAT_RAW => Right(SignatureFormat.Raw)
    }
}

/** Only intended to be used for signing keys to distinguish keys used for generating the namespace,
  * for identity delegations, authenticate members to a sequencer and signing protocol messages.
  */
sealed trait SigningKeyUsage extends Product with Serializable with PrettyPrinting {

  // A unique identifier that is used to differentiate different key usages and that is usually embedded in a key
  // name to identify existing keys on bootstrap.
  def identifier: String

  // A unique byte representation for a key usage that is stored in the DB
  // NOTE: If you add a new dbType, add them also to `function debug.key_usage` in sql for debugging
  def dbType: Byte

  def toProtoEnum: v30.SigningKeyUsage

  override def pretty: Pretty[SigningKeyUsage.this.type] = prettyOfString(_.identifier)
}

object SigningKeyUsage {

  val All: NonEmpty[Set[SigningKeyUsage]] =
    NonEmpty.mk(Set, Namespace, IdentityDelegation, SequencerAuthentication, Protocol)

  val NamespaceOnly: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(Set, Namespace)
  val IdentityDelegationOnly: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(Set, IdentityDelegation)
  val SequencerAuthenticationOnly: NonEmpty[Set[SigningKeyUsage]] =
    NonEmpty.mk(Set, SequencerAuthentication)
  val ProtocolOnly: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(Set, Protocol)

  def fromDbTypeToSigningKeyUsage(dbTypeInt: Int): SigningKeyUsage =
    All
      .find(sku => sku.dbType == dbTypeInt.toByte)
      .getOrElse(throw new DbDeserializationException(s"Unknown key usage id: $dbTypeInt"))

  case object Namespace extends SigningKeyUsage {
    override val identifier: String = "namespace"
    override val dbType: Byte = 0
    override def toProtoEnum: v30.SigningKeyUsage = v30.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE
  }

  case object IdentityDelegation extends SigningKeyUsage {
    override val identifier: String = "identity-delegation"
    override val dbType: Byte = 1
    override def toProtoEnum: v30.SigningKeyUsage =
      v30.SigningKeyUsage.SIGNING_KEY_USAGE_IDENTITY_DELEGATION
  }

  case object SequencerAuthentication extends SigningKeyUsage {
    override val identifier: String = "sequencer-auth"
    override val dbType: Byte = 2
    override def toProtoEnum: v30.SigningKeyUsage =
      v30.SigningKeyUsage.SIGNING_KEY_USAGE_SEQUENCER_AUTHENTICATION
  }

  case object Protocol extends SigningKeyUsage {
    // In this case, the identifier does not match the class name because it needs to match the identifier tag
    // that we use to search for keys during node bootstrap.
    override val identifier: String = "signing"
    override val dbType: Byte = 3
    override def toProtoEnum: v30.SigningKeyUsage =
      v30.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL
  }

  def fromProtoEnum(
      field: String,
      usageP: v30.SigningKeyUsage,
  ): ParsingResult[SigningKeyUsage] =
    usageP match {
      case v30.SigningKeyUsage.SIGNING_KEY_USAGE_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.SigningKeyUsage.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE => Right(Namespace)
      case v30.SigningKeyUsage.SIGNING_KEY_USAGE_IDENTITY_DELEGATION =>
        Right(IdentityDelegation)
      case v30.SigningKeyUsage.SIGNING_KEY_USAGE_SEQUENCER_AUTHENTICATION =>
        Right(SequencerAuthentication)
      case v30.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL => Right(Protocol)
    }

  /** When deserializing the usages for a signing key, if the usages are empty, we default to allowing all usages to
    * maintain backward compatibility.
    */
  def fromProtoListWithDefault(
      usages: Seq[v30.SigningKeyUsage]
  ): ParsingResult[NonEmpty[Set[SigningKeyUsage]]] =
    usages
      .traverse(usageAux => SigningKeyUsage.fromProtoEnum("usage", usageAux))
      .map(listUsages => NonEmpty.from(listUsages.toSet).getOrElse(SigningKeyUsage.All))

  def fromProtoListWithoutDefault(
      usages: Seq[v30.SigningKeyUsage]
  ): ParsingResult[NonEmpty[Set[SigningKeyUsage]]] =
    usages
      .traverse(usageAux => SigningKeyUsage.fromProtoEnum("usage", usageAux))
      .flatMap(listUsages =>
        // for commands, we should not default to All; instead, the request should fail because usage is now a mandatory parameter.
        NonEmpty.from(listUsages.toSet).toRight(ProtoDeserializationError.FieldNotSet("usage"))
      )

}

sealed trait SigningKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v30.SigningKeyScheme
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
    override def toProtoEnum: v30.SigningKeyScheme = v30.SigningKeyScheme.SIGNING_KEY_SCHEME_ED25519
  }

  case object EcDsaP256 extends SigningKeyScheme {
    override def name: String = "ECDSA-P256"
    override def toProtoEnum: v30.SigningKeyScheme =
      v30.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P256
  }

  case object EcDsaP384 extends SigningKeyScheme {
    override def name: String = "ECDSA-P384"
    override def toProtoEnum: v30.SigningKeyScheme =
      v30.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P384
  }

  val EdDsaSchemes: NonEmpty[Set[SigningKeyScheme]] = NonEmpty.mk(Set, Ed25519)
  val EcDsaSchemes: NonEmpty[Set[SigningKeyScheme]] = NonEmpty.mk(Set, EcDsaP256, EcDsaP384)

  val allSchemes: NonEmpty[Set[SigningKeyScheme]] = EdDsaSchemes ++ EcDsaSchemes

  def toProtoEnumOpt(schemeO: Option[SigningKeyScheme]): v30.SigningKeyScheme =
    schemeO
      .map(_.toProtoEnum)
      .getOrElse(v30.SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED)

  def fromProtoEnum(
      field: String,
      schemeP: v30.SigningKeyScheme,
  ): ParsingResult[SigningKeyScheme] =
    schemeP match {
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.SigningKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_ED25519 => Right(SigningKeyScheme.Ed25519)
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P256 => Right(SigningKeyScheme.EcDsaP256)
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P384 => Right(SigningKeyScheme.EcDsaP384)
    }
}

final case class SigningKeyPair(publicKey: SigningPublicKey, privateKey: SigningPrivateKey)
    extends CryptoKeyPair[SigningPublicKey, SigningPrivateKey]
    with NoCopy {

  protected def toProtoV30: v30.SigningKeyPair =
    v30.SigningKeyPair(Some(publicKey.toProtoV30), Some(privateKey.toProtoV30))

  protected def toProtoCryptoKeyPairPairV30: v30.CryptoKeyPair.Pair =
    v30.CryptoKeyPair.Pair.SigningKeyPair(toProtoV30)
}

object SigningKeyPair {

  private[this] def apply(
      publicKey: SigningPublicKey,
      privateKey: SigningPrivateKey,
  ): SigningKeyPair =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  private[crypto] def create(
      format: CryptoKeyFormat,
      publicKeyBytes: ByteString,
      privateKeyBytes: ByteString,
      scheme: SigningKeyScheme,
      usage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
  ): SigningKeyPair = {
    val publicKey = new SigningPublicKey(format, publicKeyBytes, scheme, usage)
    val privateKey =
      new SigningPrivateKey(publicKey.id, format, privateKeyBytes, scheme, usage)
    new SigningKeyPair(publicKey, privateKey)
  }

  def fromProtoV30(
      signingKeyPairP: v30.SigningKeyPair
  ): ParsingResult[SigningKeyPair] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        SigningPublicKey.fromProtoV30,
        "public_key",
        signingKeyPairP.publicKey,
      )
      privateKey <- ProtoConverter.parseRequired(
        SigningPrivateKey.fromProtoV30,
        "private_key",
        signingKeyPairP.privateKey,
      )
    } yield new SigningKeyPair(publicKey, privateKey)
}

final case class SigningPublicKey private[crypto] (
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: SigningKeyScheme,
    usage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
) extends PublicKey
    with PrettyPrinting
    with HasVersionedWrapper[SigningPublicKey] {

  override val purpose: KeyPurpose = KeyPurpose.Signing

  override protected def companionObj: SigningPublicKey.type = SigningPublicKey

  // TODO(#15649): Make SigningPublicKey object invariant
  protected def validated: Either[ProtoDeserializationError.CryptoDeserializationError, this.type] =
    CryptoKeyValidation
      .parseAndValidatePublicKey(
        this,
        errMsg =>
          ProtoDeserializationError.CryptoDeserializationError(DefaultDeserializationError(errMsg)),
      )
      .map(_ => this)

  def toProtoV30: v30.SigningPublicKey =
    v30.SigningPublicKey(
      format = format.toProtoEnum,
      publicKey = key,
      scheme = scheme.toProtoEnum,
      usage = usage.map(_.toProtoEnum).toSeq,
    )

  override protected def toProtoPublicKeyKeyV30: v30.PublicKey.Key =
    v30.PublicKey.Key.SigningPublicKey(toProtoV30)

  override protected def pretty: Pretty[SigningPublicKey] =
    prettyOfClass(param("id", _.id), param("format", _.format), param("scheme", _.scheme))

}

object SigningPublicKey
    extends HasVersionedMessageCompanion[SigningPublicKey]
    with HasVersionedMessageCompanionDbHelpers[SigningPublicKey] {
  override def name: String = "signing public key"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.SigningPublicKey)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private[crypto] def create(
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: SigningKeyScheme,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[ProtoDeserializationError.CryptoDeserializationError, SigningPublicKey] =
    new SigningPublicKey(format, key, scheme, usage).validated

  /** If we end up deserializing from a proto version that does not have any usage, set it to `All`.
    */
  def fromProtoV30(
      publicKeyP: v30.SigningPublicKey
  ): ParsingResult[SigningPublicKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", publicKeyP.format)
      scheme <- SigningKeyScheme.fromProtoEnum("scheme", publicKeyP.scheme)
      usage <- SigningKeyUsage.fromProtoListWithDefault(publicKeyP.usage)
      signingPublicKey <- SigningPublicKey.create(
        format,
        publicKeyP.publicKey,
        scheme,
        usage,
      )
    } yield signingPublicKey

  def collect(initialKeys: Map[Member, Seq[PublicKey]]): Map[Member, Seq[SigningPublicKey]] =
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

  override val id: Fingerprint = publicKey.id

  override protected def pretty: Pretty[SigningPublicKeyWithName] =
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
    usage: NonEmpty[Set[SigningKeyUsage]],
) extends PrivateKey
    with HasVersionedWrapper[SigningPrivateKey]
    with NoCopy {

  override protected def companionObj: SigningPrivateKey.type = SigningPrivateKey

  def toProtoV30: v30.SigningPrivateKey =
    v30.SigningPrivateKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      privateKey = key,
      scheme = scheme.toProtoEnum,
      usage = usage.map(_.toProtoEnum).toSeq,
    )

  override def purpose: KeyPurpose = KeyPurpose.Signing

  override protected def toProtoPrivateKeyKeyV30: v30.PrivateKey.Key =
    v30.PrivateKey.Key.SigningPrivateKey(toProtoV30)
}

object SigningPrivateKey extends HasVersionedMessageCompanion[SigningPrivateKey] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v32,
      supportedProtoVersion(v30.SigningPrivateKey)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  override def name: String = "signing private key"

  def fromProtoV30(
      privateKeyP: v30.SigningPrivateKey
  ): ParsingResult[SigningPrivateKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(privateKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", privateKeyP.format)
      scheme <- SigningKeyScheme.fromProtoEnum("scheme", privateKeyP.scheme)
      usage <- SigningKeyUsage.fromProtoListWithDefault(privateKeyP.usage)
    } yield new SigningPrivateKey(id, format, privateKeyP.privateKey, scheme, usage)

}

sealed trait SigningError extends Product with Serializable with PrettyPrinting
object SigningError {

  final case class GeneralError(error: Exception) extends SigningError {
    override protected def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class InvariantViolation(error: String) extends SigningError {
    override protected def pretty: Pretty[InvariantViolation] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class InvalidSigningKey(error: String) extends SigningError {
    override protected def pretty: Pretty[InvalidSigningKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class UnknownSigningKey(keyId: Fingerprint) extends SigningError {
    override protected def pretty: Pretty[UnknownSigningKey] = prettyOfClass(
      param("keyId", _.keyId)
    )
  }

  final case class FailedToSign(error: String) extends SigningError {
    override protected def pretty: Pretty[FailedToSign] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class KeyStoreError(error: String) extends SigningError {
    override protected def pretty: Pretty[KeyStoreError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}

sealed trait SigningKeyGenerationError extends Product with Serializable with PrettyPrinting
object SigningKeyGenerationError extends CantonErrorGroups.CommandErrorGroup {

  @Explanation("This error indicates that a signing key could not be created.")
  @Resolution("Inspect the error details")
  object ErrorCode
      extends ErrorCode(
        id = "SIGNING_KEY_GENERATION_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Wrap(reason: SigningKeyGenerationError)
        extends BaseCantonError.Impl(cause = "Unable to create signing key")
  }

  final case class GeneralError(error: Exception) extends SigningKeyGenerationError {
    override protected def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class GeneralKmsError(error: String) extends SigningKeyGenerationError {
    override protected def pretty: Pretty[GeneralKmsError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class NameInvalidError(error: String) extends SigningKeyGenerationError {
    override protected def pretty: Pretty[NameInvalidError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class FingerprintError(error: String) extends SigningKeyGenerationError {
    override protected def pretty: Pretty[FingerprintError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class UnsupportedKeyScheme(scheme: SigningKeyScheme)
      extends SigningKeyGenerationError {
    override protected def pretty: Pretty[UnsupportedKeyScheme] = prettyOfClass(
      param("scheme", _.scheme)
    )
  }

  final case class SigningPrivateStoreError(error: CryptoPrivateStoreError)
      extends SigningKeyGenerationError {
    override protected def pretty: Pretty[SigningPrivateStoreError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

  final case class SigningPublicStoreError(error: CryptoPublicStoreError)
      extends SigningKeyGenerationError {
    override protected def pretty: Pretty[SigningPublicStoreError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }
}

sealed trait SignatureCheckError extends Product with Serializable with PrettyPrinting
object SignatureCheckError {

  final case class MultipleErrors(errors: Seq[SignatureCheckError], message: Option[String] = None)
      extends SignatureCheckError {
    override protected def pretty: Pretty[MultipleErrors] = prettyOfClass[MultipleErrors](
      paramIfDefined("message", _.message.map(_.unquoted)),
      param("errors", _.errors),
    )
  }

  final case class InvalidSignature(signature: Signature, bytes: ByteString, error: String)
      extends SignatureCheckError {
    override protected def pretty: Pretty[InvalidSignature] =
      prettyOfClass(
        param("signature", _.signature),
        param("bytes", _.bytes),
        param("error", _.error.doubleQuoted),
      )
  }

  final case class InvalidCryptoScheme(message: String) extends SignatureCheckError {
    override protected def pretty: Pretty[InvalidCryptoScheme] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class InvalidKeyError(message: String) extends SignatureCheckError {
    override protected def pretty: Pretty[InvalidKeyError] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class MemberGroupDoesNotExist(message: String) extends SignatureCheckError {
    override protected def pretty: Pretty[MemberGroupDoesNotExist] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class GeneralError(error: Exception) extends SignatureCheckError {
    override protected def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  final case class SignatureWithWrongKey(message: String) extends SignatureCheckError {
    override protected def pretty: Pretty[SignatureWithWrongKey] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class SignerHasNoValidKeys(message: String) extends SignatureCheckError {
    override protected def pretty: Pretty[SignerHasNoValidKeys] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

}
