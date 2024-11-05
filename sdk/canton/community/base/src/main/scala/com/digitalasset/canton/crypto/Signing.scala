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
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPrivateStoreExtended}
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

import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

/** Signing operations that do not require access to a private key store but operates with provided keys. */
trait SigningOps {

  def defaultSigningAlgorithmSpec: SigningAlgorithmSpec
  def supportedSigningAlgorithmSpecs: NonEmpty[Set[SigningAlgorithmSpec]]

  /** Signs the given hash using the private signing key. */
  def sign(
      hash: Hash,
      signingKey: SigningPrivateKey,
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  ): Either[SigningError, Signature] =
    signBytes(hash.getCryptographicEvidence, signingKey, signingAlgorithmSpec)

  /** Preferably, we sign a hash; however, we also allow signing arbitrary bytes when necessary. */
  protected[crypto] def signBytes(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
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

  def defaultSigningAlgorithmSpec: SigningAlgorithmSpec
  def defaultSigningKeySpec: SigningKeySpec

  /** Signs the given hash using the referenced private signing key. */
  def sign(
      hash: Hash,
      signingKeyId: Fingerprint,
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    signBytes(hash.getCryptographicEvidence, signingKeyId, signingAlgorithmSpec)

  /** Signs the byte string directly, however it is encouraged to sign a hash. */
  protected[crypto] def signBytes(
      bytes: ByteString,
      signingKeyId: Fingerprint,
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature]

  /** Generates a new signing key pair with the given scheme and optional name, stores the private key and returns the public key. */
  def generateSigningKey(
      keySpec: SigningKeySpec = defaultSigningKeySpec,
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

  override protected[crypto] def signBytes(
      bytes: ByteString,
      signingKeyId: Fingerprint,
      signingAlgorithmSpec: SigningAlgorithmSpec,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    store
      .signingKey(signingKeyId)
      .leftMap(storeError => SigningError.KeyStoreError(storeError.show))
      .subflatMap(_.toRight(SigningError.UnknownSigningKey(signingKeyId)))
      .subflatMap(signingKey => signingOps.signBytes(bytes, signingKey, signingAlgorithmSpec))

  /** Internal method to generate and return the entire signing key pair */
  protected[crypto] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningKeyPair]

  override def generateSigningKey(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    for {
      keypair <- generateSigningKeypair(keySpec, usage)
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
    signingAlgorithmSpec: Option[SigningAlgorithmSpec],
) extends HasVersionedWrapper[Signature]
    with PrettyPrinting
    with NoCopy {

  override protected def companionObj: Signature.type = Signature

  def toProtoV30: v30.Signature =
    v30.Signature(
      format = format.toProtoEnum,
      signature = signature,
      signedBy = signedBy.toProtoPrimitive,
      signingAlgorithmSpec = SigningAlgorithmSpec.toProtoEnumOption(signingAlgorithmSpec),
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
      None,
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
      signingAlgorithmSpec: SigningAlgorithmSpec,
  ): Signature =
    throw new UnsupportedOperationException("Use deserialization method instead")

  def fromProtoV30(signatureP: v30.Signature): ParsingResult[Signature] =
    for {
      format <- SignatureFormat.fromProtoEnum("format", signatureP.format)
      signature = signatureP.signature
      signedBy <- Fingerprint.fromProtoPrimitive(signatureP.signedBy)
      // ensures compatibility with previous signature versions where the signing algorithm specification is not set
      signingAlgorithmSpecO <- SigningAlgorithmSpec.fromProtoEnumOption(
        "signing_algorithm_spec",
        signatureP.signingAlgorithmSpec,
      )
    } yield new Signature(format, signature, signedBy, signingAlgorithmSpecO)

  def fromExternalSigning(
      format: SignatureFormat,
      signature: ByteString,
      signedBy: Fingerprint,
      signingAlgorithmSpec: SigningAlgorithmSpec,
  ): Signature =
    new Signature(format, signature, signedBy, Some(signingAlgorithmSpec))

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

  def nonEmptyIntersection(
      usage: NonEmpty[Set[SigningKeyUsage]],
      filterUsage: NonEmpty[Set[SigningKeyUsage]],
  ): Boolean =
    usage.intersect(filterUsage).nonEmpty

}

/** A signing key specification. */
sealed trait SigningKeySpec extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v30.SigningKeySpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SigningKeySpec {

  implicit val signingKeySpecOrder: Order[SigningKeySpec] =
    Order.by[SigningKeySpec, String](_.name)

  /** Elliptic Curve Key from the Curve25519 curve
    * as defined in http://ed25519.cr.yp.to/
    */
  case object EcCurve25519 extends SigningKeySpec {
    override val name: String = "EC-Curve25519"
    override def toProtoEnum: v30.SigningKeySpec =
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519
  }

  /** Elliptic Curve Key from the P-256 curve (aka Secp256r1)
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP256 extends SigningKeySpec {
    override val name: String = "EC-P256"
    override def toProtoEnum: v30.SigningKeySpec =
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256
  }

  /** Elliptic Curve Key from the P-384 curve (aka Secp384r1)
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP384 extends SigningKeySpec {
    override val name: String = "EC-P384"
    override def toProtoEnum: v30.SigningKeySpec =
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384
  }

  def fromProtoEnum(
      field: String,
      schemeP: v30.SigningKeySpec,
  ): ParsingResult[SigningKeySpec] =
    schemeP match {
      case v30.SigningKeySpec.SIGNING_KEY_SPEC_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.SigningKeySpec.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519 =>
        Right(SigningKeySpec.EcCurve25519)
      case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256 =>
        Right(SigningKeySpec.EcP256)
      case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384 =>
        Right(SigningKeySpec.EcP384)
    }

  /** If keySpec is unspecified, use the old SigningKeyScheme from the key */
  def fromProtoEnumWithDefaultScheme(
      keySpecP: v30.SigningKeySpec,
      keySchemeP: v30.SigningKeyScheme,
  ): ParsingResult[SigningKeySpec] =
    SigningKeySpec.fromProtoEnum("key_spec", keySpecP).leftFlatMap {
      case ProtoDeserializationError.FieldNotSet(_) =>
        SigningKeySpec.fromProtoEnumSigningKeyScheme("scheme", keySchemeP)
      case err => Left(err)
    }

  /** Converts an old SigningKeyScheme enum to the new key scheme,
    * ensuring backward compatibility with existing data.
    */
  def fromProtoEnumSigningKeyScheme(
      field: String,
      schemeP: v30.SigningKeyScheme,
  ): ParsingResult[SigningKeySpec] =
    schemeP match {
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.SigningKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_ED25519 =>
        Right(SigningKeySpec.EcCurve25519)
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P256 =>
        Right(SigningKeySpec.EcP256)
      case v30.SigningKeyScheme.SIGNING_KEY_SCHEME_EC_DSA_P384 =>
        Right(SigningKeySpec.EcP384)
    }
}

/** Algorithm schemes for signing. */
sealed trait SigningAlgorithmSpec extends Product with Serializable with PrettyPrinting {
  def name: String
  def supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]]
  def toProtoEnum: v30.SigningAlgorithmSpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SigningAlgorithmSpec {

  implicit val signingAlgorithmSpecOrder: Order[SigningAlgorithmSpec] =
    Order.by[SigningAlgorithmSpec, String](_.name)

  /** EdDSA signature scheme based on Curve25519 and SHA512
    * as defined in http://ed25519.cr.yp.to/
    */
  case object Ed25519 extends SigningAlgorithmSpec {
    override val name: String = "Ed25519"
    override val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
      NonEmpty.mk(Set, SigningKeySpec.EcCurve25519)
    override def toProtoEnum: v30.SigningAlgorithmSpec =
      v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519
  }

  /** Elliptic Curve Digital Signature Algorithm with SHA256
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha256 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA256"
    override val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
      NonEmpty.mk(Set, SigningKeySpec.EcP256)
    override def toProtoEnum: v30.SigningAlgorithmSpec =
      v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256
  }

  /** Elliptic Curve Digital Signature Algorithm with SHA384
    * as defined in https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha384 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA384"
    override val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
      NonEmpty.mk(Set, SigningKeySpec.EcP384)
    override def toProtoEnum: v30.SigningAlgorithmSpec =
      v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384
  }

  def toProtoEnumOption(
      signingAlgorithmSpecO: Option[SigningAlgorithmSpec]
  ): v30.SigningAlgorithmSpec =
    signingAlgorithmSpecO match {
      case None => v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_UNSPECIFIED
      case Some(signingAlgorithmSpec) => signingAlgorithmSpec.toProtoEnum
    }

  /** For backwards compatibility, this method will return None if no SigningScheme is found.
    */
  def fromProtoEnumOption(
      field: String,
      schemeP: v30.SigningAlgorithmSpec,
  ): ParsingResult[Option[SigningAlgorithmSpec]] =
    schemeP match {
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_UNSPECIFIED =>
        Right(None)
      case v30.SigningAlgorithmSpec.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519 =>
        Right(Some(SigningAlgorithmSpec.Ed25519))
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256 =>
        Right(Some(SigningAlgorithmSpec.EcDsaSha256))
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384 =>
        Right(Some(SigningAlgorithmSpec.EcDsaSha384))
    }

  def fromProtoEnum(
      field: String,
      schemeP: v30.SigningAlgorithmSpec,
  ): ParsingResult[SigningAlgorithmSpec] =
    schemeP match {
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.SigningAlgorithmSpec.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519 =>
        Right(SigningAlgorithmSpec.Ed25519)
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256 =>
        Right(SigningAlgorithmSpec.EcDsaSha256)
      case v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_384 =>
        Right(SigningAlgorithmSpec.EcDsaSha384)
    }
}

/** Required signing algorithms and keys specifications to be supported by all domain members.
  *
  * @param algorithms list of required signing algorithm specifications
  * @param keys list of required signing key specifications
  */
final case class RequiredSigningSpecs(
    algorithms: NonEmpty[Set[SigningAlgorithmSpec]],
    keys: NonEmpty[Set[SigningKeySpec]],
) extends Product
    with Serializable
    with PrettyPrinting {
  def toProtoV30: v30.RequiredSigningSpecs =
    v30.RequiredSigningSpecs(
      algorithms.forgetNE.map(_.toProtoEnum).toSeq,
      keys.forgetNE.map(_.toProtoEnum).toSeq,
    )
  override val pretty: Pretty[this.type] = prettyOfClass(
    param("algorithms", _.algorithms),
    param("keys", _.keys),
  )
}

object RequiredSigningSpecs {
  def fromProtoV30(
      requiredSigningSpecsP: v30.RequiredSigningSpecs
  ): ParsingResult[RequiredSigningSpecs] =
    for {
      keySpecs <- requiredSigningSpecsP.keys.traverse(keySpec =>
        SigningKeySpec.fromProtoEnum("keys", keySpec)
      )
      algorithmSpecs <- requiredSigningSpecsP.algorithms
        .traverse(algorithmSpec => SigningAlgorithmSpec.fromProtoEnum("algorithms", algorithmSpec))
      keySpecsNE <- NonEmpty
        .from(keySpecs.toSet)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            "keys",
            "no required signing algorithm specification",
          )
        )
      algorithmSpecsNE <- NonEmpty
        .from(algorithmSpecs.toSet)
        .toRight(
          ProtoDeserializationError.InvariantViolation(
            "algorithms",
            "no required signing key specification",
          )
        )
    } yield RequiredSigningSpecs(algorithmSpecsNE, keySpecsNE)
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
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): SigningKeyPair = {
    val publicKey = new SigningPublicKey(format, publicKeyBytes, keySpec, usage)
    val privateKey =
      new SigningPrivateKey(publicKey.id, format, privateKeyBytes, keySpec, usage)
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
    keySpec: SigningKeySpec,
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
      // we no longer use this field so we set this scheme as unspecified
      scheme = v30.SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED,
      keySpec = keySpec.toProtoEnum,
      usage = usage.map(_.toProtoEnum).toSeq,
    )

  override protected def toProtoPublicKeyKeyV30: v30.PublicKey.Key =
    v30.PublicKey.Key.SigningPublicKey(toProtoV30)

  override protected def pretty: Pretty[SigningPublicKey] =
    prettyOfClass(param("id", _.id), param("format", _.format), param("keySpec", _.keySpec))

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
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[ProtoDeserializationError.CryptoDeserializationError, SigningPublicKey] =
    new SigningPublicKey(format, key, keySpec, usage).validated

  @nowarn("cat=deprecation")
  // If we end up deserializing from a proto version that does not have any usage, set it to `All`.
  def fromProtoV30(
      publicKeyP: v30.SigningPublicKey
  ): ParsingResult[SigningPublicKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", publicKeyP.format)
      keySpec <- SigningKeySpec.fromProtoEnumWithDefaultScheme(
        publicKeyP.keySpec,
        publicKeyP.scheme,
      )
      usage <- SigningKeyUsage.fromProtoListWithDefault(publicKeyP.usage)
      signingPublicKey <- SigningPublicKey.create(
        format,
        publicKeyP.publicKey,
        keySpec,
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
    keySpec: SigningKeySpec,
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
      // we no longer use this field so we set this scheme as unspecified
      scheme = v30.SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED,
      keySpec = keySpec.toProtoEnum,
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

  @nowarn("cat=deprecation")
  def fromProtoV30(
      privateKeyP: v30.SigningPrivateKey
  ): ParsingResult[SigningPrivateKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(privateKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", privateKeyP.format)
      keySpec <- SigningKeySpec.fromProtoEnumWithDefaultScheme(
        privateKeyP.keySpec,
        privateKeyP.scheme,
      )
      usage <- SigningKeyUsage.fromProtoListWithDefault(privateKeyP.usage)
    } yield new SigningPrivateKey(id, format, privateKeyP.privateKey, keySpec, usage)

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

  final case class UnsupportedAlgorithmSpec(
      algorithmSpec: SigningAlgorithmSpec,
      supportedAlgorithmSpecs: Set[SigningAlgorithmSpec],
  ) extends SigningError {
    override def pretty: Pretty[UnsupportedAlgorithmSpec] = prettyOfClass(
      param("algorithmSpec", _.algorithmSpec),
      param("supportedAlgorithmSpecs", _.supportedAlgorithmSpecs),
    )
  }

  final case class UnsupportedKeySpec(
      signingKeySpec: SigningKeySpec,
      supportedKeySpecs: Set[SigningKeySpec],
  ) extends SigningError {
    override def pretty: Pretty[UnsupportedKeySpec] = prettyOfClass(
      param("signingKeySpec", _.signingKeySpec),
      param("supportedKeySpecs", _.supportedKeySpecs),
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

  final case class SigningPrivateStoreError(error: CryptoPrivateStoreError)
      extends SigningKeyGenerationError {
    override protected def pretty: Pretty[SigningPrivateStoreError] = prettyOfClass(
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

  final case class NoMatchingAlgorithmSpec(message: String) extends SignatureCheckError {
    override protected def pretty: Pretty[NoMatchingAlgorithmSpec] = prettyOfClass(
      unnamedParam(_.message.unquoted)
    )
  }

  final case class UnsupportedAlgorithmSpec(
      algorithmSpec: SigningAlgorithmSpec,
      supportedAlgorithmSpecs: Set[SigningAlgorithmSpec],
  ) extends SignatureCheckError {
    override def pretty: Pretty[UnsupportedAlgorithmSpec] = prettyOfClass(
      param("algorithmSpec", _.algorithmSpec),
      param("supportedAlgorithmSpecs", _.supportedAlgorithmSpecs),
    )
  }

  final case class KeyAlgoSpecsMismatch(
      signingKeySpec: SigningKeySpec,
      algorithmSpec: SigningAlgorithmSpec,
      supportedKeySpecsByAlgo: Set[SigningKeySpec],
  ) extends SignatureCheckError {
    override def pretty: Pretty[KeyAlgoSpecsMismatch] = prettyOfClass(
      param("signingKeySpec", _.signingKeySpec),
      param("algorithmSpec", _.algorithmSpec),
      param("supportedKeySpecsByAlgo", _.supportedKeySpecsByAlgo),
    )
  }

  final case class UnsupportedKeySpec(
      signingKeySpec: SigningKeySpec,
      supportedKeySpecs: Set[SigningKeySpec],
  ) extends SignatureCheckError {
    override def pretty: Pretty[UnsupportedKeySpec] = prettyOfClass(
      param("signingKeySpec", _.signingKeySpec),
      param("supportedKeySpecs", _.supportedKeySpecs),
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
