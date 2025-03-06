// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{CantonConfigValidator, UniformCantonConfigValidation}
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.SigningPublicKey.getDataForFingerprint
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPrivateStoreExtended}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.{CantonBaseError, CantonErrorGroups}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DefaultDeserializationError, ProtoConverter}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionDbHelpers,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.bouncycastle.asn1.{ASN1OctetString, DEROctetString}
import slick.jdbc.GetResult

import java.time.Duration
import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

/** Signing operations that do not require access to a private key store but operates with provided
  * keys.
  */
trait SigningOps {

  def defaultSigningAlgorithmSpec: SigningAlgorithmSpec
  def supportedSigningAlgorithmSpecs: NonEmpty[Set[SigningAlgorithmSpec]]

  /** Signs the given hash using the private signing key.
    *
    * @param usage
    *   the usage we intend to enforce. If multiple usages are enforced, at least one of them must
    *   be satisfied. In other words, the provided signing key's usage must intersect with the
    *   specified usages.
    */
  def sign(
      hash: Hash,
      signingKey: SigningPrivateKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  )(implicit traceContext: TraceContext): Either[SigningError, Signature] =
    signBytes(hash.getCryptographicEvidence, signingKey, usage, signingAlgorithmSpec)

  /** Preferably, we sign a hash; however, we also allow signing arbitrary bytes when necessary. */
  protected[crypto] def signBytes(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  )(implicit traceContext: TraceContext): Either[SigningError, Signature]

  /** Confirms if the provided signature is a valid signature of the payload using the public key */
  def verifySignature(
      hash: Hash,
      publicKey: SigningPublicKey,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, Unit] =
    verifySignature(hash.getCryptographicEvidence, publicKey, signature, usage)

  protected[crypto] def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, Unit]
}

/** Signing operations that require access to stored private keys. */
trait SigningPrivateOps {

  def defaultSigningAlgorithmSpec: SigningAlgorithmSpec
  def defaultSigningKeySpec: SigningKeySpec

  /** Signs the given hash using the referenced private signing key. */
  def sign(
      hash: Hash,
      signingKeyId: Fingerprint,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    signBytes(hash.getCryptographicEvidence, signingKeyId, usage, signingAlgorithmSpec)

  /** Signs the byte string directly, however it is encouraged to sign a hash. */
  def signBytes(
      bytes: ByteString,
      signingKeyId: Fingerprint,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature]

  /** Generates a new signing key pair with the given scheme and optional name, stores the private
    * key and returns the public key.
    */
  def generateSigningKey(
      keySpec: SigningKeySpec = defaultSigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
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

  override def signBytes(
      bytes: ByteString,
      signingKeyId: Fingerprint,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    store
      .signingKey(signingKeyId)
      .leftMap(storeError => SigningError.KeyStoreError(storeError.show))
      .subflatMap(_.toRight(SigningError.UnknownSigningKey(signingKeyId)))
      .subflatMap(signingKey =>
        signingOps.signBytes(bytes, signingKey, usage, signingAlgorithmSpec)
      )

  /** Internal method to generate and return the entire signing key pair */
  protected[crypto] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
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

final case class Signature private (
    format: SignatureFormat,
    private val signature: ByteString,
    signedBy: Fingerprint,
    signingAlgorithmSpec: Option[SigningAlgorithmSpec],
    signatureDelegation: Option[SignatureDelegation],
) extends HasVersionedWrapper[Signature]
    with PrettyPrinting {

  override protected def companionObj: Signature.type = Signature

  def toProtoV30: v30.Signature =
    v30.Signature(
      format = format.toProtoEnum,
      signature = signature,
      signedBy = signedBy.toProtoPrimitive,
      signingAlgorithmSpec = SigningAlgorithmSpec.toProtoEnumOption(signingAlgorithmSpec),
      signatureDelegation = signatureDelegation.map(_.toProtoV30),
    )

  @nowarn("msg=Raw in object SignatureFormat is deprecated")
  private[crypto] def migrate(): Option[Signature] =
    Option.when(format == SignatureFormat.Raw) {
      val newFormat = signingAlgorithmSpec match {
        case Some(algo) =>
          algo match {
            case SigningAlgorithmSpec.EcDsaSha256 | SigningAlgorithmSpec.EcDsaSha384 =>
              SignatureFormat.Der
            case SigningAlgorithmSpec.Ed25519 => SignatureFormat.Concat
          }

        case None =>
          // We don't have the signing algo spec. This is a backwards-compatibility case, which should happen
          // mostly for pre-existing topology transaction signatures.
          //
          // We try to look at the signature to determine its format:
          //  * EdDSA `Concat` signatures are always 64 bytes
          //  * ECDSA `Der` signatures are encoded as an ASN.1 SEQUENCE of two INTEGER's. This contains 6 bytes
          //     for type tags and lengths, and two integers in the range [1..n-1] (with n related to the key size),
          //     encoded without padding.
          //
          // The absence of padding in the ASN.1 encoding makes the size of Der signatures variable, and theoretically
          // possible to also be 64 bytes. For this to happen though, the two integers must be just the right size
          // (small enough but not too small) to exactly compensate for the 6 extra bytes. A quick empirical test
          // generating ~50'000 ECDSA-SHA256 signatures encountered most of the time sizes between 70 and 72 bytes, and
          // very rarely 69 bytes (< 0.3% of the cases), with nothing lower.
          //
          // We consider the chances of incorrectly guessing a Der signature as Concat small enough that they can
          // be ignored for our use case (famous last words...).

          if (signature.size == 64) SignatureFormat.Concat else SignatureFormat.Der
      }

      Signature(newFormat, signature, signedBy, signingAlgorithmSpec, signatureDelegation)
    }

  @VisibleForTesting
  @nowarn("msg=Raw in object SignatureFormat is deprecated")
  // Inverse operation from migrate(): used in tests to produce legacy signatures.
  private[crypto] def reverseMigrate(): Signature = copy(format = format match {
    case SignatureFormat.Der | SignatureFormat.Concat => SignatureFormat.Raw
    case SignatureFormat.Symbolic => format
    case SignatureFormat.Raw => throw new IllegalStateException("Original signature has Raw format")
  })

  override protected def pretty: Pretty[Signature] =
    prettyOfClass(param("signature", _.signature), param("signedBy", _.signedBy))

  /** Access to the raw signature, must NOT be used for serialization */
  private[crypto] def unwrap: ByteString = signature
}

object Signature
    extends HasVersionedMessageCompanion[Signature]
    with HasVersionedMessageCompanionDbHelpers[Signature] {
  val noSignature =
    Signature.create(
      SignatureFormat.Symbolic,
      ByteString.EMPTY,
      Fingerprint.tryFromString("no-fingerprint"),
      None,
    )
  val noSignatures: NonEmpty[Set[Signature]] = NonEmpty(Set, noSignature)

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(v30.Signature)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "signature"

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
      signatureDelegationO <- signatureP.signatureDelegation.traverse(
        SignatureDelegation.fromProtoV30
      )
    } yield Signature.create(
      format,
      signature,
      signedBy,
      signingAlgorithmSpecO,
      signatureDelegationO,
    )

  def create(
      format: SignatureFormat,
      signature: ByteString,
      signedBy: Fingerprint,
      signingAlgorithmSpec: Option[SigningAlgorithmSpec],
      signatureDelegation: Option[SignatureDelegation] = None,
  ): Signature = {
    val signatureBeforeMigration =
      Signature(format, signature, signedBy, signingAlgorithmSpec, signatureDelegation)
    val signatureAfterMigration =
      signatureBeforeMigration.migrate().getOrElse(signatureBeforeMigration)

    signatureAfterMigration
  }

  def fromExternalSigning(
      format: SignatureFormat,
      signature: ByteString,
      signedBy: Fingerprint,
      signingAlgorithmSpec: SigningAlgorithmSpec,
  ): Signature =
    create(format, signature, signedBy, Some(signingAlgorithmSpec))

}

/** Defines the validity period of a session signing key delegation within a specific synchronizer
  * timeframe. This period starts at a creation 'from' timestamp and extends for a specified
  * duration, covering both the initial and end times inclusively.
  *
  * @param fromInclusive
  *   the inclusive timestamp, indicating when a delegation to the session key was created
  * @param periodLength
  *   the inclusive validity duration of the session key delegation in seconds
  */
final case class SignatureDelegationValidityPeriod(
    fromInclusive: CantonTimestamp,
    periodLength: PositiveSeconds,
) extends PrettyPrinting {
  val toInclusive: CantonTimestamp = fromInclusive + periodLength

  override protected def pretty: Pretty[SignatureDelegationValidityPeriod] =
    prettyOfClass(
      param("fromInclusive", _.fromInclusive),
      param("toInclusive", _.toInclusive),
    )
}

/** An extension to the signature to accommodate the necessary information to be able to use session
  * signing keys for protocol messages.
  *
  * @param sessionKey
  *   the session signing key that can be used to verify the protocol message, must be in
  *   DerX509Spki format
  * @param validityPeriod
  *   indicates the 'lifespan' (i.e. how long the key is valid) of a session signing key
  * @param signature
  *   this signature authorizes the session key to act on behalf of a long-term key We sign over the
  *   combined hash of the fingerprint of the session key, the validity period, and the synchronizer
  *   id.
  */
final case class SignatureDelegation private[crypto] (
    sessionKey: SigningPublicKey,
    validityPeriod: SignatureDelegationValidityPeriod,
    signature: Signature,
) extends Product
    with Serializable {

  // All session signing keys must be an ASN.1 + DER-encoding of X.509 SubjectPublicKeyInfo structure and be
  // set to be used for protocol messages
  require(
    sessionKey.format == CryptoKeyFormat.DerX509Spki &&
      SigningKeyUsage.matchesRelevantUsages(sessionKey.usage, SigningKeyUsage.ProtocolOnly) &&
      signature.signatureDelegation.isEmpty // we don't support recursive delegations
  )

  def toProtoV30: v30.SignatureDelegation =
    v30.SignatureDelegation(
      sessionKey = sessionKey.key,
      sessionKeySpec = sessionKey.keySpec.toProtoEnum,
      validityPeriodFromInclusive = validityPeriod.fromInclusive.toProtoPrimitive,
      validityPeriodDurationInclusive = validityPeriod.periodLength.duration.toSeconds.toInt,
      format = signature.format.toProtoEnum,
      // In this case, we send the raw content of the signature because the remaining parameters for deserialization are
      // already included in the v30.SignatureDelegation message (e.g. format).
      signature = signature.toProtoV30.signature,
      signingAlgorithmSpec = SigningAlgorithmSpec.toProtoEnumOption(signature.signingAlgorithmSpec),
    )
}

object SignatureDelegation {

  // TODO(#22362): https://github.com/DACH-NY/canton/pull/22185#discussion_r1846626744
  def create(
      sessionKey: SigningPublicKey,
      validityPeriod: SignatureDelegationValidityPeriod,
      signature: Signature,
  ): Either[String, SignatureDelegation] =
    for {
      _ <-
        Either.cond(
          sessionKey.format == CryptoKeyFormat.DerX509Spki,
          (),
          s"session key must be in ${CryptoKeyFormat.DerX509Spki.name} format (${sessionKey.format})",
        )
      _ <-
        Either.cond(
          sessionKey.usage == SigningKeyUsage.ProtocolWithProofOfOwnership,
          (),
          s"session key must only be used for protocol messages (${sessionKey.usage})",
        )
      _ <-
        Either.cond(
          signature.signatureDelegation.isEmpty,
          (),
          s"a signature delegation cannot itself be delegated",
        )
    } yield SignatureDelegation(
      sessionKey = sessionKey,
      validityPeriod = validityPeriod,
      signature = signature,
    )

  def fromProtoV30(signatureP: v30.SignatureDelegation): ParsingResult[SignatureDelegation] =
    for {
      scheme <- SigningKeySpec.fromProtoEnum("session_key_spec", signatureP.sessionKeySpec)
      sessionKey <- SigningPublicKey.create(
        CryptoKeyFormat.DerX509Spki,
        signatureP.sessionKey,
        scheme,
        SigningKeyUsage.ProtocolOnly,
      )
      fromInclusive <-
        CantonTimestamp.fromProtoPrimitive(
          signatureP.validityPeriodFromInclusive
        )
      validityPeriodDurationInclusive <-
        ProtoConverter.parsePositiveInt(
          "validity_period_duration_inclusive",
          signatureP.validityPeriodDurationInclusive,
        )
      // Duration is already validated as positive and non-zero during parsing,
      // so calling Positive.create method here is unnecessary.
      periodLength = PositiveSeconds.tryCreate(
        Duration.ofSeconds(validityPeriodDurationInclusive.value.toLong)
      )
      signatureRaw = signatureP.signature
      signatureFormat <- SignatureFormat.fromProtoEnum("format", signatureP.format)
      signatureAlgorithmSpecO <- SigningAlgorithmSpec.fromProtoEnumOption(
        "signing_algorithm_spec",
        signatureP.signingAlgorithmSpec,
      )
      signature = Signature.create(
        signatureFormat,
        signatureRaw,
        sessionKey.fingerprint,
        signatureAlgorithmSpecO,
      )
      signatureDelegation <-
        SignatureDelegation
          .create(
            sessionKey,
            SignatureDelegationValidityPeriod(
              fromInclusive,
              periodLength,
            ),
            signature,
          )
          .leftMap(errMsg =>
            ProtoDeserializationError.InvariantViolation(
              None,
              "Failed to create signature " +
                s"delegation from deserialized content with: $errMsg",
            )
          )
    } yield signatureDelegation
}

sealed trait SignatureFormat extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v30.SignatureFormat
  override protected def pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SignatureFormat {

  /** ASN.1 + DER-encoding of the `r` and `s` integers, as defined in
    * https://datatracker.ietf.org/doc/html/rfc3279#section-2.2.3
    *
    * Used for ECDSA signatures.
    */
  case object Der extends SignatureFormat {
    override val name: String = "DER"
    override def toProtoEnum: v30.SignatureFormat = v30.SignatureFormat.SIGNATURE_FORMAT_DER
  }

  /** Concatenation of the `r` and `s` integers in little-endian form, as defined in
    * https://datatracker.ietf.org/doc/html/rfc8032#section-3.3
    *
    * Note that this is different from the format defined in IEEE P1363, which uses concatenation in
    * big-endian form.
    *
    * Used for EdDSA signatures.
    */
  case object Concat extends SignatureFormat {
    override val name: String = "Concat"
    override def toProtoEnum: v30.SignatureFormat =
      v30.SignatureFormat.SIGNATURE_FORMAT_CONCAT
  }

  /** Signature scheme specific signature format.
    *
    * Legacy format no longer used, except for migrations.
    */
  @deprecated(
    message = "Use the more specific `Der` or `Concat` formats instead.",
    since = "3.3",
  )
  case object Raw extends SignatureFormat {
    override val name: String = "Raw"
    override def toProtoEnum: v30.SignatureFormat = v30.SignatureFormat.SIGNATURE_FORMAT_RAW
  }

  /** Signature format used for tests.
    */
  case object Symbolic extends SignatureFormat {
    override val name: String = "Symbolic"
    override def toProtoEnum: v30.SignatureFormat = v30.SignatureFormat.SIGNATURE_FORMAT_SYMBOLIC
  }

  def fromSigningAlgoSpec(signingAlgoSpec: SigningAlgorithmSpec): SignatureFormat =
    signingAlgoSpec match {
      case SigningAlgorithmSpec.EcDsaSha256 | SigningAlgorithmSpec.EcDsaSha384 =>
        SignatureFormat.Der
      case SigningAlgorithmSpec.Ed25519 => SignatureFormat.Concat
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
      case v30.SignatureFormat.SIGNATURE_FORMAT_DER => Right(SignatureFormat.Der)
      case v30.SignatureFormat.SIGNATURE_FORMAT_CONCAT => Right(SignatureFormat.Concat)
      case v30.SignatureFormat.SIGNATURE_FORMAT_RAW =>
        Right(SignatureFormat.Raw: @nowarn("msg=Raw in object SignatureFormat is deprecated"))
      case v30.SignatureFormat.SIGNATURE_FORMAT_SYMBOLIC => Right(SignatureFormat.Symbolic)
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
    NonEmpty.mk(
      Set,
      Namespace,
      IdentityDelegation,
      SequencerAuthentication,
      Protocol,
      ProofOfOwnership,
    )

  val NamespaceOnly: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(Set, Namespace)
  val IdentityDelegationOnly: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(Set, IdentityDelegation)
  val NamespaceOrIdentityDelegation: NonEmpty[Set[SigningKeyUsage]] =
    NonEmpty.mk(Set, Namespace, IdentityDelegation)
  val NamespaceOrProofOfOwnership: NonEmpty[Set[SigningKeyUsage]] =
    NonEmpty.mk(Set, Namespace, ProofOfOwnership)
  val SequencerAuthenticationOnly: NonEmpty[Set[SigningKeyUsage]] =
    NonEmpty.mk(Set, SequencerAuthentication)
  val ProtocolOnly: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(Set, Protocol)
  val ProofOfOwnershipOnly: NonEmpty[Set[SigningKeyUsage]] = NonEmpty.mk(Set, ProofOfOwnership)
  val ProtocolWithProofOfOwnership: NonEmpty[Set[SigningKeyUsage]] =
    NonEmpty.mk(Set, Protocol, ProofOfOwnership)

  /** The following combinations are invalid because:
    *   - `ProofOfOwnership` is an internal type and must always be associated with another usage.
    *     It identifies that a key can be used to prove ownership within the context of
    *     `OwnerToKeyMappings` and `PartyToKeyMappings` topology transactions.
    *   - Keys associated with `Namespace` and `IdentityDelegation` are not part of
    *     `OwnerToKeyMappings` or `PartyToKeyMappings`, and therefore are not used to prove
    *     ownership.
    */
  private val invalidUsageCombinations: Set[NonEmpty[Set[SigningKeyUsage]]] =
    Set(
      ProofOfOwnershipOnly,
      NamespaceOnly ++ ProofOfOwnershipOnly,
      IdentityDelegationOnly ++ ProofOfOwnershipOnly,
      NamespaceOnly ++ IdentityDelegationOnly ++ ProofOfOwnershipOnly,
    )

  def isUsageValid(usage: NonEmpty[Set[SigningKeyUsage]]): Boolean =
    !SigningKeyUsage.invalidUsageCombinations.contains(usage)

  def fromDbTypeToSigningKeyUsage(dbTypeInt: Int): SigningKeyUsage =
    All
      .find(sku => sku.dbType == dbTypeInt.toByte)
      .getOrElse(throw new DbDeserializationException(s"Unknown key usage id: $dbTypeInt"))

  def fromIdentifier(identifier: String): Option[SigningKeyUsage] =
    All.find(_.identifier == identifier)

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

  /** Internal type used to identify keys that can self-sign to prove ownership, required for
    * topology requests such as OwnerToKeyMappings and PartyToKeyMappings. Generally, any key not
    * intended for namespace or identity delegation will have this usage automatically assigned.
    */
  private case object ProofOfOwnership extends SigningKeyUsage {
    override val identifier: String = "proof-of-ownership"
    override val dbType: Byte = 4
    override def toProtoEnum: v30.SigningKeyUsage =
      v30.SigningKeyUsage.SIGNING_KEY_USAGE_PROOF_OF_OWNERSHIP

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
      case v30.SigningKeyUsage.SIGNING_KEY_USAGE_PROOF_OF_OWNERSHIP => Right(ProofOfOwnership)
    }

  /** When deserializing the usages for a signing key, if the usages are empty, we default to
    * allowing all usages to maintain backward compatibility.
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

  /** Ensures that the intersection of a `key's usages` and the `allowed usages` is not empty,
    * guaranteeing that at least one usage is shared.
    *
    * This function should only be used in the context of signing and verification, as it treats
    * `ProofOfOwnership` like any other usage and attempts to match it.
    *
    * The only case where `ProofOfOwnership` can be part of the allowed usages is when signing or
    * verifying topology mappings such as `OwnerToKeyMappings` or `PartyToKeyMappings`, where the
    * keys must self-sign the request.
    */
  def compatibleUsageForSignAndVerify(
      keyUsage: NonEmpty[Set[SigningKeyUsage]],
      allowedUsages: NonEmpty[Set[SigningKeyUsage]],
  ): Boolean =
    keyUsage.intersect(allowedUsages).nonEmpty

  /** Verifies that there is at least one common usage between a `key's usages` and the `allowed
    * usages`, considering only relevant usages and excluding the internal `ProofOfOwnership` type.
    * `ProofOfOwnership` should only be considered when signing or verifying. This method must be
    * used for all other scenarios, such as filtering or finding keys.
    */
  def matchesRelevantUsages(
      keyUsage: NonEmpty[Set[SigningKeyUsage]],
      allowedUsages: NonEmpty[Set[SigningKeyUsage]],
  ): Boolean = {
    val relevantUsages = allowedUsages - ProofOfOwnership
    keyUsage.intersect(relevantUsages).nonEmpty
  }

  /** Adds the `ProofOfOwnershipOnly` usage to the list of usages, unless it forms an invalid
    * combination.
    */
  def addProofOfOwnership(usage: NonEmpty[Set[SigningKeyUsage]]): NonEmpty[Set[SigningKeyUsage]] = {
    val newUsage = usage ++ ProofOfOwnershipOnly
    if (SigningKeyUsage.invalidUsageCombinations.contains(newUsage)) usage
    else newUsage
  }

}

/** A signing key specification. */
sealed trait SigningKeySpec
    extends Product
    with Serializable
    with PrettyPrinting
    with UniformCantonConfigValidation {
  def name: String
  def toProtoEnum: v30.SigningKeySpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SigningKeySpec {

  implicit val signingKeySpecOrder: Order[SigningKeySpec] =
    Order.by[SigningKeySpec, String](_.name)

  implicit val signingKeySpecCantonConfigValidation: CantonConfigValidator[SigningKeySpec] =
    CantonConfigValidatorDerivation[SigningKeySpec]

  /** Elliptic Curve Key from the Curve25519 curve as defined in http://ed25519.cr.yp.to/
    */
  case object EcCurve25519 extends SigningKeySpec {
    override val name: String = "EC-Curve25519"
    override def toProtoEnum: v30.SigningKeySpec =
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519
  }

  /** Elliptic Curve Key from the P-256 curve (aka secp256r1) as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP256 extends SigningKeySpec {
    override val name: String = "EC-P256"
    override def toProtoEnum: v30.SigningKeySpec =
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P256
  }

  /** Elliptic Curve Key from the P-384 curve (aka secp384r1) as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcP384 extends SigningKeySpec {
    override val name: String = "EC-P384"
    override def toProtoEnum: v30.SigningKeySpec =
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_P384
  }

  /** Elliptic Curve Key from SECG P256k1 curve (aka secp256k1) commonly used in bitcoin and
    * ethereum as defined in https://www.secg.org/sec2-v2.pdf
    */
  case object EcSecp256k1 extends SigningKeySpec {
    override val name: String = "EC-Secp256k1"
    override def toProtoEnum: v30.SigningKeySpec =
      v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_SECP256K1
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
      case v30.SigningKeySpec.SIGNING_KEY_SPEC_EC_SECP256K1 =>
        Right(SigningKeySpec.EcSecp256k1)
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

  /** Converts an old SigningKeyScheme enum to the new key scheme, ensuring backward compatibility
    * with existing data.
    */
  private def fromProtoEnumSigningKeyScheme(
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
sealed trait SigningAlgorithmSpec
    extends Product
    with Serializable
    with PrettyPrinting
    with UniformCantonConfigValidation {
  def name: String
  def supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]]
  def supportedSignatureFormats: NonEmpty[Set[SignatureFormat]]
  def toProtoEnum: v30.SigningAlgorithmSpec
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SigningAlgorithmSpec {

  implicit val signingAlgorithmSpecOrder: Order[SigningAlgorithmSpec] =
    Order.by[SigningAlgorithmSpec, String](_.name)

  implicit val signingAlgorithmSpecCantonConfigValidator
      : CantonConfigValidator[SigningAlgorithmSpec] =
    CantonConfigValidatorDerivation[SigningAlgorithmSpec]

  /** EdDSA signature scheme based on Curve25519 and SHA512 as defined in http://ed25519.cr.yp.to/
    */
  case object Ed25519 extends SigningAlgorithmSpec {
    override val name: String = "Ed25519"
    override val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
      NonEmpty.mk(Set, SigningKeySpec.EcCurve25519)
    override val supportedSignatureFormats: NonEmpty[Set[SignatureFormat]] =
      NonEmpty.mk(Set, SignatureFormat.Concat)
    override def toProtoEnum: v30.SigningAlgorithmSpec =
      v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519
  }

  /** Elliptic Curve Digital Signature Algorithm with SHA256 as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha256 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA256"
    override val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
      NonEmpty.mk(Set, SigningKeySpec.EcP256, SigningKeySpec.EcSecp256k1)
    override val supportedSignatureFormats: NonEmpty[Set[SignatureFormat]] =
      NonEmpty.mk(Set, SignatureFormat.Der)
    override def toProtoEnum: v30.SigningAlgorithmSpec =
      v30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_EC_DSA_SHA_256
  }

  /** Elliptic Curve Digital Signature Algorithm with SHA384 as defined in
    * https://doi.org/10.6028/NIST.FIPS.186-4
    */
  case object EcDsaSha384 extends SigningAlgorithmSpec {
    override val name: String = "EC-DSA-SHA384"
    override val supportedSigningKeySpecs: NonEmpty[Set[SigningKeySpec]] =
      NonEmpty.mk(Set, SigningKeySpec.EcP384)
    override val supportedSignatureFormats: NonEmpty[Set[SignatureFormat]] =
      NonEmpty.mk(Set, SignatureFormat.Der)
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

/** Required signing algorithms and keys specifications to be supported by all synchronizer members.
  *
  * @param algorithms
  *   list of required signing algorithm specifications
  * @param keys
  *   list of required signing key specifications
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
    extends CryptoKeyPair[SigningPublicKey, SigningPrivateKey] {

  require(
    publicKey.usage == privateKey.usage,
    "Public and private key must have the same key usage",
  )

  @VisibleForTesting
  def replaceUsage(usage: NonEmpty[Set[SigningKeyUsage]]): SigningKeyPair =
    this.copy(
      publicKey = publicKey.replaceUsage(usage),
      privateKey = privateKey.replaceUsage(usage),
    )

  protected def toProtoV30: v30.SigningKeyPair =
    v30.SigningKeyPair(Some(publicKey.toProtoV30), Some(privateKey.toProtoV30))

  protected def toProtoCryptoKeyPairPairV30: v30.CryptoKeyPair.Pair =
    v30.CryptoKeyPair.Pair.SigningKeyPair(toProtoV30)
}

object SigningKeyPair {

  private[crypto] def create(
      publicFormat: CryptoKeyFormat,
      publicKeyBytes: ByteString,
      privateFormat: CryptoKeyFormat,
      privateKeyBytes: ByteString,
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[SigningKeyGenerationError, SigningKeyPair] = {
    val usageUpdate = SigningKeyUsage.addProofOfOwnership(usage)
    for {
      publicKey <- SigningPublicKey
        .create(publicFormat, publicKeyBytes, keySpec, usageUpdate)
        .leftMap[SigningKeyGenerationError](err =>
          SigningKeyGenerationError.GeneralError(
            new IllegalStateException(s"Failed to create public signing key: $err")
          )
        )
      privateKey <- SigningPrivateKey
        .create(publicKey.id, privateFormat, privateKeyBytes, keySpec, usageUpdate)
        .leftMap[SigningKeyGenerationError](err =>
          SigningKeyGenerationError.GeneralError(
            new IllegalStateException(s"Failed to create private signing key: $err")
          )
        )
    } yield SigningKeyPair(publicKey, privateKey)
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
    } yield SigningKeyPair(publicKey, privateKey)
}

final case class SigningPublicKey private[crypto] (
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    keySpec: SigningKeySpec,
    usage: NonEmpty[Set[SigningKeyUsage]],
    override protected val dataForFingerprintO: Option[ByteString] = None,
)(
    override val migrated: Boolean = false
) extends PublicKey
    with PrettyPrinting
    with HasVersionedWrapper[SigningPublicKey] {

  override type K = SigningPublicKey

  require(
    SigningKeyUsage.isUsageValid(usage),
    s"Invalid usage $usage",
  )

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
    prettyOfClass(
      param("id", _.id),
      param("format", _.format),
      param("keySpec", _.keySpec),
      param("usage", _.usage),
    )

  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  private def migrate(): Either[KeyParseAndValidateError, Option[SigningPublicKey]] = {
    def mkNewKeyO(
        newKey: ByteString
    ): Either[KeyParseAndValidateError, Option[SigningPublicKey]] = {
      val newFormat = CryptoKeyFormat.DerX509Spki
      getDataForFingerprint(keySpec, newFormat, newKey).map(dataForFpO =>
        Some(SigningPublicKey(newFormat, newKey, keySpec, usage, dataForFpO)(migrated = true))
      )
    }

    (keySpec, format) match {
      case (SigningKeySpec.EcCurve25519, CryptoKeyFormat.Raw) =>
        // The key is stored as pure bytes; we need to convert it to a SubjectPublicKeyInfo structure
        val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
        val subjectPublicKeyInfo =
          new SubjectPublicKeyInfo(algoId, key.toByteArray).getEncoded

        mkNewKeyO(ByteString.copyFrom(subjectPublicKeyInfo))

      case (SigningKeySpec.EcP256, CryptoKeyFormat.Der) |
          (SigningKeySpec.EcP384, CryptoKeyFormat.Der) =>
        mkNewKeyO(key)

      case _ => Right(None)
    }
  }

  @VisibleForTesting
  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  override private[canton] def reverseMigrate(): Option[K] =
    (keySpec, format) match {
      case (SigningKeySpec.EcCurve25519, CryptoKeyFormat.DerX509Spki) =>
        val subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(key.toByteArray)
        val publicKeyData = subjectPublicKeyInfo.getPublicKeyData.getBytes

        Some(
          SigningPublicKey(
            CryptoKeyFormat.Raw,
            ByteString.copyFrom(publicKeyData),
            SigningKeySpec.EcCurve25519,
            usage,
            dataForFingerprintO = None,
          )()
        )

      case (SigningKeySpec.EcP256, CryptoKeyFormat.DerX509Spki) |
          (SigningKeySpec.EcP384, CryptoKeyFormat.DerX509Spki) =>
        Some(
          SigningPublicKey(CryptoKeyFormat.Der, key, keySpec, usage, dataForFingerprintO = None)()
        )

      case _ => None
    }

  @VisibleForTesting
  def replaceUsage(usage: NonEmpty[Set[SigningKeyUsage]]): SigningPublicKey =
    this.copy(usage = usage)(migrated)
}

object SigningPublicKey
    extends HasVersionedMessageCompanion[SigningPublicKey]
    with HasVersionedMessageCompanionDbHelpers[SigningPublicKey] {
  override def name: String = "signing public key"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(v30.SigningPublicKey)(fromProtoV30),
      _.toProtoV30,
    )
  )

  private def getDataForFingerprint(
      keySpec: SigningKeySpec,
      format: CryptoKeyFormat,
      key: ByteString,
  ): Either[KeyParseAndValidateError, Option[ByteString]] =
    (format, keySpec) match {
      case (CryptoKeyFormat.DerX509Spki, SigningKeySpec.EcCurve25519) =>
        // To be backward-compatible, the hash for the fingerprint must apply only to the "raw" public key
        for {
          publicKeyData <- Either
            .catchOnly[IllegalArgumentException] {
              val subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(key.toByteArray)
              subjectPublicKeyInfo.getPublicKeyData.getBytes
            }
            .leftMap(err => KeyParseAndValidateError(s"Failed to parse public key: $err"))
        } yield Some(ByteString.copyFrom(publicKeyData))

      case _ => Right(None)
    }

  private[crypto] def create(
      format: CryptoKeyFormat,
      key: ByteString,
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[ProtoDeserializationError.CryptoDeserializationError, SigningPublicKey] =
    for {
      _ <- Either
        .cond(
          SigningKeyUsage.isUsageValid(usage),
          (),
          s"Invalid usage $usage",
        )
        .leftMap(err =>
          ProtoDeserializationError.CryptoDeserializationError(DefaultDeserializationError(s"$err"))
        )
      dataForFingerprintO <- getDataForFingerprint(keySpec, format, key).leftMap(err =>
        ProtoDeserializationError.CryptoDeserializationError(DefaultDeserializationError(s"$err"))
      )

      keyBeforeMigration = SigningPublicKey(
        format,
        key,
        keySpec,
        // if a key is something else than a namespace or identity delegation, then it can be used to sign itself to
        // prove ownership for OwnerToKeyMapping and PartyToKeyMapping requests.
        SigningKeyUsage.addProofOfOwnership(usage),
        dataForFingerprintO,
      )()
      keyAfterMigrationO <- keyBeforeMigration
        .migrate()
        .leftMap(err =>
          ProtoDeserializationError.CryptoDeserializationError(DefaultDeserializationError(s"$err"))
        )
      keyAfterMigration = keyAfterMigrationO match {
        case None => keyBeforeMigration
        case Some(migratedKey) if migratedKey.id == keyBeforeMigration.id => migratedKey
        case Some(migratedKey) =>
          throw new IllegalStateException(
            s"Key ID changed: ${keyBeforeMigration.id} -> ${migratedKey.id}"
          )
      }
      validatedKey <- keyAfterMigration.validated
    } yield validatedKey

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

  type PK = SigningPublicKey

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

final case class SigningPrivateKey private (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    keySpec: SigningKeySpec,
    usage: NonEmpty[Set[SigningKeyUsage]],
)(
    override val migrated: Boolean = false
) extends PrivateKey
    with HasVersionedWrapper[SigningPrivateKey] {

  override type K = SigningPrivateKey

  require(SigningKeyUsage.isUsageValid(usage), s"Invalid usage $usage")

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

  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  private[crypto] def migrate(): Option[SigningPrivateKey] = {
    def mkNewKeyO(newKey: ByteString): Option[SigningPrivateKey] = {
      val newFormat = CryptoKeyFormat.DerPkcs8Pki
      Some(SigningPrivateKey(id, newFormat, newKey, keySpec, usage)(migrated = true))
    }

    (keySpec, format) match {
      case (SigningKeySpec.EcCurve25519, CryptoKeyFormat.Raw) =>
        // The key is stored as pure bytes; we need to convert it to a PrivateKeyInfo structure
        val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
        val privateKeyInfo = new PrivateKeyInfo(
          algoId,
          new DEROctetString(key.toByteArray),
        ).getEncoded

        mkNewKeyO(ByteString.copyFrom(privateKeyInfo))

      case (SigningKeySpec.EcP256, CryptoKeyFormat.Der) |
          (SigningKeySpec.EcP384, CryptoKeyFormat.Der) =>
        mkNewKeyO(key)

      case _ => None
    }
  }

  @VisibleForTesting
  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
  override private[canton] def reverseMigrate(): Option[K] =
    (keySpec, format) match {
      case (SigningKeySpec.EcCurve25519, CryptoKeyFormat.DerPkcs8Pki) =>
        val privateKeyInfo = PrivateKeyInfo.getInstance(key.toByteArray)
        val privateKeyData =
          ASN1OctetString.getInstance(privateKeyInfo.getPrivateKey.getOctets).getOctets

        Some(
          new SigningPrivateKey(
            id,
            CryptoKeyFormat.Raw,
            ByteString.copyFrom(privateKeyData),
            SigningKeySpec.EcCurve25519,
            usage,
          )()
        )

      case (SigningKeySpec.EcP256, CryptoKeyFormat.DerPkcs8Pki) |
          (SigningKeySpec.EcP384, CryptoKeyFormat.DerPkcs8Pki) =>
        Some(
          SigningPrivateKey(id, CryptoKeyFormat.Der, key, keySpec, usage)()
        )

      case _ => None
    }

  @VisibleForTesting
  def replaceUsage(usage: NonEmpty[Set[SigningKeyUsage]]): SigningPrivateKey =
    this.copy(usage = usage)(migrated)
}

object SigningPrivateKey extends HasVersionedMessageCompanion[SigningPrivateKey] {
  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v33,
      supportedProtoVersion(v30.SigningPrivateKey)(fromProtoV30),
      _.toProtoV30,
    )
  )

  override def name: String = "signing private key"

  private[crypto] def create(
      id: Fingerprint,
      format: CryptoKeyFormat,
      key: ByteString,
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[ProtoDeserializationError.CryptoDeserializationError, SigningPrivateKey] =
    Either.cond(
      SigningKeyUsage.isUsageValid(usage), {
        val keyBeforeMigration = SigningPrivateKey(
          id,
          format,
          key,
          keySpec,
          // if a key is something else than a namespace or identity delegation, then it can be used to sign itself to
          // prove ownership for OwnerToKeyMapping and PartyToKeyMapping requests.
          SigningKeyUsage.addProofOfOwnership(usage),
        )()

        val keyAfterMigration = keyBeforeMigration.migrate().getOrElse(keyBeforeMigration)
        keyAfterMigration
      },
      ProtoDeserializationError.CryptoDeserializationError(
        DefaultDeserializationError(s"Invalid usage $usage")
      ),
    )

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
      key <- SigningPrivateKey.create(id, format, privateKeyP.privateKey, keySpec, usage)
    } yield key

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

  final case class KeyAlgoSpecsMismatch(
      signingKeySpec: SigningKeySpec,
      algorithmSpec: SigningAlgorithmSpec,
      supportedKeySpecsByAlgo: Set[SigningKeySpec],
  ) extends SigningError {
    override def pretty: Pretty[KeyAlgoSpecsMismatch] = prettyOfClass(
      param("signingKeySpec", _.signingKeySpec),
      param("algorithmSpec", _.algorithmSpec),
      param("supportedKeySpecsByAlgo", _.supportedKeySpecsByAlgo),
    )
  }

  final case class InvalidKeyUsage(
      keyId: Fingerprint,
      keyUsage: Set[SigningKeyUsage],
      expectedKeyUsage: Set[SigningKeyUsage],
  ) extends SigningError {
    override def pretty: Pretty[InvalidKeyUsage] = prettyOfClass(
      param("keyId", _.keyId),
      param("keyUsage", _.keyUsage),
      param("expectedKeyUsage", _.expectedKeyUsage),
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
        extends CantonBaseError.Impl(cause = "Unable to create signing key")
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

  final case class InvalidKeyUsage(
      keyId: Fingerprint,
      keyUsage: Set[SigningKeyUsage],
      expectedKeyUsage: Set[SigningKeyUsage],
  ) extends SignatureCheckError {
    override def pretty: Pretty[InvalidKeyUsage] = prettyOfClass(
      param("keyId", _.keyId),
      param("keyUsage", _.keyUsage),
      param("expectedKeyUsage", _.expectedKeyUsage),
    )
  }

  final case class InvalidSignatureFormat(message: String) extends SignatureCheckError {
    override protected def pretty: Pretty[InvalidSignatureFormat] = prettyOfClass(
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
