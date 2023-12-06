// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String300,
  String68,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.SafeSimpleString
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.google.protobuf.ByteString
import io.circe.Encoder
import slick.jdbc.{GetResult, SetParameter}

trait CryptoKey extends Product with Serializable {
  def format: CryptoKeyFormat
  protected[crypto] def key: ByteString
}

/** a human readable fingerprint of a key that serves as a unique identifier */
final case class Fingerprint(protected val str: String68)
    extends LengthLimitedStringWrapper
    with PrettyPrinting {
  def toLengthLimitedString: String68 = str

  override def pretty: Pretty[Fingerprint] = prettyOfParam(_.unwrap.readableHash)
}

object Fingerprint {

  implicit val fingerprintOrder: Order[Fingerprint] =
    Order.by[Fingerprint, String](_.unwrap)

  implicit val setParameterFingerprint: SetParameter[Fingerprint] = (f, pp) =>
    pp >> f.toLengthLimitedString
  implicit val getResultFingerprint: GetResult[Fingerprint] = GetResult { r =>
    Fingerprint
      .fromProtoPrimitive(r.nextString())
      .valueOr(err =>
        throw new DbDeserializationException(s"Failed to deserialize fingerprint: $err")
      )
  }

  implicit val fingerprintEncoder: Encoder[Fingerprint] =
    Encoder.encodeString.contramap[Fingerprint](_.unwrap)

  private[this] def apply(hash: Hash): Fingerprint =
    throw new UnsupportedOperationException("Use create/deserialization methods instead.")

  /** create fingerprint from a human readable string */
  def fromProtoPrimitive(str: String): ParsingResult[Fingerprint] =
    SafeSimpleString
      .fromProtoPrimitive(str)
      .leftMap(ProtoDeserializationError.StringConversionError)
      .flatMap(String68.fromProtoPrimitive(_, "Fingerprint"))
      .map(Fingerprint(_))

  private[crypto] def create(
      bytes: ByteString
  ): Fingerprint = {
    val hash = Hash.digest(HashPurpose.PublicKeyFingerprint, bytes, HashAlgorithm.Sha256)
    new Fingerprint(hash.toLengthLimitedHexString)
  }

  def tryCreate(str: String): Fingerprint =
    fromProtoPrimitive(str).valueOr(err =>
      throw new IllegalArgumentException(s"Invalid fingerprint $str: $err")
    )
}

trait CryptoKeyPairKey extends CryptoKey {
  def id: Fingerprint

  def isPublicKey: Boolean
}

trait CryptoKeyPair[+PK <: PublicKey, +SK <: PrivateKey]
    extends HasVersionedWrapper[CryptoKeyPair[PublicKey, PrivateKey]] {

  require(
    publicKey.id == privateKey.id,
    "Public and private key of the same key pair must have the same ids.",
  )

  override protected def companionObj = CryptoKeyPair

  def publicKey: PK
  def privateKey: SK

  // The keypair is identified by the public key's id
  def id: Fingerprint = publicKey.id

  protected def toProtoCryptoKeyPairPairV0: v0.CryptoKeyPair.Pair

  def toProtoCryptoKeyPairV0: v0.CryptoKeyPair = v0.CryptoKeyPair(toProtoCryptoKeyPairPairV0)
}

object CryptoKeyPair extends HasVersionedMessageCompanion[CryptoKeyPair[PublicKey, PrivateKey]] {

  override def name: String = "crypto key pair"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v0.CryptoKeyPair)(fromProtoCryptoKeyPairV0),
      _.toProtoCryptoKeyPairV0.toByteString,
    )
  )

  def fromProtoCryptoKeyPairV0(
      keyPair: v0.CryptoKeyPair
  ): ParsingResult[CryptoKeyPair[_ <: PublicKey, _ <: PrivateKey]] =
    for {
      pair <- keyPair.pair match {
        case v0.CryptoKeyPair.Pair.EncryptionKeyPair(value) =>
          EncryptionKeyPair
            .fromProtoV0(value): Either[
            ProtoDeserializationError,
            CryptoKeyPair[EncryptionPublicKey, EncryptionPrivateKey],
          ]
        case v0.CryptoKeyPair.Pair.SigningKeyPair(value) =>
          SigningKeyPair
            .fromProtoV0(value): Either[
            ProtoDeserializationError,
            CryptoKeyPair[SigningPublicKey, SigningPrivateKey],
          ]
        case v0.CryptoKeyPair.Pair.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("pair"))
      }
    } yield pair
}

trait PublicKey extends CryptoKeyPairKey {
  def toByteString(version: ProtocolVersion): ByteString

  def fingerprint: Fingerprint = id

  def purpose: KeyPurpose

  def isSigning: Boolean = purpose == KeyPurpose.Signing

  override def isPublicKey: Boolean = true

  protected def toProtoPublicKeyKeyV0: v0.PublicKey.Key

  /** With the v0.PublicKey message we model the class hierarchy of public keys in protobuf.
    * Each child class that implements this trait can be serialized with `toProto` to their corresponding protobuf
    * message. With the following method, it can be serialized to this trait's protobuf message.
    */
  def toProtoPublicKeyV0: v0.PublicKey = v0.PublicKey(key = toProtoPublicKeyKeyV0)
}

object PublicKey {
  def fromProtoPublicKeyV0(publicKeyP: v0.PublicKey): ParsingResult[PublicKey] =
    publicKeyP.key match {
      case v0.PublicKey.Key.Empty => Left(ProtoDeserializationError.FieldNotSet("key"))
      case v0.PublicKey.Key.EncryptionPublicKey(encPubKeyP) =>
        EncryptionPublicKey.fromProtoV0(encPubKeyP)
      case v0.PublicKey.Key.SigningPublicKey(signPubKeyP) =>
        SigningPublicKey.fromProtoV0(signPubKeyP)
    }

}

final case class KeyName(protected val str: String300)
    extends LengthLimitedStringWrapper
    with PrettyPrinting {
  def emptyStringAsNone: Option[KeyName] = if (str.unwrap.isEmpty) None else Some(this)
  override def pretty: Pretty[KeyName] = prettyOfClass(unnamedParam(_.str.unwrap.unquoted))
}
object KeyName extends LengthLimitedStringWrapperCompanion[String300, KeyName] {
  override def instanceName: String = "KeyName"
  override protected def companion: String300.type = String300
  override protected def factoryMethodWrapper(str: String300): KeyName = KeyName(str)
}

trait PublicKeyWithName
    extends Product
    with Serializable
    with HasVersionedWrapper[PublicKeyWithName] {
  type K <: PublicKey
  def publicKey: K
  def name: Option[KeyName]

  override protected def companionObj = PublicKeyWithName

  def toProtoV0: v0.PublicKeyWithName =
    v0.PublicKeyWithName(
      publicKey = Some(
        publicKey.toProtoPublicKeyV0
      ),
      name = name.map(_.unwrap).getOrElse(""),
    )
}

object PublicKeyWithName extends HasVersionedMessageCompanion[PublicKeyWithName] {

  override def name: String = "PublicKeyWithName"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v0.PublicKeyWithName)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def fromProtoV0(key: v0.PublicKeyWithName): ParsingResult[PublicKeyWithName] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        PublicKey.fromProtoPublicKeyV0,
        "public_key",
        key.publicKey,
      )
      name <- KeyName.fromProtoPrimitive(key.name)
    } yield {
      (publicKey: @unchecked) match {
        case k: SigningPublicKey => SigningPublicKeyWithName(k, name.emptyStringAsNone)
        case k: EncryptionPublicKey => EncryptionPublicKeyWithName(k, name.emptyStringAsNone)
      }
    }
}

// The private key id must match the corresponding public key's one
trait PrivateKey extends CryptoKeyPairKey {
  def purpose: KeyPurpose

  override def isPublicKey: Boolean = false

  protected def toProtoPrivateKeyKeyV0: v0.PrivateKey.Key

  /** Same representation of the class hierarchy in protobuf messages, see [[PublicKey]]. */
  def toProtoPrivateKey: v0.PrivateKey = v0.PrivateKey(key = toProtoPrivateKeyKeyV0)
}

object PrivateKey {

  def fromProtoPrivateKey(
      privateKeyP: v0.PrivateKey
  ): ParsingResult[PrivateKey] =
    privateKeyP.key match {
      case v0.PrivateKey.Key.Empty => Left(ProtoDeserializationError.FieldNotSet("key"))
      case v0.PrivateKey.Key.EncryptionPrivateKey(encPrivKeyP) =>
        EncryptionPrivateKey.fromProtoV0(encPrivKeyP)
      case v0.PrivateKey.Key.SigningPrivateKey(signPrivKeyP) =>
        SigningPrivateKey.fromProtoV0(signPrivKeyP)
    }

}

sealed trait CryptoKeyFormat extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v0.CryptoKeyFormat
  override def pretty: Pretty[this.type] = prettyOfString(_.name)
}

object CryptoKeyFormat {

  implicit val cryptoKeyFormatOrder: Order[CryptoKeyFormat] =
    Order.by[CryptoKeyFormat, String](_.name)

  case object Tink extends CryptoKeyFormat {
    override val name: String = "Tink"
    override def toProtoEnum: v0.CryptoKeyFormat = v0.CryptoKeyFormat.Tink
  }

  case object Der extends CryptoKeyFormat {
    override val name: String = "DER"
    override def toProtoEnum: v0.CryptoKeyFormat = v0.CryptoKeyFormat.Der
  }

  case object Raw extends CryptoKeyFormat {
    override val name: String = "Raw"
    override def toProtoEnum: v0.CryptoKeyFormat = v0.CryptoKeyFormat.Raw
  }

  case object Symbolic extends CryptoKeyFormat {
    override val name: String = "Symbolic"
    override def toProtoEnum: v0.CryptoKeyFormat = v0.CryptoKeyFormat.Symbolic
  }

  def fromProtoEnum(
      field: String,
      formatP: v0.CryptoKeyFormat,
  ): ParsingResult[CryptoKeyFormat] =
    formatP match {
      case v0.CryptoKeyFormat.MissingCryptoKeyFormat =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.CryptoKeyFormat.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.CryptoKeyFormat.Tink => Right(CryptoKeyFormat.Tink)
      case v0.CryptoKeyFormat.Der => Right(CryptoKeyFormat.Der)
      case v0.CryptoKeyFormat.Raw => Right(CryptoKeyFormat.Raw)
      case v0.CryptoKeyFormat.Symbolic => Right(CryptoKeyFormat.Symbolic)
    }
}

sealed trait KeyPurpose extends Product with Serializable with PrettyPrinting {

  def name: String

  // An identifier for a key purpose that is used for serialization
  def id: Byte

  def toProtoEnum: v0.KeyPurpose

  override def pretty: Pretty[KeyPurpose.this.type] = prettyOfString(_.name)
}

object KeyPurpose {

  val all = Set(Signing, Encryption)

  implicit val setParameterKeyPurpose: SetParameter[KeyPurpose] = (k, pp) => pp.setByte(k.id)
  implicit val getResultKeyPurpose: GetResult[KeyPurpose] = GetResult { r =>
    r.nextByte() match {
      case Signing.id => Signing
      case Encryption.id => Encryption
      case unknown => throw new DbDeserializationException(s"Unknown key purpose id: $unknown")
    }
  }

  case object Signing extends KeyPurpose {
    override val name: String = "signing"
    override val id: Byte = 0
    override def toProtoEnum: v0.KeyPurpose = v0.KeyPurpose.SigningKeyPurpose
  }

  case object Encryption extends KeyPurpose {
    override val name: String = "encryption"
    override val id: Byte = 1
    override def toProtoEnum: v0.KeyPurpose = v0.KeyPurpose.EncryptionKeyPurpose
  }

  def fromProtoEnum(
      field: String,
      purposeP: v0.KeyPurpose,
  ): ParsingResult[KeyPurpose] =
    purposeP match {
      case v0.KeyPurpose.UnknownKeyPurpose => Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.KeyPurpose.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.KeyPurpose.SigningKeyPurpose => Right(Signing)
      case v0.KeyPurpose.EncryptionKeyPurpose => Right(Encryption)
    }

}

/** Information that is cached for each view and is to be re-used if another view has
  * the same recipients and transparency can be respected.
  * @param sessionKeyRandomness the randomness to create the session key that is then used to encrypt the randomness of the view.
  * @param encryptedSessionKeys the randomness of the session key encrypted for each recipient.
  */
final case class SessionKeyInfo(
    sessionKeyRandomness: SecureRandomness,
    encryptedSessionKeys: Seq[AsymmetricEncrypted[SecureRandomness]],
)
