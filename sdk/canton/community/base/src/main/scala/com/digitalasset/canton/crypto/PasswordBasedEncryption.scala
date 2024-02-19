// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.{
  HasVersionedMessageCompanion,
  HasVersionedMessageCompanionCommon,
  HasVersionedToByteString,
  HasVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
}
import com.google.protobuf.ByteString

/** A symmetric key derived from a password */
final case class PasswordBasedEncryptionKey(key: SymmetricKey, salt: SecureRandomness)

/** A password-based encrypted message */
final case class PasswordBasedEncrypted(
    ciphertext: ByteString,
    symmetricKeyScheme: SymmetricKeyScheme,
    pbkdfScheme: PbkdfScheme,
    salt: SecureRandomness,
) extends HasVersionedWrapper[PasswordBasedEncrypted] {

  override protected def companionObj: HasVersionedMessageCompanionCommon[PasswordBasedEncrypted] =
    PasswordBasedEncrypted

  protected def toProtoV30: v30.PasswordBasedEncrypted =
    v30.PasswordBasedEncrypted(
      ciphertext = ciphertext,
      symmetricKeyScheme = symmetricKeyScheme.toProtoEnum,
      pbkdfScheme = pbkdfScheme.toProtoEnum,
      salt = salt.unwrap,
    )
}

object PasswordBasedEncrypted extends HasVersionedMessageCompanion[PasswordBasedEncrypted] {
  override val name: String = "PasswordBasedEncrypted"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(30) -> ProtoCodec(
      ProtocolVersion.v30,
      supportedProtoVersion(v30.PasswordBasedEncrypted)(fromProtoV30),
      _.toProtoV30.toByteString,
    )
  )

  private def fromProtoV30(
      encryptedP: v30.PasswordBasedEncrypted
  ): ParsingResult[PasswordBasedEncrypted] =
    for {
      symmetricKeyScheme <- SymmetricKeyScheme.fromProtoEnum(
        "symmetric_key_scheme",
        encryptedP.symmetricKeyScheme,
      )
      pbkdfScheme <- PbkdfScheme.fromProtoEnum("pbkdf_scheme", encryptedP.pbkdfScheme)
    } yield PasswordBasedEncrypted(
      ciphertext = encryptedP.ciphertext,
      symmetricKeyScheme = symmetricKeyScheme,
      pbkdfScheme = pbkdfScheme,
      salt = SecureRandomness.apply(encryptedP.salt),
    )
}

/** Password-Based Encryption (PBE) */
trait PasswordBasedEncryptionOps { this: EncryptionOps =>

  protected def defaultPbkdfScheme: PbkdfScheme

  /** Derive a symmetric encryption key from a given password.
    *
    * @param password The password used to derive the key
    * @param symmetricKeyScheme The intended symmetric encryption scheme for the password-based encryption.
    * @param pbkdfScheme The password-based key derivation function (PBKDF) scheme to derive a key from the password.
    * @param saltO The optional salt used for the key derivation. If none is a given a random salt is generated.
    */
  def deriveSymmetricKey(
      password: String,
      symmetricKeyScheme: SymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme,
      saltO: Option[SecureRandomness],
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncryptionKey]

  def encryptWithPassword[M <: HasVersionedToByteString](
      message: M,
      password: String,
      protocolVersion: ProtocolVersion,
      symmetricKeyScheme: SymmetricKeyScheme = defaultSymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme = defaultPbkdfScheme,
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncrypted] = for {
    pbkey <- deriveSymmetricKey(password, symmetricKeyScheme, pbkdfScheme, saltO = None)
    encrypted <- encryptWith(message, pbkey.key, protocolVersion).leftMap(
      PasswordBasedEncryptionError.EncryptError
    )
  } yield PasswordBasedEncrypted(encrypted.ciphertext, symmetricKeyScheme, pbkdfScheme, pbkey.salt)

  def decryptWithPassword[M](pbencrypted: PasswordBasedEncrypted, password: String)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[PasswordBasedEncryptionError, M] = for {
    pbkey <- deriveSymmetricKey(
      password,
      pbencrypted.symmetricKeyScheme,
      pbencrypted.pbkdfScheme,
      Some(pbencrypted.salt),
    )
    message <- decryptWith(Encrypted.fromByteString[M](pbencrypted.ciphertext), pbkey.key)(
      deserialize
    )
      .leftMap(PasswordBasedEncryptionError.DecryptError)
  } yield message

}

/** Schemes for Password-Based Key Derivation Functions */
sealed trait PbkdfScheme extends Product with Serializable {
  def name: String

  def toProtoEnum: v30.PbkdfScheme

  def defaultSaltLengthInBytes: Int
}

object PbkdfScheme {
  implicit val signingKeySchemeOrder: Order[PbkdfScheme] =
    Order.by[PbkdfScheme, String](_.name)

  case object Argon2idMode1 extends PbkdfScheme {
    override def name: String = "Argon2idMode1"

    override def toProtoEnum: v30.PbkdfScheme = v30.PbkdfScheme.PBKDF_SCHEME_ARGON2ID_MODE1

    override def defaultSaltLengthInBytes: Int = 16

    // Recommended parameters: https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#argon2id
    // NOTE: Do not change those parameters, instead add a new mode as a new PBKDF scheme
    val memoryInKb = 12288
    val iterations = 3
    val parallelism = 1
  }

  def fromProtoEnum(
      field: String,
      schemeP: v30.PbkdfScheme,
  ): ParsingResult[PbkdfScheme] =
    schemeP match {
      case v30.PbkdfScheme.PBKDF_SCHEME_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.PbkdfScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v30.PbkdfScheme.PBKDF_SCHEME_ARGON2ID_MODE1 =>
        Right(PbkdfScheme.Argon2idMode1)
    }

}

sealed trait PasswordBasedEncryptionError extends Product with Serializable with PrettyPrinting

object PasswordBasedEncryptionError {

  final case class DecryptError(error: DecryptionError) extends PasswordBasedEncryptionError {
    override def pretty: Pretty[DecryptError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

  final case class EncryptError(error: EncryptionError) extends PasswordBasedEncryptionError {
    override def pretty: Pretty[EncryptError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

  final case class PbkdfOutputLengthInvalid(expectedLength: Int, actualLength: Int)
      extends PasswordBasedEncryptionError {
    override def pretty: Pretty[PbkdfOutputLengthInvalid] = prettyOfClass(
      param("expectedLength", _.expectedLength),
      param("actualLength", _.actualLength),
    )
  }
}
