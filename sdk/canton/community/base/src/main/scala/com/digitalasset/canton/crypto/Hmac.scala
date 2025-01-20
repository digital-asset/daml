// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbSerializationException
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import java.security.{InvalidKeyException, NoSuchAlgorithmException}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

sealed abstract class HmacAlgorithm(val name: String, val hashAlgorithm: HashAlgorithm)
    extends PrettyPrinting {

  def toProtoEnum: v0.HmacAlgorithm

  override def pretty: Pretty[HmacAlgorithm] = prettyOfString(_.name)
}

object HmacAlgorithm {

  val algorithms: Seq[HmacAlgorithm] = Seq(HmacSha256)

  case object HmacSha256 extends HmacAlgorithm("HMACSHA256", HashAlgorithm.Sha256) {
    override def toProtoEnum: v0.HmacAlgorithm = v0.HmacAlgorithm.HmacSha256
  }

  def fromProtoEnum(
      field: String,
      hmacAlgorithmP: v0.HmacAlgorithm,
  ): ParsingResult[HmacAlgorithm] =
    hmacAlgorithmP match {
      case v0.HmacAlgorithm.MissingHmacAlgorithm =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.HmacAlgorithm.HmacSha256 => Right(HmacSha256)
      case v0.HmacAlgorithm.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
    }

}

final case class Hmac private (private val hmac: ByteString, private val algorithm: HmacAlgorithm)
    extends PrettyPrinting {

  require(!hmac.isEmpty, "HMAC must not be empty")
  require(
    hmac.size() == algorithm.hashAlgorithm.length,
    s"HMAC size ${hmac.size()} must match HMAC's hash algorithm length ${algorithm.hashAlgorithm.length}",
  )

  def toProtoV0: v0.Hmac =
    v0.Hmac(algorithm = algorithm.toProtoEnum, hmac = hmac)

  override def pretty: Pretty[Hmac] = {
    implicit val ps = PrettyInstances.prettyString
    PrettyUtil.prettyInfix[Hmac](_.algorithm.name, ":", _.hmac)
  }

  /** Access to the raw HMAC, should NOT be used for serialization. */
  def unwrap: ByteString = hmac
}

object Hmac {

  import HmacError.*

  private[crypto] def create(
      hmac: ByteString,
      algorithm: HmacAlgorithm,
  ): Either[HmacError, Hmac] = {
    Either.cond(
      hmac.size() == algorithm.hashAlgorithm.length,
      new Hmac(hmac, algorithm),
      InvalidHmacLength(hmac.size(), algorithm.hashAlgorithm.length),
    )
  }

  def fromProtoV0(hmacP: v0.Hmac): ParsingResult[Hmac] =
    for {
      hmacAlgorithm <- HmacAlgorithm.fromProtoEnum("algorithm", hmacP.algorithm)
      hmac <- Hmac
        .create(hmacP.hmac, hmacAlgorithm)
        .leftMap(err =>
          ProtoDeserializationError.CryptoDeserializationError(
            DefaultDeserializationError(s"Failed to deserialize HMAC: $err")
          )
        )
    } yield hmac

  /** Computes the HMAC of the given message using an explicit secret.
    * See [[https://en.wikipedia.org/wiki/HMAC]]
    */
  def compute(
      secret: HmacSecret,
      message: ByteString,
      algorithm: HmacAlgorithm,
  ): Either[HmacError, Hmac] =
    for {
      mac <- Either
        .catchOnly[NoSuchAlgorithmException](Mac.getInstance(algorithm.name))
        .leftMap(ex => UnknownHmacAlgorithm(algorithm, ex))
      key = new SecretKeySpec(secret.unwrap.toByteArray, algorithm.name)
      _ <- Either.catchOnly[InvalidKeyException](mac.init(key)).leftMap(ex => InvalidHmacSecret(ex))
      hmacBytes <- Either
        .catchOnly[IllegalStateException](mac.doFinal(message.toByteArray))
        .leftMap(ex => FailedToComputeHmac(ex))
      hmac = new Hmac(ByteString.copyFrom(hmacBytes), algorithm)
    } yield hmac
}

final case class HmacSecret private (private val secret: ByteString) extends PrettyPrinting {

  require(!secret.isEmpty, "HMAC secret cannot be empty")

  private[crypto] def unwrap: ByteString = secret

  // intentionally removing the value from toString to avoid printing secret in logs
  override def pretty: Pretty[HmacSecret] =
    prettyOfString(secret => s"HmacSecret(length: ${secret.length})")

  val length: Int = secret.size()
}

object HmacSecret {

  implicit val setHmacSecretParameter: SetParameter[HmacSecret] = (v, pp) => {
    import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteString
    pp.>>(v.secret)
  }

  implicit val getHmacSecretResult: GetResult[HmacSecret] = GetResult { r =>
    import com.digitalasset.canton.resource.DbStorage.Implicits.getResultByteString
    HmacSecret
      .create(r.<<)
      .valueOr(err =>
        throw new DbSerializationException(s"Failed to deserialize HMAC secret: $err")
      )
  }

  /** Recommended length for HMAC secret keys is 128 bits */
  val defaultLength = 16

  private[crypto] def create(bytes: ByteString): Either[HmacError, HmacSecret] =
    Either.cond(!bytes.isEmpty, new HmacSecret(bytes), HmacError.EmptyHmacSecret)

  /** Generates a new random HMAC secret key. A minimum secret key length of 128 bits is enforced.
    *
    * NOTE: The length of the HMAC secret should not exceed the internal _block_ size of the hash function,
    * e.g., 512 bits for SHA256.
    */
  def generate(randomOps: RandomOps, length: Int = defaultLength): HmacSecret = {
    require(length >= defaultLength, s"Specified HMAC secret key length ${length} too small.")
    new HmacSecret(randomOps.generateRandomByteString(length))
  }
}

/** pure HMAC operations that do not require access to external keys. */
trait HmacOps {

  def defaultHmacAlgorithm: HmacAlgorithm = HmacAlgorithm.HmacSha256

  def hmacWithSecret(
      secret: HmacSecret,
      message: ByteString,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HmacError, Hmac] =
    Hmac.compute(secret, message, algorithm)

}

sealed trait HmacError extends Product with Serializable with PrettyPrinting

object HmacError {
  final case class UnknownHmacAlgorithm(algorithm: HmacAlgorithm, cause: Exception)
      extends HmacError {
    override def pretty: Pretty[UnknownHmacAlgorithm] = prettyOfClass(
      param("algorithm", _.algorithm.name.unquoted),
      param("cause", _.cause),
    )
  }
  case object EmptyHmacSecret extends HmacError {
    override def pretty: Pretty[EmptyHmacSecret.type] = prettyOfObject[EmptyHmacSecret.type]
  }
  final case class InvalidHmacSecret(cause: Exception) extends HmacError {
    override def pretty: Pretty[InvalidHmacSecret] = prettyOfClass(unnamedParam(_.cause))
  }
  final case class FailedToComputeHmac(cause: Exception) extends HmacError {
    override def pretty: Pretty[FailedToComputeHmac] = prettyOfClass(unnamedParam(_.cause))
  }
  final case class InvalidHmacLength(inputLength: Int, expectedLength: Long) extends HmacError {
    override def pretty: Pretty[InvalidHmacLength] = prettyOfClass(
      param("inputLength", _.inputLength),
      param("expectedLength", _.expectedLength),
    )
  }
  case object MissingHmacSecret extends HmacError {
    override def pretty: Pretty[MissingHmacSecret.type] = prettyOfObject[MissingHmacSecret.type]
  }
  final case class HmacPrivateStoreError(error: CryptoPrivateStoreError) extends HmacError {
    override def pretty: Pretty[HmacPrivateStoreError] = prettyOfParam(_.error)
  }
}
