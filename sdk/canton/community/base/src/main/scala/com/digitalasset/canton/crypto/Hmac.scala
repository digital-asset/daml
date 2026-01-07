// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.EitherUtil
import com.google.protobuf.ByteString

sealed abstract class HmacAlgorithm(val name: String, val hashAlgorithm: HashAlgorithm)
    extends PrettyPrinting {

  def toProtoEnum: v30.HmacAlgorithm

  override protected def pretty: Pretty[HmacAlgorithm] = prettyOfString(_.name)
}

object HmacAlgorithm {

  val algorithms: Seq[HmacAlgorithm] = Seq(HmacSha256)

  case object HmacSha256 extends HmacAlgorithm("HMACSHA256", HashAlgorithm.Sha256) {
    override def toProtoEnum: v30.HmacAlgorithm = v30.HmacAlgorithm.HMAC_ALGORITHM_HMAC_SHA256
  }

  def fromProtoEnum(
      field: String,
      hmacAlgorithmP: v30.HmacAlgorithm,
  ): ParsingResult[HmacAlgorithm] =
    hmacAlgorithmP match {
      case v30.HmacAlgorithm.HMAC_ALGORITHM_UNSPECIFIED =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.HmacAlgorithm.HMAC_ALGORITHM_HMAC_SHA256 => Right(HmacSha256)
      case v30.HmacAlgorithm.Unrecognized(value) =>
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

  override protected def pretty: Pretty[Hmac] = {
    implicit val ps: Pretty[String] = PrettyInstances.prettyString
    PrettyUtil.prettyInfix[Hmac](_.algorithm.name, ":", _.hmac)
  }

  /** Access to the raw HMAC, should NOT be used for serialization. */
  private[crypto] def unwrap: ByteString = hmac
}

object Hmac {

  private[crypto] def create(
      hmac: ByteString,
      algorithm: HmacAlgorithm,
  ): Either[HmacError, Hmac] =
    for {
      _ <- EitherUtil.condUnit(
        hmac.size() == algorithm.hashAlgorithm.length,
        HmacError.InvalidHmacLength(hmac.size(), algorithm.hashAlgorithm.length.toInt),
      )
    } yield Hmac(hmac, algorithm)

}

/** pure HMAC operations that do not require access to external keys. */
trait HmacOps {

  /** Minimum length for HMAC secret keys is 128 bits */
  private[crypto] def minimumSecretKeyLengthInBytes = 16

  def defaultHmacAlgorithm: HmacAlgorithm = HmacAlgorithm.HmacSha256

  /** Computes the HMAC of the given message using an explicit secret. See
    * [[https://en.wikipedia.org/wiki/HMAC]].
    *
    * A minimum secret key length of 128 bits is enforced. NOTE: The length of the HMAC secret
    * should not exceed the internal _block_ size of the hash function, e.g., 512 bits for SHA256.
    */
  private[crypto] def computeHmacWithSecret(
      secret: ByteString,
      message: ByteString,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HmacError, Hmac] = {

    // The length of the HMAC secret should not exceed the internal _block_ size of the hash
    // function, e.g., 512 bits for SHA256.
    val maximumSecretKeyLengthInBytes = algorithm.hashAlgorithm.internalBlockSizeInBytes
    val secretKeyLength = secret.size()
    for {
      _ <- EitherUtil.condUnit(
        secretKeyLength >= minimumSecretKeyLengthInBytes && secretKeyLength <= maximumSecretKeyLengthInBytes,
        HmacError.InvalidHmacKeyLength(
          secretKeyLength,
          minimumSecretKeyLengthInBytes,
          maximumSecretKeyLengthInBytes,
        ),
      )
      hmac <- computeHmacWithSecretInternal(secret, message, algorithm)
    } yield hmac
  }

  private[crypto] def computeHmacWithSecretInternal(
      secret: ByteString,
      message: ByteString,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HmacError, Hmac]

}

sealed trait HmacError extends Product with Serializable with PrettyPrinting

object HmacError {
  final case class UnknownHmacAlgorithm(algorithm: HmacAlgorithm, cause: Exception)
      extends HmacError {
    override protected def pretty: Pretty[UnknownHmacAlgorithm] = prettyOfClass(
      param("algorithm", _.algorithm.name.unquoted),
      param("cause", _.cause),
    )
  }
  final case class InvalidHmacLength(
      length: Int,
      expectedLength: Int,
  ) extends HmacError {
    override protected def pretty: Pretty[InvalidHmacLength] = prettyOfClass(
      param("length", _.length),
      param("expectedLength", _.expectedLength),
    )
  }
  final case class InvalidHmacKeyLength(
      length: Int,
      minimumLength: Int,
      maximumLength: Int,
  ) extends HmacError {
    override protected def pretty: Pretty[InvalidHmacKeyLength] = prettyOfClass(
      param("length", _.length),
      param("minimumLength", _.minimumLength),
      param("maximumLength", _.maximumLength),
    )
  }
  final case class InvalidHmacSecret(cause: Exception) extends HmacError {
    override protected def pretty: Pretty[InvalidHmacSecret] = prettyOfClass(unnamedParam(_.cause))
  }
  final case class FailedToComputeHmac(cause: Exception) extends HmacError {
    override protected def pretty: Pretty[FailedToComputeHmac] = prettyOfClass(
      unnamedParam(_.cause)
    )
  }
}
