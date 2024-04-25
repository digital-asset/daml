// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.CantonContractIdVersion
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DefaultDeserializationError, DeterministicEncoding}
import com.digitalasset.canton.{ProtoDeserializationError, admin, crypto}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.scalaland.chimney.Transformer

/** A seed to derive further salts from.
  *
  * Unlike [[Salt]] this seed will not be shipped to another participant.
  */
abstract sealed case class SaltSeed(unwrap: ByteString)

object SaltSeed {

  /** Default length for a salt seed is 128 bits */
  val defaultLength = 16

  private[crypto] def apply(bytes: ByteString): SaltSeed =
    new SaltSeed(bytes) {}

  def generate(length: Int = defaultLength)(randomOps: RandomOps): SaltSeed =
    SaltSeed(randomOps.generateRandomByteString(length))
}

/** Indicates the algorithm used to generate and derive salts. */
sealed trait SaltAlgorithm extends Product with Serializable with PrettyPrinting {
  def toProtoOneOf: v30.Salt.Algorithm
  def length: Long
}

object SaltAlgorithm {

  /** Uses an HMAC algorithm as a pseudo-random function to generate/derive salts. */
  final case class Hmac(hmacAlgorithm: HmacAlgorithm) extends SaltAlgorithm {
    override def toProtoOneOf: v30.Salt.Algorithm =
      v30.Salt.Algorithm.Hmac(hmacAlgorithm.toProtoEnum)
    override def length: Long = hmacAlgorithm.hashAlgorithm.length
    override def pretty: Pretty[Hmac] = prettyOfClass(
      param("hmacAlgorithm", _.hmacAlgorithm.name.unquoted)
    )
  }

  def fromProtoOneOf(
      field: String,
      saltAlgorithmP: v30.Salt.Algorithm,
  ): ParsingResult[SaltAlgorithm] =
    saltAlgorithmP match {
      case v30.Salt.Algorithm.Empty => Left(ProtoDeserializationError.FieldNotSet(field))
      case v30.Salt.Algorithm.Hmac(hmacAlgorithmP) =>
        HmacAlgorithm.fromProtoEnum("hmac", hmacAlgorithmP).map(Hmac)
    }
}

/** A (pseudo-)random salt used for hashing to prevent pre-computed hash attacks.
  *
  * The algorithm that was used to generate/derive the salt is kept to support the verification of the salt generation.
  */
final case class Salt private (private val salt: ByteString, private val algorithm: SaltAlgorithm)
    extends PrettyPrinting {

  require(!salt.isEmpty, "Salt must not be empty")
  require(
    salt.size() == algorithm.length,
    s"Salt size ${salt.size()} must match salt algorithm length ${algorithm.length}",
  )

  /** Returns the serialization used for networking/storing, must NOT be used for hashing. */
  def toProtoV30: v30.Salt = v30.Salt(salt = salt, algorithm = algorithm.toProtoOneOf)

  /** Returns the salt used for hashing, must NOT be used for networking/storing. */
  def forHashing: ByteString = salt

  def size: Int = salt.size()

  @VisibleForTesting
  private[crypto] def unwrap: ByteString = salt

  override val pretty: Pretty[Salt] = prettyOfParam(_.salt)
}

object Salt {

  private[crypto] def create(bytes: ByteString, algorithm: SaltAlgorithm): Either[SaltError, Salt] =
    Either.cond(
      !bytes.isEmpty && bytes.size() == algorithm.length,
      new Salt(bytes, algorithm),
      SaltError.InvalidSaltCreation(bytes, algorithm),
    )

  private def deriveSalt(
      seed: ByteString,
      bytes: ByteString,
      hmacOps: HmacOps,
  ): Either[SaltError, Salt] =
    for {
      pseudoSecret <- HmacSecret
        .create(seed)
        .leftMap(SaltError.HmacGenerationError)
      saltAlgorithm = SaltAlgorithm.Hmac(hmacOps.defaultHmacAlgorithm)
      hmac <- hmacOps
        .hmacWithSecret(pseudoSecret, bytes, saltAlgorithm.hmacAlgorithm)
        .leftMap(SaltError.HmacGenerationError)
      salt <- create(hmac.unwrap, saltAlgorithm)
    } yield salt

  /** Derives a salt from a `seed` salt and an `index`. */
  def deriveSalt(seed: SaltSeed, index: Int, hmacOps: HmacOps): Either[SaltError, Salt] = {
    deriveSalt(seed, DeterministicEncoding.encodeInt(index), hmacOps)
  }

  def tryDeriveSalt(seed: SaltSeed, index: Int, hmacOps: HmacOps): Salt = {
    deriveSalt(seed, index, hmacOps).valueOr(err => throw new IllegalStateException(err.toString))
  }

  /** Derives a salt from a `seed` salt and `bytes` using an HMAC as a pseudo-random function. */
  def deriveSalt(seed: SaltSeed, bytes: ByteString, hmacOps: HmacOps): Either[SaltError, Salt] =
    deriveSalt(seed.unwrap, bytes, hmacOps)

  def tryDeriveSalt(seed: SaltSeed, bytes: ByteString, hmacOps: HmacOps): Salt =
    deriveSalt(seed, bytes, hmacOps).valueOr(err => throw new IllegalStateException(err.toString))

  def tryDeriveSalt(
      seed: Salt,
      bytes: ByteString,
      contractIdVersion: CantonContractIdVersion,
      hmacOps: HmacOps,
  ): Salt =
    deriveSalt(seed.forHashing, bytes, hmacOps).valueOr(err =>
      throw new IllegalStateException(err.toString)
    )

  def fromProtoV30(saltP: v30.Salt): ParsingResult[Salt] =
    for {
      saltAlgorithm <- SaltAlgorithm.fromProtoOneOf("algorithm", saltP.algorithm)
      salt <- create(saltP.salt, saltAlgorithm).leftMap(err =>
        ProtoDeserializationError.CryptoDeserializationError(
          DefaultDeserializationError(err.toString)
        )
      )
    } yield salt

  object TransformerImplicits {
    implicit val v30ToAdminV30AlgorithmTransformer
        : Transformer[crypto.v30.Salt.Algorithm, admin.crypto.v30.Salt.Algorithm] = {
      case crypto.v30.Salt.Algorithm.Empty => admin.crypto.v30.Salt.Algorithm.Empty
      case crypto.v30.Salt.Algorithm.Hmac(value) =>
        val hmac = value match {
          case crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_UNSPECIFIED =>
            admin.crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_UNSPECIFIED
          case crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_HMAC_SHA256 =>
            admin.crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_HMAC_SHA256
          case crypto.v30.HmacAlgorithm.Unrecognized(unrecognizedValue) =>
            admin.crypto.v30.HmacAlgorithm.Unrecognized(unrecognizedValue)
        }

        admin.crypto.v30.Salt.Algorithm.Hmac(hmac)
    }

    implicit val adminV30Tov30AlgorithmTransformer
        : Transformer[admin.crypto.v30.Salt.Algorithm, crypto.v30.Salt.Algorithm] = {
      case admin.crypto.v30.Salt.Algorithm.Empty => crypto.v30.Salt.Algorithm.Empty
      case admin.crypto.v30.Salt.Algorithm.Hmac(value) =>
        val hmac = value match {
          case admin.crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_UNSPECIFIED =>
            crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_UNSPECIFIED
          case admin.crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_HMAC_SHA256 =>
            crypto.v30.HmacAlgorithm.HMAC_ALGORITHM_HMAC_SHA256
          case admin.crypto.v30.HmacAlgorithm.Unrecognized(unrecognizedValue) =>
            crypto.v30.HmacAlgorithm.Unrecognized(unrecognizedValue)
        }

        crypto.v30.Salt.Algorithm.Hmac(hmac)
    }
  }

}

sealed trait SaltError extends Product with Serializable with PrettyPrinting

object SaltError {
  final case class InvalidSaltCreation(bytes: ByteString, algorithm: SaltAlgorithm)
      extends SaltError {
    override def pretty: Pretty[InvalidSaltCreation] =
      prettyOfClass(
        param("bytes", _.bytes),
        param("algorithm", _.algorithm),
      )
  }

  final case class HmacGenerationError(error: HmacError) extends SaltError {
    override def pretty: Pretty[HmacGenerationError] = prettyOfClass(
      param("error", _.error)
    )
  }
}
