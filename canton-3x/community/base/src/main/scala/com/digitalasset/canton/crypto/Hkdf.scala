// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** The expansion step of the HMAC-based key derivation function (HKDF) as defined in:
  * https://tools.ietf.org/html/rfc5869
  */
trait HkdfOps {
  this: HmacOps =>

  /** Sanity check the parameters to the HKDF before calling the internal implementations. */
  private def checkParameters(
      outputBytes: Int,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HkdfError, Unit] = {
    import HkdfError.*

    for {
      _ <- Either.cond(outputBytes >= 0, (), HkdfOutputNegative(outputBytes))
      hashBytes = algorithm.hashAlgorithm.length
      nrChunks = scala.math.ceil(outputBytes.toDouble / hashBytes).toInt
      _ <- Either
        .cond[HkdfError, Unit](
          nrChunks <= 255,
          (),
          HkdfOutputTooLong(length = outputBytes, maximum = hashBytes * 255),
        )
    } yield ()
  }

  protected def hkdfExpandInternal(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HkdfError, SecureRandomness]

  protected def computeHkdfInternal(
      keyMaterial: ByteString,
      outputBytes: Int,
      info: HkdfInfo,
      salt: ByteString = ByteString.EMPTY,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HkdfError, SecureRandomness]

  /** Produce a new secret from the given key material using the HKDF from RFC 5869 with both extract and expand phases.
    *
    * @param keyMaterial Input key material from which to derive another key.
    * @param outputBytes The length of the produced secret. May be at most 255 times the size of the output of the
    *                    selected hashing algorithm. If you need to derive multiple keys, set the `info` parameter
    *                    to different values, for each key that you need.
    * @param info        Specify the purpose of the derived key (optional). Note that you can derive multiple
    *                    independent keys from the same key material by varying the purpose.
    * @param salt        Optional salt. Should be set if the input key material is not cryptographically secure, uniformly random.
    * @param algorithm   The hash algorithm to be used for the HKDF construction
    */
  def computeHkdf(
      keyMaterial: ByteString,
      outputBytes: Int,
      info: HkdfInfo,
      salt: ByteString = ByteString.EMPTY,
      algorithm: HmacAlgorithm = defaultHmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] = for {
    _ <- checkParameters(outputBytes, algorithm)
    expansion <- computeHkdfInternal(keyMaterial, outputBytes, info, salt, algorithm)
  } yield expansion

}

/** Ensures unique values of "info" HKDF parameter for the different usages of the KDF. E.g., we may have
  * one purpose for deriving the encryption key for a view from a random value, and another one for deriving the random
  * values used for the subviews.
  */
class HkdfInfo private (val bytes: ByteString) extends AnyVal

object HkdfInfo {

  /** Use when deriving a view encryption key from randomness */
  val ViewKey = new HkdfInfo(ByteString.copyFromUtf8("view-key"))

  /** Use when deriving subview-randomness from the randomness used for a view */
  def subview(position: MerklePathElement) =
    new HkdfInfo(ByteString.copyFromUtf8("subview-").concat(position.encodeDeterministically))

  /** Use when deriving a session encryption key from randomness */
  val SessionKey = new HkdfInfo(ByteString.copyFromUtf8("session-key"))

  /** Used to specify arbitrary randomness for golden tests. Don't use in production! */
  @VisibleForTesting
  def testOnly(bytes: ByteString) = new HkdfInfo(bytes)
}

sealed trait HkdfError extends Product with Serializable with PrettyPrinting

object HkdfError {

  final case class HkdfOutputNegative(length: Int) extends HkdfError {
    override def pretty: Pretty[HkdfOutputNegative] = prettyOfClass(
      param("length", _.length)
    )
  }

  final case class HkdfOutputTooLong(length: Int, maximum: Long) extends HkdfError {
    override def pretty: Pretty[HkdfOutputTooLong] = prettyOfClass(
      param("length", _.length),
      param("maximum", _.maximum),
    )
  }

  final case class HkdfHmacError(error: HmacError) extends HkdfError {
    override def pretty: Pretty[HkdfHmacError] = prettyOfClass(
      unnamedParam(_.error)
    )
  }

  final case class HkdfInternalError(error: String) extends HkdfError {
    override def pretty: Pretty[HkdfInternalError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

}
