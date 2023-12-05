// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.deterministic.encryption

import com.digitalasset.canton.crypto.{Fingerprint, SecureRandomness}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.google.protobuf.ByteString
import org.bouncycastle.crypto.digests.SHA512Digest
import org.bouncycastle.crypto.prng.drbg.HashSP800DRBG
import org.bouncycastle.crypto.prng.{EntropySource, EntropyUtil}

import java.security.{MessageDigest, NoSuchAlgorithmException, SecureRandom}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.blocking

/** A SP800-90A Hash (SHA512) Deterministic Random Bit Generator (DRBG). A DRBG mechanism uses
  * an algorithm (i.e., the 'hash' algorithm) that produces a sequence
  * of bits from an initial value that is determined by a seed that is generated
  * from the output of an entropy source. Once the seed is provided and the initial value is
  * determined, the DRBG is said to be instantiated and may be used to produce output.
  * We set the security strength for the underlying DRGB algorithm to 256 bits, similarly to what is defined in
  * [[org.bouncycastle.crypto.prng.SP800SecureRandomBuilder]].
  * This implementation does not make use of a personalization string or additional input (i.e., both empty)
  * and we disabled prediction resistance (= false).
  *
  * The code is based on [[org.bouncycastle.crypto.prng.SP800SecureRandom]] and
  * [[org.bouncycastle.crypto.prng.drbg.HashSP800DRBG]].
  *
  * @param entropySource the entropy source to be used. We expect the entropy to
  *                      be the hash of the message we want to later encrypt, but we can also feed
  *                      other random sources of entropy. The entropy size must be
  *                      chosen based on the security strength of the underlying DRGB.
  */
private class SP800HashDRBGSecureRandom(entropySource: EntropySource) extends SecureRandom {

  // we use the same security strength (in bits) as defined in [[org.bouncycastle.crypto.prng.SP800SecureRandomBuilder]]
  private val securityStrength = 256

  // we use the same digest function as in [[org.bouncycastle.jcajce.provider.drbg.DRBG]]
  private val digest = new SHA512Digest()

  /* Reseeding is not supported as this implementation does not come with a randomness source.
   * Therefore, it does not provide prediction resistance.
   */
  val predictionResistant = false

  private val hashSP800DRBG: HashSP800DRBG =
    new HashSP800DRBG(digest, securityStrength, entropySource, Array[Byte](), Array[Byte]())

  /* All the implementations of getAlgorithm(), setSeed(), generateSeed() and nextBytes() are based on
   * [[org.bouncycastle.crypto.prng.SP800SecureRandom]] using a custom entropy source and no randomness source.
   */

  override def setSeed(seed: Array[Byte]): Unit = ()
  override def getAlgorithm: String = "HASH-DRBG-" ++ digest.getAlgorithmName
  override def generateSeed(numBytes: Int): Array[Byte] =
    EntropyUtil.generateSeed(entropySource, numBytes)
  override def nextBytes(bytes: Array[Byte]): Unit = {
    blocking(
      this.synchronized {
        if (hashSP800DRBG.generate(bytes, Array[Byte](), predictionResistant) < 0) {
          hashSP800DRBG.reseed(Array[Byte]())
          hashSP800DRBG.generate(bytes, Array[Byte](), predictionResistant)
          ()
        }
      }
    )
  }
}

private object SP800HashDRBGSecureRandom {
  def apply(
      message: Array[Byte],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      loggingContext: ErrorLoggingContext,
  ): SP800HashDRBGSecureRandom = {

    /* [org.bouncycastle.crypto.prng.SP800SecureRandomBuilder]] defines entropyBitsRequired = 256.
     * Therefore, we opted to use SHA-256 since it returns a 256-bit hash. This also conforms with the underlying DRGB
     * security strength of 256 bits.
     */
    val digest: MessageDigest =
      try {
        MessageDigest.getInstance("SHA-256")
      } catch {
        case exc: NoSuchAlgorithmException =>
          ErrorUtil.internalError(
            new IllegalStateException("SHA-256 not available.", exc)
          )
      }

    new SP800HashDRBGSecureRandom(
      new EntropySourceFromInput(
        SecureRandomness(ByteString.copyFrom(digest.digest(message))),
        loggerFactory,
      )
    )
  }
}

/** Custom entropy source where the entropy bits are directly passed as a parameter.
  * We intend to use it to set our DBRG entropy to be the hash of a message (i.e., data we want to encrypt) + publicKey.
  *
  * VERY IMPORTANT: we assume the input entropy to be high and to be only used once, otherwise the
  * pseudo randomness guarantees might not hold.
  *
  * @param entropy a byte string originated from a securely generated random value
  */
private class EntropySourceFromInput(
    val entropy: SecureRandomness,
    override val loggerFactory: NamedLoggerFactory,
)(implicit traceContext: TraceContext)
    extends EntropySource
    with NamedLogging {

  private val scheduled = new AtomicBoolean(false)

  override def isPredictionResistant: Boolean = false

  /* Directly sets the entropy to be the bytes passed as input to the class.
   * In case of multiple calls it will throw a 'CATASTROPHIC_ERROR_FLAG' error.
   */
  override def getEntropy: Array[Byte] =
    if (!scheduled.getAndSet(true)) {
      entropy.unwrap.toByteArray
    } else {
      ErrorUtil.internalError(
        new IllegalStateException("Multiple request to getEntropy")
      )
    }

  // entropy size in bits
  override def entropySize(): Int = entropy.unwrap.size() * 8

}

/** Deterministic random generator, MUST NOT be used for security-relevant operations EXCEPT when the use of
  * deterministic crypto is deemed secure (e.g. during the encryption of the views to ensure
  * transparency).
  */
object DeterministicRandom {
  def getDeterministicRandomGenerator(
      message: ByteString,
      publicKey: Fingerprint,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      loggingContext: ErrorLoggingContext,
  ): SecureRandom = {
    // the source of randomness must be the hash over both the public key and the message
    SP800HashDRBGSecureRandom(
      DeterministicEncoding
        .encodeBytes(message)
        .concat(ByteString.copyFromUtf8(publicKey.toLengthLimitedString.str))
        .toByteArray,
      loggerFactory,
    )
  }
}
