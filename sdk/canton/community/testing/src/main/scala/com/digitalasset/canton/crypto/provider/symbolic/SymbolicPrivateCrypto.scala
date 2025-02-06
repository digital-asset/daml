// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.ExecutionContext

class SymbolicPrivateCrypto(
    pureCrypto: SymbolicPureCrypto,
    override val store: CryptoPrivateStoreExtended,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(
    override implicit val ec: ExecutionContext
) extends CryptoPrivateStoreApi
    with NamedLogging {

  private val keyCounter = new AtomicInteger

  private val randomKeys = new AtomicBoolean(false)

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  // NOTE: These schemes are not really used by Symbolic crypto
  override val defaultSigningAlgorithmSpec: SigningAlgorithmSpec = SigningAlgorithmSpec.Ed25519
  override val defaultSigningKeySpec: SigningKeySpec = SigningKeySpec.EcCurve25519
  override val defaultEncryptionKeySpec: EncryptionKeySpec = EncryptionKeySpec.EcP256

  @VisibleForTesting
  def setRandomKeysFlag(newValue: Boolean): Unit =
    randomKeys.set(newValue)

  private def genKeyPair[K](keypair: (ByteString, ByteString) => K): K = {
    val key = if (randomKeys.get()) {
      PseudoRandom.randomAlphaNumericString(8)
    } else {
      s"key-${keyCounter.incrementAndGet()}"
    }
    val publicKey = ByteString.copyFromUtf8(s"pub-$key")
    val privateKey = ByteString.copyFromUtf8(s"priv-$key")
    keypair(publicKey, privateKey)
  }

  override protected[crypto] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.All,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningKeyPair] =
    genKeyPair((pubKey, privKey) =>
      SigningKeyPair.create(
        CryptoKeyFormat.Symbolic,
        pubKey,
        CryptoKeyFormat.Symbolic,
        privKey,
        keySpec,
        usage,
      )
    ).toEitherT[FutureUnlessShutdown]

  override protected[crypto] def generateEncryptionKeypair(keySpec: EncryptionKeySpec)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionKeyPair] =
    EitherT.rightT(
      genKeyPair((pubKey, privKey) =>
        EncryptionKeyPair.create(
          CryptoKeyFormat.Symbolic,
          pubKey,
          CryptoKeyFormat.Symbolic,
          privKey,
          keySpec,
        )
      )
    )

  override def name: String = "symbolic-private-crypto"

  override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
}
