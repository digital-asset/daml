// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}

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

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  // NOTE: These schemes are not really used by Symbolic crypto
  override val defaultSigningKeyScheme: SigningKeyScheme = SigningKeyScheme.Ed25519
  override val defaultEncryptionKeyScheme: EncryptionKeyScheme =
    EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm

  private def genKeyPair[K](keypair: (Fingerprint, ByteString, ByteString) => K): K = {
    val key = s"key-${keyCounter.incrementAndGet()}"
    val id = Fingerprint.create(ByteString.copyFromUtf8(key))
    val publicKey = ByteString.copyFromUtf8(s"pub-$key")
    val privateKey = ByteString.copyFromUtf8(s"priv-$key")
    keypair(id, publicKey, privateKey)
  }

  override protected[crypto] def generateSigningKeypair(scheme: SigningKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningKeyPair] =
    EitherT.rightT(
      genKeyPair((id, pubKey, privKey) =>
        SigningKeyPair.create(id, CryptoKeyFormat.Symbolic, pubKey, privKey, scheme)
      )
    )

  override protected[crypto] def generateEncryptionKeypair(scheme: EncryptionKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionKeyPair] =
    EitherT.rightT(
      genKeyPair((id, pubKey, privKey) =>
        EncryptionKeyPair.create(id, CryptoKeyFormat.Symbolic, pubKey, privKey, scheme)
      )
    )

  override def name: String = "symbolic-private-crypto"

  override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
}
