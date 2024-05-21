// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.data.EitherT
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.memory.{
  InMemoryCryptoPrivateStore,
  InMemoryCryptoPublicStore,
}
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, CryptoPublicStore}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

class SymbolicCrypto(
    pureCrypto: SymbolicPureCrypto,
    privateCrypto: SymbolicPrivateCrypto,
    cryptoPrivateStore: CryptoPrivateStore,
    cryptoPublicStore: CryptoPublicStore,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends Crypto(
      pureCrypto,
      privateCrypto,
      cryptoPrivateStore,
      cryptoPublicStore,
      timeouts,
      loggerFactory,
    ) {

  private def process[E, A](
      description: String
  )(fn: TraceContext => EitherT[FutureUnlessShutdown, E, A]): A = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      timeouts.default.await(description) {
        fn(traceContext)
          .valueOr(err => sys.error(s"Failed operation $description: $err"))
          .onShutdown(sys.error("aborted due to shutdown"))
      }
    }
  }

  def getOrGenerateSymbolicSigningKey(name: String): SigningPublicKey = {
    process("get or generate symbolic signing key") { implicit traceContext =>
      cryptoPublicStore
        .findSigningKeyIdByName(KeyName.tryCreate(name))
        .mapK(FutureUnlessShutdown.outcomeK)
    }.getOrElse(generateSymbolicSigningKey(Some(name)))
  }

  def getOrGenerateSymbolicEncryptionKey(name: String): EncryptionPublicKey = {
    process("get or generate symbolic encryption key") { implicit traceContext =>
      cryptoPublicStore
        .findEncryptionKeyIdByName(KeyName.tryCreate(name))
        .mapK(FutureUnlessShutdown.outcomeK)
    }.getOrElse(generateSymbolicEncryptionKey(Some(name)))
  }

  /** Generates a new symbolic signing keypair and stores the public key in the public store */
  def generateSymbolicSigningKey(
      name: Option[String] = None
  ): SigningPublicKey = {
    process("generate symbolic signing key") { implicit traceContext =>
      // We don't care about the signing key scheme in symbolic crypto
      generateSigningKey(SigningKeyScheme.Ed25519, name.map(KeyName.tryCreate))
    }
  }

  /** Generates a new symbolic signing keypair but does not store it in the public store */
  def newSymbolicSigningKeyPair(): SigningKeyPair = {
    process("generate symbolic signing keypair") { implicit traceContext =>
      // We don't care about the signing key scheme in symbolic crypto
      privateCrypto
        .generateSigningKeypair(SigningKeyScheme.Ed25519)
        .mapK(FutureUnlessShutdown.outcomeK)
    }
  }

  def generateSymbolicEncryptionKey(
      name: Option[String] = None
  ): EncryptionPublicKey =
    process("generate symbolic encryption key") { implicit traceContext =>
      // We don't care about the encryption key scheme in symbolic crypto
      generateEncryptionKey(
        EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm,
        name.map(KeyName.tryCreate),
      )
    }

  def newSymbolicEncryptionKeyPair(): EncryptionKeyPair = {
    process("generate symbolic encryption keypair") { implicit traceContext =>
      // We don't care about the encryption key scheme in symbolic crypto
      privateCrypto
        .generateEncryptionKeypair(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm)
        .mapK(FutureUnlessShutdown.outcomeK)
    }
  }

  def sign(hash: Hash, signingKeyId: Fingerprint): Signature =
    process("symbolic signing") { implicit traceContext =>
      privateCrypto.sign(hash, signingKeyId)
    }

  def setRandomKeysFlag(newValue: Boolean): Unit = {
    privateCrypto.setRandomKeysFlag(newValue)
  }
}

object SymbolicCrypto {

  def signature(signature: ByteString, signedBy: Fingerprint): Signature =
    SymbolicPureCrypto.createSignature(signature, signedBy, 0xffffffff)

  def emptySignature: Signature =
    signature(ByteString.EMPTY, Fingerprint.create(ByteString.EMPTY))

  def create(
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): SymbolicCrypto = {
    implicit val ec: ExecutionContext =
      DirectExecutionContext(loggerFactory.getLogger(this.getClass))

    val pureCrypto = new SymbolicPureCrypto()
    val cryptoPublicStore = new InMemoryCryptoPublicStore
    val cryptoPrivateStore = new InMemoryCryptoPrivateStore(releaseProtocolVersion, loggerFactory)
    val privateCrypto = new SymbolicPrivateCrypto(pureCrypto, cryptoPrivateStore)

    new SymbolicCrypto(
      pureCrypto,
      privateCrypto,
      cryptoPrivateStore,
      cryptoPublicStore,
      timeouts,
      loggerFactory,
    )
  }

}
