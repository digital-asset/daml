// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CacheConfig, CryptoConfig, CryptoProvider, ProcessingTimeout}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.crypto.{
  Hash,
  KeyPurpose,
  PublicKey,
  Signature,
  SigningKeyUsage,
  SigningPublicKey,
  SyncCryptoError,
  SynchronizerCrypto,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{Member, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Aggregates all methods related to protocol signing. These methods require a topology snapshot to
  * ensure the correct signing keys are used, based on the current state (i.e., OwnerToKeyMappings).
  */
trait SyncCryptoSigner extends NamedLogging with AutoCloseable {

  protected def cryptoPrivateStore: CryptoPrivateStore

  /** Signs a given hash using the currently active signing keys in the current topology state.
    */
  def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature]

  protected def findSigningKey(
      member: Member,
      topologySnapshot: TopologySnapshot,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, SigningPublicKey] =
    for {
      signingKeys <- EitherT.right(topologySnapshot.signingKeys(member, usage))
      existingKeys <- signingKeys.toList
        .parFilterA(pk => cryptoPrivateStore.existsSigningKey(pk.fingerprint))
        .leftMap[SyncCryptoError](SyncCryptoError.StoreError.apply)
      kk <- NonEmpty
        .from(existingKeys)
        .map(PublicKey.getLatestKey)
        .toRight[SyncCryptoError](
          SyncCryptoError
            .KeyNotAvailable(
              member,
              KeyPurpose.Signing,
              topologySnapshot.timestamp,
              signingKeys.map(_.fingerprint),
            )
        )
        .toEitherT[FutureUnlessShutdown]
    } yield kk

}

object SyncCryptoSigner {

  def createWithLongTermKeys(
      member: Member,
      crypto: SynchronizerCrypto,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) =
    new SyncCryptoSignerWithLongTermKeys(
      member,
      crypto.privateCrypto,
      crypto.cryptoPrivateStore,
      loggerFactory,
    )

  def createWithOptionalSessionKeys(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      member: Member,
      crypto: SynchronizerCrypto,
      cryptoConfig: CryptoConfig,
      publicKeyConversionCacheConfig: CacheConfig,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SyncCryptoSigner =
    cryptoConfig.kms.map(_.sessionSigningKeys) match {
      // session signing keys can only be used if we are directly storing all our private keys in an external KMS
      case Some(sessionSigningKeysConfig)
          if cryptoConfig.provider == CryptoProvider.Kms &&
            cryptoConfig.privateKeyStore.encryption.isEmpty &&
            sessionSigningKeysConfig.enabled =>
        new SyncCryptoSignerWithSessionKeys(
          synchronizerId,
          staticSynchronizerParameters,
          member,
          crypto.privateCrypto,
          crypto.cryptoPrivateStore,
          sessionSigningKeysConfig,
          publicKeyConversionCacheConfig,
          futureSupervisor: FutureSupervisor,
          timeouts,
          loggerFactory,
        )
      case _ =>
        SyncCryptoSigner.createWithLongTermKeys(
          member,
          crypto,
          loggerFactory,
        )
    }

}
