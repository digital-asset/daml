// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.crypto.EncryptionAlgorithmSpec.RsaOaepSha256
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.SymmetricKeyScheme.Aes128Gcm
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.crypto.{
  CryptoPrivateApi,
  Hash,
  PbkdfScheme,
  Signature,
  SignatureCheckError,
  SigningKeyUsage,
  SyncCryptoError,
  SynchronizerCryptoPureApi,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Aggregates all methods related to protocol signing and signature verification. These methods
  * require a topology snapshot to ensure the correct signing keys are used, based on the current
  * state (i.e., OwnerToKeyMappings).
  */
trait SyncCryptoSigner {

  /** Signs a given hash using the currently active signing keys in the current topology state.
    */
  def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature]

  /** Verify a given signature using the currently active signing keys in the current topology
    * state.
    */
  def verifySignature(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  /** Verifies multiple signatures using the currently active signing keys in the current topology
    * state.
    */
  def verifySignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signer: Member,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

  /** Verifies multiple group signatures using the currently active signing keys of the different
    * signers in the current topology state.
    *
    * @param threshold
    *   the number of valid signatures required for the overall verification to be considered
    *   correct.
    */
  def verifyGroupSignatures(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      signers: Seq[Member],
      threshold: PositiveInt,
      groupName: String,
      signatures: NonEmpty[Seq[Signature]],
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, SignatureCheckError, Unit]

}

object SyncCryptoSigner {

  def create(
      staticSynchronizerParameters: StaticSynchronizerParameters,
      member: Member,
      pureCrypto: SynchronizerCryptoPureApi,
      privateCrypto: CryptoPrivateApi,
      cryptoPrivateStore: CryptoPrivateStore,
      verificationParallelismLimit: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) =
    new SyncCryptoSignerDefault(
      member,
      new SynchronizerCryptoPureApi(staticSynchronizerParameters, pureCrypto),
      privateCrypto,
      cryptoPrivateStore,
      verificationParallelismLimit,
      loggerFactory,
    )

  def createWithOptionalSessionKeys(
      staticSynchronizerParameters: StaticSynchronizerParameters,
      member: Member,
      pureCrypto: SynchronizerCryptoPureApi,
      privateCrypto: CryptoPrivateApi,
      cryptoPrivateStore: CryptoPrivateStore,
      sessionSigningKeysConfig: SessionSigningKeysConfig,
      verificationParallelismLimit: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SyncCryptoSigner =
    if (sessionSigningKeysConfig.enabled) {
      // Except for the signing algorithm and key specifications, all other schemes are not needed for
      // the ProtocolSigner. Therefore, we use fixed schemes (i.e. placeholders) for the other crypto parameters.
      // TODO(#23731): Split up pure crypto into smaller modules and only use the signing module here
      val pureCryptoForSessionKeys = new JcePureCrypto(
        Aes128Gcm,
        sessionSigningKeysConfig.signingAlgorithmSpec,
        pureCrypto.supportedSigningAlgorithmSpecs,
        RsaOaepSha256,
        NonEmpty.mk(Set, RsaOaepSha256),
        Sha256,
        PbkdfScheme.Argon2idMode1,
        loggerFactory,
      )

      new SyncCryptoSignerWithSessionKeys(
        new SynchronizerCryptoPureApi(staticSynchronizerParameters, pureCryptoForSessionKeys),
        verificationParallelismLimit,
      )
    } else
      SyncCryptoSigner.create(
        staticSynchronizerParameters,
        member,
        pureCrypto,
        privateCrypto,
        cryptoPrivateStore,
        verificationParallelismLimit,
        loggerFactory,
      )

}
