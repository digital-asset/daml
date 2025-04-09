// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.signer

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.SessionSigningKeysConfig
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.crypto.{
  CryptoPrivateApi,
  Hash,
  Signature,
  SigningKeyUsage,
  SyncCryptoError,
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
trait SyncCryptoSigner extends NamedLogging {

  /** Signs a given hash using the currently active signing keys in the current topology state.
    */
  def sign(
      topologySnapshot: TopologySnapshot,
      hash: Hash,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature]

}

object SyncCryptoSigner {

  def createWithLongTermKeys(
      member: Member,
      privateCrypto: CryptoPrivateApi,
      cryptoPrivateStore: CryptoPrivateStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) =
    new SyncCryptoSignerWithLongTermKeys(
      member,
      privateCrypto,
      cryptoPrivateStore,
      loggerFactory,
    )

  def createWithOptionalSessionKeys(
      synchronizerId: SynchronizerId,
      staticSynchronizerParameters: StaticSynchronizerParameters,
      member: Member,
      privateCrypto: CryptoPrivateApi,
      cryptoPrivateStore: CryptoPrivateStore,
      sessionSigningKeysConfig: SessionSigningKeysConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SyncCryptoSigner =
    if (sessionSigningKeysConfig.enabled) {
      new SyncCryptoSignerWithSessionKeys(
        synchronizerId,
        staticSynchronizerParameters,
        member,
        privateCrypto,
        sessionSigningKeysConfig,
        loggerFactory,
      )
    } else
      SyncCryptoSigner.createWithLongTermKeys(
        member,
        privateCrypto,
        cryptoPrivateStore,
        loggerFactory,
      )

}
