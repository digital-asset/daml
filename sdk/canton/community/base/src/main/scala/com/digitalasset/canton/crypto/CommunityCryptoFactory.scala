// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  CommunityCryptoConfig,
  CryptoConfig,
  CryptoProvider,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.kms.CommunityKms
import com.digitalasset.canton.crypto.provider.jce.JceCrypto
import com.digitalasset.canton.crypto.provider.kms.{CommunityKmsPrivateCrypto, KmsCrypto}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.store.KmsCryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.version.ReleaseProtocolVersion

import scala.concurrent.ExecutionContext

class CommunityCryptoFactory extends CryptoFactory {

  def create(
      config: CryptoConfig,
      storage: Storage,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      releaseProtocolVersion: ReleaseProtocolVersion,
      nonStandardConfig: Boolean,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      executionContext: ExecutionContext,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Crypto] =
    for {
      communityConfig <- config match {
        case conf: CommunityCryptoConfig => EitherT.rightT[FutureUnlessShutdown, String](conf)
        case _ =>
          EitherT.leftT[FutureUnlessShutdown, CommunityCryptoConfig]("Invalid crypto config type")
      }
      storesAndSchemes <- initStoresAndSelectSchemes(
        config,
        storage,
        cryptoPrivateStoreFactory,
        releaseProtocolVersion,
        timeouts,
        loggerFactory,
        tracerProvider,
      )
      crypto <- communityConfig.provider match {
        case CryptoProvider.Jce =>
          JceCrypto
            .create(config, storesAndSchemes, timeouts, loggerFactory)
            .mapK(FutureUnlessShutdown.outcomeK)
        case CryptoProvider.Kms =>
          EitherT.fromEither[FutureUnlessShutdown] {
            for {
              kmsConfig <- communityConfig.kms.toRight(
                "Missing KMS configuration for KMS crypto provider"
              )
              kms <- CommunityKms
                .create(
                  kmsConfig,
                  timeouts,
                  futureSupervisor,
                  clock,
                  loggerFactory,
                  executionContext,
                )
                .leftMap(err => s"Failed to create the KMS client: $err")
              kmsCryptoPrivateStore <- KmsCryptoPrivateStore.fromCryptoPrivateStore(
                storesAndSchemes.cryptoPrivateStore
              )
              kmsPrivateCrypto <- CommunityKmsPrivateCrypto.create(
                kms,
                storesAndSchemes,
                kmsCryptoPrivateStore,
                timeouts,
                loggerFactory,
              )
              kmsCrypto <- KmsCrypto.create(
                config,
                storesAndSchemes,
                kmsPrivateCrypto,
                timeouts,
                loggerFactory,
              )
            } yield kmsCrypto
          }
      }
    } yield crypto

}
