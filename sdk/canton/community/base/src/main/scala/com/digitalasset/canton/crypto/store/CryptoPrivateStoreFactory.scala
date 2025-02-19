// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  CachingConfigs,
  CryptoProvider,
  PrivateKeyStoreConfig,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

trait CryptoPrivateStoreFactory {
  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore]
}

object CryptoPrivateStoreFactory {

  /** A simple version of a crypto private store factory that does not use a KMS for testing. */
  @VisibleForTesting
  def withoutKms(clock: Clock, executionContext: ExecutionContext): CryptoPrivateStoreFactory =
    new CommunityCryptoPrivateStoreFactory(
      cryptoProvider = CryptoProvider.Jce,
      kmsConfigO = None,
      kmsFactory = CommunityKmsFactory,
      kmsStoreCacheConfig = CachingConfigs.kmsMetadataCache,
      privateKeyStoreConfig = PrivateKeyStoreConfig(),
      nonStandardConfig = false,
      futureSupervisor = FutureSupervisor.Noop,
      clock = clock,
      executionContext = executionContext,
    )

}
