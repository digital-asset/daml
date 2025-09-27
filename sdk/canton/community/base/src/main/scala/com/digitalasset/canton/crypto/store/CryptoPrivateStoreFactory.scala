// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.kms.{Kms, KmsFactory}
import com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggerFactory}
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

class CryptoPrivateStoreFactory(
    cryptoProvider: CryptoProvider,
    kmsConfigO: Option[KmsConfig],
    kmsStoreCacheConfig: CacheConfig,
    privateKeyStoreConfig: PrivateKeyStoreConfig,
    replicaManager: Option[ReplicaManager],
    futureSupervisor: FutureSupervisor,
    clock: Clock,
    executionContext: ExecutionContext,
) extends EncryptedCryptoPrivateStoreHelper
    with HasLoggerName {

  private def createKms(
      errFn: String => CryptoPrivateStoreError,
      timeouts: ProcessingTimeout,
      tracerProvider: TracerProvider,
      loggerFactory: NamedLoggerFactory,
  ): Either[CryptoPrivateStoreError, Kms] = for {
    kmsConfig <- kmsConfigO.toRight(
      errFn(
        "Missing KMS configuration for KMS crypto provider"
      )
    )
    kms <- KmsFactory
      .create(
        kmsConfig,
        timeouts,
        futureSupervisor,
        tracerProvider,
        clock,
        loggerFactory,
        executionContext,
      )
      .leftMap(err => errFn(s"Failed to create KMS client: $err"))
  } yield kms

  private def createInternal(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] =
    cryptoProvider match {
      case CryptoProvider.Kms =>
        EitherT.fromEither[FutureUnlessShutdown] {
          createKms(
            CryptoPrivateStoreError.KmsPrivateStoreError.apply,
            timeouts,
            tracerProvider,
            loggerFactory,
          ).map { kms =>
            KmsCryptoPrivateStore
              .create(storage, kms, kmsStoreCacheConfig, timeouts, loggerFactory)
          }
        }
      case CryptoProvider.Jce =>
        for {
          store <- storage match {
            case jdbc: DbStorage =>
              val dbCryptoPrivateStore = new DbCryptoPrivateStore(
                jdbc,
                releaseProtocolVersion,
                timeouts,
                loggerFactory,
              )
              // check if encryption is enabled
              privateKeyStoreConfig.encryption match {
                case Some(EncryptedPrivateStoreConfig.Kms(kmsKeyId, reverted)) =>
                  for {
                    kms <- createKms(
                      CryptoPrivateStoreError.EncryptedPrivateStoreError.apply,
                      timeouts,
                      tracerProvider,
                      loggerFactory,
                    ).toEitherT[FutureUnlessShutdown]
                    store <- EncryptedCryptoPrivateStore
                      .create(
                        storage,
                        dbCryptoPrivateStore,
                        kms,
                        kmsKeyId,
                        reverted,
                        releaseProtocolVersion,
                        timeouts,
                        loggerFactory,
                      )
                  } yield store
                case None =>
                  EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
                    dbCryptoPrivateStore
                  )
              }
            case _: MemoryStorage =>
              EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
                new InMemoryCryptoPrivateStore(releaseProtocolVersion, loggerFactory)
              )
          }
          _ <- store.toExtended match {
            case Some(extendedStore) => extendedStore.migratePrivateKeys(storage.isActive, timeouts)
            case None => EitherT.pure[FutureUnlessShutdown, CryptoPrivateStoreError](())
          }
        } yield store
    }

  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      tracerProvider: TracerProvider,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] =
    createInternal(storage, releaseProtocolVersion, timeouts, loggerFactory, tracerProvider).map {
      case encryptedPrivateStore: EncryptedCryptoPrivateStore =>
        // Register the encrypted private store with the replica manager to refresh the in-memory caches on failover
        replicaManager.foreach(_.setPrivateKeyStore(encryptedPrivateStore))
        encryptedPrivateStore

      case store => store
    }

}

object CryptoPrivateStoreFactory {

  /** A simple version of a crypto private store factory that does not use a KMS for testing. */
  @VisibleForTesting
  def withoutKms(clock: Clock, executionContext: ExecutionContext): CryptoPrivateStoreFactory =
    new CryptoPrivateStoreFactory(
      cryptoProvider = CryptoProvider.Jce,
      kmsConfigO = None,
      kmsStoreCacheConfig = CachingConfigs.kmsMetadataCache,
      privateKeyStoreConfig = PrivateKeyStoreConfig(),
      replicaManager = None,
      futureSupervisor = FutureSupervisor.Noop,
      clock = clock,
      executionContext = executionContext,
    )

}
