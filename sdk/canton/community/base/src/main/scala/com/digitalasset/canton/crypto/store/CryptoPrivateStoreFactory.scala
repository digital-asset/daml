// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.kms.Kms
import com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggerFactory}
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

class CryptoPrivateStoreFactory(
    cryptoProvider: CryptoProvider,
    kmsStoreCacheConfig: CacheConfig,
    privateKeyStoreConfig: PrivateKeyStoreConfig,
    replicaManager: Option[ReplicaManager],
) extends EncryptedCryptoPrivateStoreHelper
    with HasLoggerName {

  private def createInternal(
      storage: Storage,
      kmsO: Option[Kms],
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] =
    cryptoProvider match {
      case CryptoProvider.Kms =>
        kmsO match {
          case Some(kms) =>
            EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
              KmsCryptoPrivateStore.create(
                storage,
                kms,
                kmsStoreCacheConfig,
                timeouts,
                loggerFactory,
              )
            )
          case None =>
            EitherT.leftT[FutureUnlessShutdown, CryptoPrivateStore](
              CryptoPrivateStoreError.KmsPrivateStoreError("no KMS client")
            )
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
                    kms <- kmsO
                      .toRight(
                        CryptoPrivateStoreError.EncryptedPrivateStoreError(
                          "Missing KMS " +
                            "instance"
                        )
                      )
                      .toEitherT[FutureUnlessShutdown]
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
      // TODO(#28252): Refactor and remove CryptoPrivateStoreFactory.
      kmsO: Option[Kms],
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] =
    createInternal(storage, kmsO, releaseProtocolVersion, timeouts, loggerFactory)
      .map {
        case encryptedPrivateStore: EncryptedCryptoPrivateStore =>
          // Register the encrypted private store with the replica manager to refresh the in-memory caches on failover
          replicaManager.foreach(_.setPrivateKeyStore(encryptedPrivateStore))
          encryptedPrivateStore

        case store => store
      }

}

object CryptoPrivateStoreFactory {

  /** A simple version of a crypto private store factory that does not use a KMS for testing.
    * TODO(#28252): Refactor and remove CryptoPrivateStoreFactory.
    */
  @VisibleForTesting
  def withoutKms(): CryptoPrivateStoreFactory =
    new CryptoPrivateStoreFactory(
      cryptoProvider = CryptoProvider.Jce,
      kmsStoreCacheConfig = CachingConfigs.kmsMetadataCache,
      privateKeyStoreConfig = PrivateKeyStoreConfig(),
      replicaManager = None,
    )

}
