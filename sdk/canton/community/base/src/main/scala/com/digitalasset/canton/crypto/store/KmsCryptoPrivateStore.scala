// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.SigningKeyUsage.nonEmptyIntersection
import com.digitalasset.canton.crypto.kms.{Kms, KmsKeyId}
import com.digitalasset.canton.crypto.store.KmsMetadataStore.KmsMetadata
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.HasReadWriteLock
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

class KmsCryptoPrivateStore(
    kms: Kms,
    metadataStore: KmsMetadataStore,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends CryptoPrivateStore
    with NamedLogging
    with HasReadWriteLock {

  private def getKeyMetadataInternal(
      keyId: Fingerprint
  )(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, KmsMetadata] =
    OptionT(metadataStore.get(keyId))

  protected[crypto] def getKeyMetadata(
      keyId: Fingerprint
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[KmsMetadata]] =
    withReadLock {
      getKeyMetadataInternal(keyId).value
    }

  private def listKeysMetadata(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[List[KmsMetadata]] =
    withReadLock {
      metadataStore.list()
    }

  protected[crypto] def storeKeyMetadata(
      keyMetadata: KmsMetadata
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    withWriteLock {
      metadataStore.store(keyMetadata)
    }

  @VisibleForTesting
  protected[canton] def listAllKmsKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[KmsKeyId]] =
    listKeysMetadata.map(_.map(_.kmsKeyId).toSet)

  def removePrivateKey(
      keyId: Fingerprint
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    withWriteLock {
      for {
        keyMetadata <- getKeyMetadataInternal(keyId)
          .toRight(
            CryptoPrivateStoreError
              .FailedToDeleteKey(keyId, s"could not find private key's metadata")
          )
        _ <- EitherT.right(metadataStore.delete(keyId))
        _ <- kms
          .deleteKey(keyMetadata.kmsKeyId)
          .leftMap[CryptoPrivateStoreError](err =>
            CryptoPrivateStoreError
              .FailedToDeleteKey(
                keyId,
                s"failed to delete KMS key from external KMS ${err.show}",
              )
          )
      } yield ()
    }

  override def existsPrivateKey(
      keyId: Fingerprint,
      purpose: KeyPurpose,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] =
    EitherT.right {
      withReadLock {
        getKeyMetadataInternal(keyId).exists(_.purpose == purpose)
      }
    }

  /** Filter signing keys by usage, using the metadata stored in the private store.
    */
  override def filterSigningKeys(
      signingKeyIds: Seq[Fingerprint],
      filterUsage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Seq[Fingerprint]] =
    for {
      signingKeys <- EitherT.right {
        signingKeyIds.parTraverseFilter(signingKeyId =>
          withReadLock {
            getKeyMetadataInternal(signingKeyId).value
          }
        )
      }
      filteredSigningKeys =
        signingKeys
          .filter { metadata =>
            val metadataUsage = metadata.usage match {
              case Some(m) => m
              // To maintain backwards compatibility, we update old signing keys without a specified
              // usage to default to 'All' usages.
              case None => SigningKeyUsage.All
            }
            if (nonEmptyIntersection(metadataUsage, filterUsage)) true
            else false

          }
    } yield filteredSigningKeys.map(_.fingerprint)

  override def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] =
    existsPrivateKey(signingKeyId, KeyPurpose.Signing)

  override def existsDecryptionKey(decryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] =
    existsPrivateKey(decryptionKeyId, KeyPurpose.Encryption)

  override def queryKmsKeyId(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[String300]] =
    EitherT
      .right[CryptoPrivateStoreError] {
        getKeyMetadata(keyId)
      }
      .map(_.map(_.kmsKeyId.str))

  override def close(): Unit = LifeCycle.close(kms)(logger)
}

object KmsCryptoPrivateStore {

  def create(
      storage: Storage,
      kms: Kms,
      kmsCacheConfig: CacheConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): CryptoPrivateStore =
    new KmsCryptoPrivateStore(
      kms,
      KmsMetadataStore.create(storage, kmsCacheConfig, timeouts, loggerFactory),
      loggerFactory,
    )

  def fromCryptoPrivateStore(
      cryptoPrivateStore: CryptoPrivateStore
  ): Either[String, KmsCryptoPrivateStore] =
    cryptoPrivateStore match {
      case store: KmsCryptoPrivateStore => Right(store)
      case _ =>
        Left(
          s"The crypto private store is not set as an external KMS crypto private store"
        )
    }
}
