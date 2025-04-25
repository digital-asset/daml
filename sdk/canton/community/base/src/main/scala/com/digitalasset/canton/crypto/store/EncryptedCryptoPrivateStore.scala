// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.implicits.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.kms.{Kms, KmsKeyId}
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError.{
  EncryptedPrivateStoreError,
  WrapperKeyAlreadyInUse,
}
import com.digitalasset.canton.crypto.store.db.{DbCryptoPrivateStore, StoredPrivateKey}
import com.digitalasset.canton.crypto.{Fingerprint, KeyPurpose}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.canton.util.retry.{NoExceptionRetryPolicy, Success}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, StampedLockWithHandle, retry}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** This class wraps a CryptoPrivateStore and implements an encrypted version that stores the
  * private keys in encrypted form using a KMS
  */
class EncryptedCryptoPrivateStore(
    @VisibleForTesting
    private[canton] val store: DbCryptoPrivateStore,
    @VisibleForTesting
    private[canton] val kms: Kms,
    private val initialWrapperKeyId: KmsKeyId,
    override protected val releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreExtended
    with FlagCloseable
    with NamedLogging
    with EncryptedCryptoPrivateStoreHelper {

  private val lock = new StampedLockWithHandle()

  lazy private val wrapperKeyIdRef = new AtomicReference[KmsKeyId](initialWrapperKeyId)

  private[canton] def wrapperKeyId: KmsKeyId = wrapperKeyIdRef.get()

  private[crypto] def writePrivateKey(
      storedKey: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    lock.withWriteLock {
      for {
        encryptedKey <- encryptStoredKey(kms, wrapperKeyId, storedKey)
        _ <- store.writePrivateKey(encryptedKey)
      } yield ()
    }

  private[crypto] def readPrivateKey(keyId: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[StoredPrivateKey]] =
    readPrivateKeys(NonEmpty.mk(Seq, keyId), purpose).map(_.headOption)

  override private[crypto] def readPrivateKeys(
      keyIds: NonEmpty[Seq[Fingerprint]],
      purpose: KeyPurpose,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[StoredPrivateKey]] =
    for {
      readKeys <- store.readPrivateKeys(keyIds, purpose)
      decryptedKeys <- readKeys.map(decryptStoredKey(kms, _)).toSeq.sequence
      keys = decryptedKeys.toSet
    } yield keys

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[StoredPrivateKey]] =
    listPrivateKeys(purpose)

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[StoredPrivateKey]] =
    for {
      storedKeys <- store
        .listPrivateKeys(purpose, encrypted = true)
      keys <- storedKeys.toList.parTraverse(decryptStoredKey(kms, _))
    } yield keys.toSet

  private[crypto] def deletePrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    // this lock prevents concurrency issues between the deletion of a key and a wrapper key rotation
    // (i.e. a delete happens in between listing the keys and replacing them)
    lock.withWriteLock {
      store.deletePrivateKey(keyId)
    }

  private[crypto] def replaceStoredPrivateKeys(newKeys: Seq[StoredPrivateKey])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    lock.withWriteLock {
      replaceStoredPrivateKeysInternal(newKeys)
    }

  // NOTE: Needs to be called with appropriate wrapper key locking
  private def replaceStoredPrivateKeysInternal(newKeys: Seq[StoredPrivateKey])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    for {
      // step3: encrypt keys with new wrapper key
      encryptedKeys <- newKeys.parTraverse(encryptStoredKey(kms, wrapperKeyId, _))
      // step4: replace keys
      _ <- store.replaceStoredPrivateKeys(encryptedKeys)
    } yield ()

  private[crypto] def rotateWrapperKey(newWrapperKeyId: KmsKeyId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    if (newWrapperKeyId == wrapperKeyId)
      EitherT
        .leftT[FutureUnlessShutdown, Unit](
          WrapperKeyAlreadyInUse("wrapper key id for rotation is already in use")
        )
        .leftWiden[CryptoPrivateStoreError]
    else {
      // to prevent multiple replace db calls happening at the same time and to stop any write to the database
      // while the rotation is happening
      lock.withWriteLock {
        for {
          // step1: get all stored keys
          signingKeys <- store
            .listPrivateKeys(Signing, encrypted = true)
          encryptionKeys <- store
            .listPrivateKeys(Encryption, encrypted = true)
          allPrivateKeys = (signingKeys ++ encryptionKeys).toSeq
          // step2: decrypt all keys
          decryptedKeys <-
            allPrivateKeys.parTraverse(decryptStoredKey(kms, _))

          // step3: encrypt and replace keys
          oldWrapperKeyId = wrapperKeyIdRef.getAndSet(newWrapperKeyId)
          _ <- EitherTUtil.onErrorOrFailureUnlessShutdown(
            (_: Either[Throwable, CryptoPrivateStoreError]) => wrapperKeyIdRef.set(oldWrapperKeyId)
          ) {
            replaceStoredPrivateKeysInternal(decryptedKeys)
          }
        } yield ()
      }
    }

  private[canton] def refreshWrapperKey()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    lock.withWriteLock {
      store.getWrapperKeyId().map { keyIdO =>
        keyIdO.fold(ErrorUtil.invalidState(s"No private key store wrapper key id found"))(keyId =>
          wrapperKeyIdRef.set(KmsKeyId(keyId))
        )
      }
    }

  private[crypto] def encrypted(
      keyId: Fingerprint
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[String300]] =
    store.encrypted(keyId)

  override def onClosed(): Unit =
    LifeCycle.close(kms, store)(logger)
}

object EncryptedCryptoPrivateStore extends EncryptedCryptoPrivateStoreHelper with HasLoggerName {

  private def retryGetWrapperKeyId(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      timeouts: ProcessingTimeout,
      logger: NamedLoggingContext,
  )(implicit
      success: Success[Either[CryptoPrivateStoreError, Option[String300]]],
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[String300]] =
    EitherT(
      retry
        .Pause(
          logger.tracedLogger,
          FlagCloseable(logger.tracedLogger, timeouts),
          timeouts.activeInit.retries(50.millis),
          50.millis,
          functionFullName,
        )
        .unlessShutdown(dbCryptoPrivateStore.getWrapperKeyId().value, NoExceptionRetryPolicy)
    )

  private def createEncryptedPrivateStore(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
      wrapperKeyId: KmsKeyId,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): EncryptedCryptoPrivateStore =
    new EncryptedCryptoPrivateStore(
      dbCryptoPrivateStore,
      kms,
      wrapperKeyId,
      releaseProtocolVersion,
      timeouts,
      loggerFactory,
    )

  private def migrateToEncrypted(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
      wrapperKeyId: KmsKeyId,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    migrate(clearToEncrypted = true, dbCryptoPrivateStore, kms, Some(wrapperKeyId), loggerFactory)

  private def migrateToClear(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    migrate(clearToEncrypted = false, dbCryptoPrivateStore, kms, None, loggerFactory)

  private def migrate(
      clearToEncrypted: Boolean,
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
      wrapperKeyId: Option[KmsKeyId],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    for {
      storedDecryptionKeys <- dbCryptoPrivateStore
        .listPrivateKeys(
          Encryption,
          encrypted = !clearToEncrypted,
        )
      storedSigningKeys <- dbCryptoPrivateStore
        .listPrivateKeys(
          Signing,
          encrypted = !clearToEncrypted,
        )
      storedKeys = (storedDecryptionKeys ++ storedSigningKeys).toSeq
      logger = NamedLoggingContext(loggerFactory, traceContext)

      _ <-
        if (storedKeys.nonEmpty) {
          for {
            keysToReplace <-
              if (clearToEncrypted) {
                logger.debug(
                  show"storing the following keys ${storedKeys.map(_.id)} in encrypted form"
                )
                wrapperKeyId match {
                  case Some(keyId) => storedKeys.parTraverse(encryptStoredKey(kms, keyId, _))
                  case None =>
                    EitherT.leftT[FutureUnlessShutdown, Seq[StoredPrivateKey]](
                      EncryptedPrivateStoreError(
                        "no wrapper key specified for encryption during migration"
                      )
                    )
                }
              } else {
                logger.info(
                  show"decrypting and storing the following keys ${storedKeys.map(_.id)} in clear form"
                )
                storedKeys.parTraverse(decryptStoredKey(kms, _))
              }
            _ <- dbCryptoPrivateStore
              .replaceStoredPrivateKeys(keysToReplace)
          } yield ()
        } else EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](())

    } yield ()

  private def getFromStoreOrCreateNewKmsKey(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, KmsKeyId] =
    for {
      wrapperKeyId <- dbCryptoPrivateStore.getWrapperKeyId()
      res <- checkWrapperKeyExistsOrCreateNewOne(
        kms,
        wrapperKeyId.map(KmsKeyId.apply),
      )
        .leftMap[CryptoPrivateStoreError](kmsError => EncryptedPrivateStoreError(kmsError.show))
    } yield res

  // The active replica will set the wrapper key id and migrate to an encrypted private key store
  private[crypto] def activeReplicaInitEncryptedStore(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
      kmsKeyId: Option[KmsKeyId],
      logger: NamedLoggingContext,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, KmsKeyId] = {
    logger.debug("starting an encrypted crypto private key store")
    for {
      // get (or generate) a new kms key
      keyId <- kmsKeyId match {
        // if key is defined, and it exists in the KMS use that id
        case Some(keyId) =>
          kms
            .keyExistsAndIsActive(keyId)
            .leftMap[CryptoPrivateStoreError](err => EncryptedPrivateStoreError(err.show))
            .map(_ => keyId)
        // if key is not defined check if there is a wrapper_key_id in the store otherwise create a new key
        case None =>
          getFromStoreOrCreateNewKmsKey(dbCryptoPrivateStore, kms)
      }
      _ <- migrateToEncrypted(dbCryptoPrivateStore, kms, keyId, logger.loggerFactory)
    } yield keyId
  }

  // The active replica will revert back to a clear private key store by decrypting the keys
  private[canton] def activeReplicaRevertEncryptedStore(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
      logger: NamedLoggingContext,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] = {
    logger.info(
      "migrating back to a clear crypto private key store due to 'reverted' flag configured"
    )
    EncryptedCryptoPrivateStore.migrateToClear(
      dbCryptoPrivateStore,
      kms,
      logger.loggerFactory,
    )
  }

  // The passive replica will wait for the active replica to decrypt the crypto private key store
  private def passiveReplicaRevertEncryptedStore(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      timeouts: ProcessingTimeout,
      logger: NamedLoggingContext,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] = {
    // Retry until all wrapper key ids are unset or an error occurred
    implicit val success: Success[Either[CryptoPrivateStoreError, Option[String300]]] =
      Success {
        case Right(None) | Left(_) => true
        case _ => false
      }
    logger.debug(
      "passive replica waiting for encrypted private key store to be reverted"
    )
    retryGetWrapperKeyId(dbCryptoPrivateStore, timeouts, logger).flatMap {
      case Some(wrapperKeyId) =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          EncryptedPrivateStoreError(
            s"passive replica waiting for the decryption of the private keys, but wrapper key still set to $wrapperKeyId"
          )
        )
      case None => EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](())
    }
  }

  // The passive replica waits for the active replica to create a wrapper key and encrypt the crypto private key store
  private[crypto] def passiveReplicaInitEncryptedStore(
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      timeouts: ProcessingTimeout,
      logger: NamedLoggingContext,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, KmsKeyId] = {
    // Retry until all wrapper key ids are set or an error has occurred
    implicit val success: Success[Either[CryptoPrivateStoreError, Option[String300]]] =
      Success {
        case Right(Some(_)) | Left(_) => true
        case _ => false
      }
    logger.debug(
      "passive replica waiting for encrypted private key store to be initialized"
    )
    // wait for the wrapper key to be defined
    retryGetWrapperKeyId(dbCryptoPrivateStore, timeouts, logger).flatMap {
      case Some(wrapperKeyId) =>
        EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](KmsKeyId(wrapperKeyId))
      case None =>
        EitherT.leftT[FutureUnlessShutdown, KmsKeyId](
          EncryptedPrivateStoreError(
            "Active replica failed to initialize encrypted crypto private store"
          )
        )
    }
  }

  def create(
      storage: Storage,
      dbCryptoPrivateStore: DbCryptoPrivateStore,
      kms: Kms,
      kmsKeyId: Option[KmsKeyId],
      reverted: Boolean,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, CryptoPrivateStore] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val logger = NamedLoggingContext(loggerFactory, traceContext)
      (storage.isActive, reverted) match {
        case (false, false) =>
          passiveReplicaInitEncryptedStore(
            dbCryptoPrivateStore,
            timeouts,
            logger,
          )
            .map { wrapperKeyId =>
              createEncryptedPrivateStore(
                dbCryptoPrivateStore,
                kms,
                wrapperKeyId,
                releaseProtocolVersion,
                timeouts,
                loggerFactory,
              )
            }
        case (true, false) =>
          activeReplicaInitEncryptedStore(
            dbCryptoPrivateStore,
            kms,
            kmsKeyId,
            logger,
          )
            .map { wrapperKeyId =>
              createEncryptedPrivateStore(
                dbCryptoPrivateStore,
                kms,
                wrapperKeyId,
                releaseProtocolVersion,
                timeouts,
                loggerFactory,
              )
            }
        case (false, true) =>
          passiveReplicaRevertEncryptedStore(
            dbCryptoPrivateStore,
            timeouts,
            logger,
          ).thereafter { _ =>
            // Close the KMS after we migrated from encrypted to clear keys
            kms.close()
          }.map { _ =>
            dbCryptoPrivateStore
          }
        case (true, true) =>
          activeReplicaRevertEncryptedStore(
            dbCryptoPrivateStore,
            kms,
            logger,
          )
            .thereafter { _ =>
              // Close the KMS after we migrated from encrypted to clear keys
              kms.close()
            }
            .map { _ =>
              dbCryptoPrivateStore
            }
      }
    }
}
