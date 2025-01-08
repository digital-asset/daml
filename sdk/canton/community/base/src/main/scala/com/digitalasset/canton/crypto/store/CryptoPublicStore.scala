// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.{EitherT, OptionT}
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.store.db.DbCryptoPublicStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPublicStore
import com.digitalasset.canton.crypto.{KeyName, *}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.{BaseCantonError, CantonErrorGroups}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/** Store for all public cryptographic material such as certificates or public keys. */
trait CryptoPublicStore extends AutoCloseable { this: NamedLogging =>

  implicit val ec: ExecutionContext

  // Cached values for public keys with names
  private val signingKeyMap: TrieMap[Fingerprint, SigningPublicKeyWithName] = TrieMap.empty
  private val encryptionKeyMap: TrieMap[Fingerprint, EncryptionPublicKeyWithName] = TrieMap.empty

  // Write methods that the underlying store has to implement for the caching
  protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  @VisibleForTesting
  private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[SigningPublicKeyWithName]]

  @VisibleForTesting
  private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[EncryptionPublicKeyWithName]]

  private[crypto] def storePublicKey(publicKey: PublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    (publicKey: @unchecked) match {
      case sigKey: SigningPublicKey => storeSigningKey(sigKey, name)
      case encKey: EncryptionPublicKey => storeEncryptionKey(encKey, name)
    }

  def publicKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, PublicKey] =
    readSigningKey(keyId)
      .widen[PublicKeyWithName]
      .orElse(readEncryptionKey(keyId).widen[PublicKeyWithName])
      .map(_.publicKey)

  def findSigningKeyIdByName(keyName: KeyName)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SigningPublicKey] =
    OptionT(listSigningKeys.map(_.find(_.name.contains(keyName)).map(_.publicKey)))

  def findEncryptionKeyIdByName(keyName: KeyName)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, EncryptionPublicKey] =
    OptionT(listEncryptionKeys.map(_.find(_.name.contains(keyName)).map(_.publicKey)))

  private[crypto] def publicKeysWithName(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[PublicKeyWithName]] =
    for {
      sigKeys <- listSigningKeys
      encKeys <- listEncryptionKeys
    } yield sigKeys ++ encKeys

  def signingKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SigningPublicKey] =
    retrieveKeyAndUpdateCache(signingKeyMap, readSigningKey(_))(signingKeyId)

  protected def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SigningPublicKeyWithName]

  def signingKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[SigningPublicKey]] =
    retrieveKeysAndUpdateCache(listSigningKeys, signingKeyMap)

  private[crypto] def storeSigningKey(key: SigningPublicKey, name: Option[KeyName] = None)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    writeSigningKey(key, name).map { _ =>
      signingKeyMap.put(key.id, SigningPublicKeyWithName(key, name)).discard
    }

  def encryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, EncryptionPublicKey] =
    retrieveKeyAndUpdateCache(encryptionKeyMap, readEncryptionKey(_))(encryptionKeyId)

  protected def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, EncryptionPublicKeyWithName]

  def encryptionKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[EncryptionPublicKey]] =
    retrieveKeysAndUpdateCache(listEncryptionKeys, encryptionKeyMap)

  def storeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName] = None)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    writeEncryptionKey(key, name)
      .map { _ =>
        encryptionKeyMap.put(key.id, EncryptionPublicKeyWithName(key, name)).discard
      }

  private[crypto] def deleteKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  private[crypto] def replaceSigningPublicKeys(newKeys: Seq[SigningPublicKey])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  private[crypto] def replaceEncryptionPublicKeys(newKeys: Seq[EncryptionPublicKey])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  private def retrieveKeyAndUpdateCache[KN <: PublicKeyWithName](
      cache: TrieMap[Fingerprint, KN],
      readKey: Fingerprint => OptionT[FutureUnlessShutdown, KN],
  )(keyId: Fingerprint): OptionT[FutureUnlessShutdown, KN#PK] =
    cache.get(keyId) match {
      case Some(key) => OptionT.some(key.publicKey)
      case None =>
        readKey(keyId).map { key =>
          cache.putIfAbsent(keyId, key).discard
          key.publicKey
        }
    }

  private def retrieveKeysAndUpdateCache[KN <: PublicKeyWithName](
      keysFromDb: FutureUnlessShutdown[Set[KN]],
      cache: TrieMap[Fingerprint, KN],
  ): FutureUnlessShutdown[Set[KN#PK]] =
    for {
      // we always rebuild the cache here just in case new keys have been added by another process
      // this should not be a problem since these operations to get all keys are infrequent and
      // typically the number of keys is not very large
      storedKeys <- keysFromDb
      _ = cache ++= storedKeys.map(k => k.publicKey.id -> k)
    } yield storedKeys.map(_.publicKey)

  def migratePublicKeys(isActive: Boolean, timeouts: ProcessingTimeout)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPublicStoreError, Unit] =
    if (isActive) performPublicKeysMigration()
    else waitForPublicKeysMigrationToComplete(timeouts)

  private def performPublicKeysMigration()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPublicStoreError, Unit] = {
    logger.info("Migrating keys in public key store")

    // During deserialization, keys are checked whether they use a legacy format, in which case they
    // are migrated and the `migrated` flag is set. If they already use the current format, the `migrated`
    // flag is not set.
    // In the active replica, we read all the keys and check whether they have been migrated,
    // in which case we write them back to the store in the new format.

    for {
      signingKeysWithNames <- EitherT.right(listSigningKeys)
      migratedSigningKeys = signingKeysWithNames.toSeq.collect {
        case SigningPublicKeyWithName(publicKey, _name) if publicKey.migrated => publicKey
      }
      _ <- EitherT.right(replaceSigningPublicKeys(migratedSigningKeys))
      _ = logger.info(
        s"Migrated ${migratedSigningKeys.size} of ${signingKeysWithNames.size} signing public keys"
      )
      // Remove migrated keys from the cache
      _ = signingKeyMap.filterInPlace((fp, _) => !migratedSigningKeys.map(_.id).contains(fp))

      encryptionKeysWithNames <- EitherT.right(listEncryptionKeys)
      migratedEncryptionKeys = encryptionKeysWithNames.toSeq.collect {
        case EncryptionPublicKeyWithName(publicKey, _name) if publicKey.migrated => publicKey
      }
      _ <- EitherT.right(replaceEncryptionPublicKeys(migratedEncryptionKeys))
      _ = logger.info(
        s"Migrated ${migratedEncryptionKeys.size} of ${encryptionKeysWithNames.size} encryption public keys"
      )
      // Remove migrated keys from the cache
      _ = encryptionKeyMap.filterInPlace((fp, _) => !migratedEncryptionKeys.map(_.id).contains(fp))
    } yield ()
  }

  private def waitForPublicKeysMigrationToComplete(timeouts: ProcessingTimeout)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPublicStoreError, Unit] = {
    logger.info("Passive replica waiting for public key migration to complete")

    // On the passive side, we just need to wait until reading all the keys shows them having the flag unset.
    EitherT
      .right(
        retry
          .Pause(
            logger,
            FlagCloseable(logger, timeouts),
            maxRetries = timeouts.activeInit.retries(50.millis),
            delay = 50.millis,
            functionFullName,
          )
          .unlessShutdown(allPublicKeysConverted(), retry.NoExceptionRetryPolicy)
      )
      .map(_ => logger.info("Passive replica ready after public key migration"))
  }

  private def allPublicKeysConverted()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = {
    // Ensure we don't read from the caches
    signingKeyMap.clear()
    encryptionKeyMap.clear()

    for {
      signingKeysWithNames <- listSigningKeys
      encryptionKeysWithNames <- listEncryptionKeys
    } yield {
      val noSigningKeysMigrated = signingKeysWithNames.forall {
        case SigningPublicKeyWithName(key, _name) => !key.migrated
      }
      val noEncryptionKeysMigrated = encryptionKeysWithNames.forall {
        case EncryptionPublicKeyWithName(key, _name) => !key.migrated
      }

      noSigningKeysMigrated && noEncryptionKeysMigrated
    }
  }

  @VisibleForTesting
  // Inverse operation from performPublicKeysMigration(): used in tests to produce legacy keys.
  private[canton] def reverseMigration()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPublicStoreError, Seq[Fingerprint]] = {
    logger.info("Reverse-migrating keys in public key store")

    def reverseMigrateKeys[PKN <: PublicKeyWithName](keys: Set[PKN]): Seq[PKN#PK#K] =
      keys.toSeq.mapFilter(keyWithName => keyWithName.publicKey.reverseMigrate())

    for {
      signingKeysWithNames <- EitherT.right(listSigningKeys)
      migratedSigningKeys = reverseMigrateKeys(signingKeysWithNames)
      _ <- EitherT.right(replaceSigningPublicKeys(migratedSigningKeys))
      _ = logger.info(
        s"Reverse-migrated ${migratedSigningKeys.size} of ${signingKeysWithNames.size} signing public keys"
      )

      encryptionKeysWithNames <- EitherT.right(listEncryptionKeys)
      migratedEncryptionKeys = reverseMigrateKeys(encryptionKeysWithNames)
      _ <- EitherT.right(replaceEncryptionPublicKeys(migratedEncryptionKeys))
      _ = logger.info(
        s"Reverse-migrated ${migratedEncryptionKeys.size} of ${encryptionKeysWithNames.size} encryption public keys"
      )
    } yield {
      val migratedSigningKeyIds = migratedSigningKeys.map(_.id)
      val migratedEncryptionKeyIds = migratedEncryptionKeys.map(_.id)

      // Remove migrated keys from the cache
      signingKeyMap.filterInPlace((fp, _) => !migratedSigningKeyIds.contains(fp))
      encryptionKeyMap.filterInPlace((fp, _) => !migratedEncryptionKeyIds.contains(fp))

      migratedSigningKeyIds ++ migratedEncryptionKeyIds
    }
  }
}

object CryptoPublicStore {
  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPublicStoreError, CryptoPublicStore] = {
    val store = storage match {
      case _: MemoryStorage => new InMemoryCryptoPublicStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbCryptoPublicStore(dbStorage, releaseProtocolVersion, timeouts, loggerFactory)
    }

    for {
      _ <- store.migratePublicKeys(storage.isActive, timeouts)
    } yield store
  }

}

sealed trait CryptoPublicStoreError extends Product with Serializable with PrettyPrinting
object CryptoPublicStoreError extends CantonErrorGroups.CommandErrorGroup {

  @Explanation("This error indicates that a key could not be stored.")
  @Resolution("Inspect the error details")
  object ErrorCode
      extends ErrorCode(
        id = "CRYPTO_PUBLIC_STORE_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Wrap(reason: CryptoPublicStoreError)
        extends BaseCantonError.Impl(cause = "An error occurred with the public crypto store")

    final case class WrapStr(reason: String)
        extends BaseCantonError.Impl(cause = "An error occurred with the public crypto store")
  }

  final case class FailedToInsertKey(keyId: Fingerprint, reason: String)
      extends CryptoPublicStoreError {
    override protected def pretty: Pretty[FailedToInsertKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KeyAlreadyExists[K <: PublicKeyWithName: Pretty](
      keyId: Fingerprint,
      existingPublicKey: K,
      newPublicKey: K,
  ) extends CryptoPublicStoreError {
    override protected def pretty: Pretty[KeyAlreadyExists[K]] =
      prettyOfClass(
        param("keyId", _.keyId),
        param("existingPublicKey", _.existingPublicKey),
        param("newPublicKey", _.newPublicKey),
      )
  }

  final case class FailedToMigrateKey(keyId: Fingerprint, reason: String)
      extends CryptoPublicStoreError {
    override protected def pretty: Pretty[FailedToMigrateKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

}
