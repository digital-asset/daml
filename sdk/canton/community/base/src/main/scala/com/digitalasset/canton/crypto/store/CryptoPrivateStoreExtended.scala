// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.SigningKeyUsage.matchesRelevantUsages
import com.digitalasset.canton.crypto.store.db.StoredPrivateKey
import com.digitalasset.canton.crypto.{
  EncryptionPrivateKey,
  Fingerprint,
  KeyName,
  KeyPurpose,
  PrivateKey,
  SigningKeyUsage,
  SigningPrivateKey,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/** Extends a CryptoPrivateStore with the necessary store write/read operations and is intended to
  * be used by canton internal private crypto stores (e.g.
  * [[com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore]],
  * [[com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore]]).
  *
  * The cache provides a write-through cache such that `get` operations can be served without
  * reading from the async store. Async population of the cache is done at creation time.
  */
trait CryptoPrivateStoreExtended extends CryptoPrivateStore { this: NamedLogging =>

  implicit val ec: ExecutionContext

  protected val releaseProtocolVersion: ReleaseProtocolVersion

  // Cached values for keys and secret
  protected val signingKeyMap: TrieMap[Fingerprint, SigningPrivateKeyWithName] = TrieMap.empty
  private val decryptionKeyMap: TrieMap[Fingerprint, EncryptionPrivateKeyWithName] = TrieMap.empty

  // Write methods that the underlying store has to implement.
  private[crypto] def writePrivateKey(
      key: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit]

  /** Replaces a set of keys transactionally to avoid an inconsistent state of the store. Key ids
    * will remain the same while replacing these keys.
    *
    * @param newKeys
    *   sequence of keys to replace
    */
  private[crypto] def replaceStoredPrivateKeys(newKeys: Seq[StoredPrivateKey])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit]

  private[crypto] def readPrivateKey(keyId: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[StoredPrivateKey]]

  private[crypto] def readPrivateKeys(keyIds: NonEmpty[Seq[Fingerprint]], purpose: KeyPurpose)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[StoredPrivateKey]]

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[StoredPrivateKey]]

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[StoredPrivateKey]]

  private[crypto] def deletePrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit]

  def storePrivateKey(key: PrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    (key: @unchecked) match {
      case signingPrivateKey: SigningPrivateKey => storeSigningKey(signingPrivateKey, name)
      case encryptionPrivateKey: EncryptionPrivateKey =>
        storeDecryptionKey(encryptionPrivateKey, name)
    }

  def exportPrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[PrivateKey]] = {
    logger.info(s"Exporting private key: $keyId")
    for {
      sigKey <- signingKey(keyId).widen[Option[PrivateKey]]
      key <- sigKey.fold(decryptionKey(keyId).widen[Option[PrivateKey]])(key =>
        EitherT.rightT(Some(key))
      )
    } yield key
  }

  def existsPrivateKey(
      keyId: Fingerprint,
      keyPurpose: KeyPurpose,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] =
    keyPurpose match {
      case KeyPurpose.Signing => existsSigningKey(keyId)
      case KeyPurpose.Encryption => existsDecryptionKey(keyId)
    }

  def removePrivateKey(
      keyId: Fingerprint
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] = {
    val deletedSigKey = signingKeyMap.remove(keyId)
    val deletedDecKey = decryptionKeyMap.remove(keyId)

    deletePrivateKey(keyId)
      .leftMap { err =>
        // In case the deletion in the persistence layer failed, we have to restore the cache.
        deletedSigKey.foreach(signingKeyMap.put(keyId, _))
        deletedDecKey.foreach(decryptionKeyMap.put(keyId, _))
        err
      }
  }

  private def readAndParsePrivateKeys[A <: PrivateKey, B <: PrivateKeyWithName](
      keyPurpose: KeyPurpose,
      parsingFunc: StoredPrivateKey => ParsingResult[A],
      buildKeyWithNameFunc: (A, Option[KeyName]) => B,
  )(keyIds: NonEmpty[Seq[Fingerprint]])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[(Fingerprint, B)]] = for {
    storedKeys <- readPrivateKeys(keyIds, keyPurpose)
    keyMap <- storedKeys.toSeq
      .parTraverse {
        storedPrivateKey => // parTraverse use is fine because computation is in-memory only
          parsingFunc(storedPrivateKey) match {
            case Left(parseErr) =>
              EitherT
                .leftT[FutureUnlessShutdown, (Fingerprint, B)](
                  CryptoPrivateStoreError
                    .FailedToReadKey(
                      storedPrivateKey.id,
                      s"could not parse stored key (it can either be corrupted or encrypted): ${parseErr.toString}",
                    )
                )
                .leftWiden[CryptoPrivateStoreError]
            case Right(privateKey) =>
              EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
                (storedPrivateKey.id, buildKeyWithNameFunc(privateKey, storedPrivateKey.name))
              )
          }
      }
      .map(_.toSet)
  } yield keyMap

  private[crypto] def signingKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[SigningPrivateKey]] =
    signingKeys(NonEmpty.mk(Seq, signingKeyId)).map(_.headOption)

  private[crypto] def signingKeys(signingKeyIds: NonEmpty[Seq[Fingerprint]])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[SigningPrivateKey]] =
    retrieveAndUpdateCache(
      signingKeyMap,
      fingerprints =>
        readAndParsePrivateKeys[SigningPrivateKey, SigningPrivateKeyWithName](
          Signing,
          key => SigningPrivateKey.fromTrustedByteString(key.data),
          (privateKey, name) => SigningPrivateKeyWithName(privateKey, name),
        )(fingerprints),
    )(signingKeyIds)

  private[crypto] def storeSigningKey(
      key: SigningPrivateKey,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    for {
      _ <- writePrivateKey(
        new StoredPrivateKey(
          id = key.id,
          data = key.toByteString(releaseProtocolVersion.v),
          purpose = key.purpose,
          name = name,
          wrapperKeyId = None,
        )
      )
        .map { _ =>
          signingKeyMap.put(key.id, SigningPrivateKeyWithName(key, name)).discard
        }
    } yield ()

  def filterSigningKeys(
      signingKeyIds: NonEmpty[Seq[Fingerprint]],
      filterUsage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Seq[Fingerprint]] =
    for {
      signingKeys <- signingKeys(signingKeyIds)
      filteredSigningKeys = signingKeys.filter(key => matchesRelevantUsages(key.usage, filterUsage))
    } yield filteredSigningKeys.map(_.id).toSeq

  def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] =
    signingKey(signingKeyId).map(_.nonEmpty)

  private[crypto] def decryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[EncryptionPrivateKey]] =
    decryptionKeys(NonEmpty.mk(Seq, encryptionKeyId)).map(_.headOption)

  private[crypto] def decryptionKeys(encryptionKeyIds: NonEmpty[Seq[Fingerprint]])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[EncryptionPrivateKey]] =
    retrieveAndUpdateCache(
      decryptionKeyMap,
      fingerprints =>
        readAndParsePrivateKeys[EncryptionPrivateKey, EncryptionPrivateKeyWithName](
          Encryption,
          key => EncryptionPrivateKey.fromTrustedByteString(key.data),
          (privateKey, name) => EncryptionPrivateKeyWithName(privateKey, name),
        )(fingerprints),
    )(encryptionKeyIds)

  private[crypto] def storeDecryptionKey(
      key: EncryptionPrivateKey,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    for {
      _ <- writePrivateKey(
        new StoredPrivateKey(
          id = key.id,
          data = key.toByteString(releaseProtocolVersion.v),
          purpose = key.purpose,
          name = name,
          wrapperKeyId = None,
        )
      )
        .map { _ =>
          decryptionKeyMap.put(key.id, EncryptionPrivateKeyWithName(key, name)).discard
        }
    } yield ()

  def existsDecryptionKey(decryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] =
    decryptionKey(decryptionKeyId).map(_.nonEmpty)

  private def retrieveAndUpdateCache[KN <: PrivateKeyWithName](
      cache: TrieMap[Fingerprint, KN],
      readKeys: NonEmpty[Seq[Fingerprint]] => EitherT[
        FutureUnlessShutdown,
        CryptoPrivateStoreError,
        Set[
          (Fingerprint, KN)
        ],
      ],
  )(
      keyIds: NonEmpty[Seq[Fingerprint]]
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[KN#K]] = {
    val missingKeys = NonEmpty.from((keyIds.toSet -- signingKeyMap.keySet).toSeq)
    for {
      missingKeyMap <- missingKeys.traverse(readKeys)
      _ = missingKeyMap.foreach(update =>
        update.foreach { case (fingerprint, privateKey) =>
          cache.putIfAbsent(fingerprint, privateKey).discard
        }
      )
      keys <- EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError]({
        keyIds.forgetNE.flatMap(keyId => cache.get(keyId).map(_.privateKey))
      })
    } yield keys.toSet

  }

  /** Returns the wrapper key used to encrypt the private key or None if private key is not
    * encrypted.
    *
    * @param keyId
    *   the private key fingerprint
    * @return
    *   the wrapper key used for encryption or None if key is not encrypted
    */
  private[crypto] def encrypted(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[String300]]

  override def queryKmsKeyId(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[String300]] =
    EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](None)

  def migratePrivateKeys(isActive: Boolean, timeouts: ProcessingTimeout)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    if (isActive) performPrivateKeysMigration()
    else waitForPrivateKeysMigrationToComplete(timeouts)

  private def performPrivateKeysMigration()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] =
    // During deserialization, keys are checked whether they use a legacy format, in which case they
    // are migrated and the `migrated` flag is set. If they already use the current format, the `migrated`
    // flag is not set.
    // In the active replica, we read all the keys and check whether they have been migrated,
    // in which case we write them back to the store in the new format.

    for {
      storedSigningKeys <- listPrivateKeys(KeyPurpose.Signing)
      signingKeys <- storedSigningKeys.toSeq.parTraverseFilter(spk =>
        signingKey(spk.id).map(_.map(spk -> _))
      )
      migratedSigningKeys = signingKeys.collect {
        case (stored, privateKey) if privateKey.migrated =>
          new StoredPrivateKey(
            id = privateKey.id,
            data = privateKey.toByteString(releaseProtocolVersion.v),
            purpose = privateKey.purpose,
            name = stored.name,
            wrapperKeyId = stored.wrapperKeyId,
          )
      }
      _ <- replaceStoredPrivateKeys(migratedSigningKeys)
      _ = if (migratedSigningKeys.nonEmpty)
        logger.info(
          s"Migrated ${migratedSigningKeys.size} of ${storedSigningKeys.size} signing private keys"
        )
      // Remove migrated keys from the cache
      _ = signingKeyMap.filterInPlace((fp, _) => !migratedSigningKeys.map(_.id).contains(fp))

      storedEncryptionKeys <- listPrivateKeys(KeyPurpose.Encryption)
      encryptionKeys <- storedEncryptionKeys.toSeq.parTraverseFilter(spk =>
        decryptionKey(spk.id).map(_.map(spk -> _))
      )
      migratedEncryptionKeys = encryptionKeys.collect {
        case (stored, privateKey) if privateKey.migrated =>
          new StoredPrivateKey(
            id = privateKey.id,
            data = privateKey.toByteString(releaseProtocolVersion.v),
            purpose = privateKey.purpose,
            name = stored.name,
            wrapperKeyId = stored.wrapperKeyId,
          )
      }
      _ <- replaceStoredPrivateKeys(migratedEncryptionKeys)
      _ = if (migratedEncryptionKeys.nonEmpty)
        logger.info(
          s"Migrated ${migratedEncryptionKeys.size} of ${storedEncryptionKeys.size} encryption private keys"
        )
      // Remove migrated keys from the cache
      _ = decryptionKeyMap.filterInPlace((fp, _) => !migratedEncryptionKeys.map(_.id).contains(fp))
    } yield ()

  private def waitForPrivateKeysMigrationToComplete(timeouts: ProcessingTimeout)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] = {
    logger.info("Passive replica waiting for private key migration to complete")

    // On the passive side, we just need to wait until reading all the keys shows them having the flag unset.
    EitherT(
      retry
        .Pause(
          logger,
          FlagCloseable(logger, timeouts),
          maxRetries = timeouts.activeInit.retries(50.millis),
          delay = 50.millis,
          functionFullName,
        )
        .unlessShutdown(allPrivateKeysConverted().value, retry.NoExceptionRetryPolicy)
    ).map(_ => logger.info("Passive replica ready after private key migration"))
  }

  private def allPrivateKeysConverted()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] = {
    // Ensure we don't read from the caches
    signingKeyMap.clear()
    decryptionKeyMap.clear()

    for {
      storedSigningKeys <- listPrivateKeys(KeyPurpose.Signing)
      signingKeys <- storedSigningKeys.toSeq.parTraverseFilter(spk =>
        signingKey(spk.id).map(_.map(spk -> _))
      )
      storedEncryptionKeys <- listPrivateKeys(KeyPurpose.Encryption)
      encryptionKeys <- storedEncryptionKeys.toSeq.parTraverseFilter(spk =>
        decryptionKey(spk.id).map(_.map(spk -> _))
      )
    } yield {
      val noSigningKeysMigrated = signingKeys.forall { case (_stored, key) => !key.migrated }
      val noEncryptionKeysMigrated = encryptionKeys.forall { case (_stored, key) => !key.migrated }

      noSigningKeysMigrated && noEncryptionKeysMigrated
    }
  }

  @VisibleForTesting
  // Inverse operation from performPrivateKeysMigration(): used in tests to produce legacy keys.
  private[canton] def reverseMigration()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Seq[Fingerprint]] = {
    logger.info("Reverse-migrating keys in private key store")

    def reverseMigrateKeys[K <: PrivateKey](
        keys: Seq[(StoredPrivateKey, K)]
    ): Seq[StoredPrivateKey] =
      keys.mapFilter { case (stored, privateKey) =>
        for {
          legacyPrivateKey <- privateKey.reverseMigrate()
        } yield new StoredPrivateKey(
          id = legacyPrivateKey.id,
          data = legacyPrivateKey.toByteString(releaseProtocolVersion.v),
          purpose = legacyPrivateKey.purpose,
          name = stored.name,
          wrapperKeyId = stored.wrapperKeyId,
        )
      }

    for {
      storedSigningKeys <- listPrivateKeys(KeyPurpose.Signing)
      signingKeys <- storedSigningKeys.toSeq.parTraverseFilter(spk =>
        signingKey(spk.id).map(_.map(spk -> _))
      )
      migratedSigningKeys = reverseMigrateKeys(signingKeys)
      _ <- replaceStoredPrivateKeys(migratedSigningKeys)
      _ = logger.info(
        s"Reverse-migrated ${migratedSigningKeys.size} of ${storedSigningKeys.size} private keys"
      )

      storedEncryptionKeys <- listPrivateKeys(KeyPurpose.Encryption)
      encryptionKeys <- storedEncryptionKeys.toSeq.parTraverseFilter(spk =>
        decryptionKey(spk.id).map(_.map(spk -> _))
      )
      migratedEncryptionKeys = reverseMigrateKeys(encryptionKeys)
      _ <- replaceStoredPrivateKeys(migratedEncryptionKeys)
      _ = logger.info(
        s"Reverse-migrated ${migratedEncryptionKeys.size} of ${storedEncryptionKeys.size} encryption keys"
      )
    } yield {
      val migratedSigningKeyIds = migratedSigningKeys.map(_.id)
      val migratedEncryptionKeyIds = migratedEncryptionKeys.map(_.id)

      // Remove migrated keys from the cache
      signingKeyMap.filterInPlace((fp, _) => !migratedSigningKeyIds.contains(fp))
      decryptionKeyMap.filterInPlace((fp, _) => !migratedEncryptionKeyIds.contains(fp))

      migratedSigningKeyIds ++ migratedEncryptionKeyIds
    }
  }
}
