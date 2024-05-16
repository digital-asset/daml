// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.store.db.StoredPrivateKey
import com.digitalasset.canton.crypto.{
  EncryptionPrivateKey,
  Fingerprint,
  KeyName,
  KeyPurpose,
  PrivateKey,
  SigningPrivateKey,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Extends a CryptoPrivateStore with the necessary store write/read operations and is intended to be used by canton
  * internal private crypto stores (e.g. [[com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore]],
  * [[com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore]]).
  *
  * The cache provides a write-through cache such that `get` operations can be served without reading from the async store.
  * Async population of the cache is done at creation time.
  */
trait CryptoPrivateStoreExtended extends CryptoPrivateStore { this: NamedLogging =>

  implicit val ec: ExecutionContext

  protected val releaseProtocolVersion: ReleaseProtocolVersion

  // Cached values for keys and secret
  protected val signingKeyMap: TrieMap[Fingerprint, SigningPrivateKeyWithName] = TrieMap.empty
  protected val decryptionKeyMap: TrieMap[Fingerprint, EncryptionPrivateKeyWithName] = TrieMap.empty

  // Write methods that the underlying store has to implement.
  private[crypto] def writePrivateKey(
      key: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit]

  /** Replaces a set of keys transactionally to avoid an inconsistent state of the store.
    * Key ids will remain the same while replacing these keys.
    *
    * @param newKeys sequence of keys to replace
    */
  private[crypto] def replaceStoredPrivateKeys(newKeys: Seq[StoredPrivateKey])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit]

  private[crypto] def readPrivateKey(keyId: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[StoredPrivateKey]]

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Set[StoredPrivateKey]]

  private[crypto] def deletePrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit]

  def storePrivateKey(key: PrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Unit] = {
    (key: @unchecked) match {
      case signingPrivateKey: SigningPrivateKey => storeSigningKey(signingPrivateKey, name)
      case encryptionPrivateKey: EncryptionPrivateKey =>
        storeDecryptionKey(encryptionPrivateKey, name)
    }
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
      .mapK(FutureUnlessShutdown.outcomeK)
  }

  private def readAndParsePrivateKey[A <: PrivateKey, B <: PrivateKeyWithName](
      keyPurpose: KeyPurpose,
      parsingFunc: StoredPrivateKey => ParsingResult[A],
      buildKeyWithNameFunc: (A, Option[KeyName]) => B,
  )(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[B]] =
    readPrivateKey(keyId, keyPurpose)
      .flatMap {
        case Some(storedPrivateKey) =>
          parsingFunc(storedPrivateKey) match {
            case Left(parseErr) =>
              EitherT.leftT[FutureUnlessShutdown, Option[B]](
                CryptoPrivateStoreError
                  .FailedToReadKey(
                    keyId,
                    s"could not parse stored key (it can either be corrupted or encrypted): ${parseErr.toString}",
                  )
              )
            case Right(privateKey) =>
              EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](
                Some(buildKeyWithNameFunc(privateKey, storedPrivateKey.name))
              )
          }
        case None => EitherT.rightT[FutureUnlessShutdown, CryptoPrivateStoreError](None)
      }

  private[crypto] def signingKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[SigningPrivateKey]] =
    retrieveAndUpdateCache(
      signingKeyMap,
      keyFingerprint =>
        readAndParsePrivateKey[SigningPrivateKey, SigningPrivateKeyWithName](
          Signing,
          key => SigningPrivateKey.fromTrustedByteString(key.data),
          (privateKey, name) => SigningPrivateKeyWithName(privateKey, name),
        )(keyFingerprint),
    )(signingKeyId)

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

  def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Boolean] =
    signingKey(signingKeyId).map(_.nonEmpty)

  private[crypto] def decryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[EncryptionPrivateKey]] = {
    retrieveAndUpdateCache(
      decryptionKeyMap,
      keyFingerprint =>
        readAndParsePrivateKey[EncryptionPrivateKey, EncryptionPrivateKeyWithName](
          Encryption,
          key => EncryptionPrivateKey.fromTrustedByteString(key.data),
          (privateKey, name) => EncryptionPrivateKeyWithName(privateKey, name),
        )(keyFingerprint),
    )(encryptionKeyId)
  }

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
      readKey: Fingerprint => EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[KN]],
  )(keyId: Fingerprint): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[KN#K]] =
    cache.get(keyId) match {
      case Some(value) => EitherT.rightT(Some(value.privateKey))
      case None =>
        readKey(keyId).map { keyOption =>
          keyOption.foreach(key => cache.putIfAbsent(keyId, key))
          keyOption.map(_.privateKey)
        }
    }

  /** Returns the wrapper key used to encrypt the private key
    * or None if private key is not encrypted.
    *
    * @param keyId private key fingerprint
    * @return the wrapper key used for encryption or None if key is not encrypted
    */
  private[crypto] def encrypted(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, Option[String300]]
}
