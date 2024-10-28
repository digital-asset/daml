// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.OptionT
import cats.syntax.functor.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.store.db.DbCryptoPublicStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPublicStore
import com.digitalasset.canton.crypto.{KeyName, *}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Store for all public cryptographic material such as certificates or public keys. */
trait CryptoPublicStore extends AutoCloseable {

  implicit val ec: ExecutionContext

  // Cached values for public keys with names
  protected val signingKeyMap: TrieMap[Fingerprint, SigningPublicKeyWithName] = TrieMap.empty
  protected val encryptionKeyMap: TrieMap[Fingerprint, EncryptionPublicKeyWithName] = TrieMap.empty

  // Write methods that the underlying store has to implement for the caching
  protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): Future[Unit]

  @VisibleForTesting
  private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): Future[Set[SigningPublicKeyWithName]]

  @VisibleForTesting
  private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): Future[Set[EncryptionPublicKeyWithName]]

  def storePublicKey(publicKey: PublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    (publicKey: @unchecked) match {
      case sigKey: SigningPublicKey => storeSigningKey(sigKey, name)
      case encKey: EncryptionPublicKey => storeEncryptionKey(encKey, name)
    }

  def publicKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, PublicKey] =
    readSigningKey(keyId)
      .widen[PublicKeyWithName]
      .orElse(readEncryptionKey(keyId).widen[PublicKeyWithName])
      .map(_.publicKey)

  def findSigningKeyIdByName(keyName: KeyName)(implicit
      traceContext: TraceContext
  ): OptionT[Future, SigningPublicKey] =
    OptionT(listSigningKeys.map(_.find(_.name.contains(keyName)).map(_.publicKey)))

  def findSigningKeyIdByFingerprint(fingerprint: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, SigningPublicKey] =
    OptionT(listSigningKeys.map(_.find(_.publicKey.fingerprint == fingerprint).map(_.publicKey)))

  def findEncryptionKeyIdByName(keyName: KeyName)(implicit
      traceContext: TraceContext
  ): OptionT[Future, EncryptionPublicKey] =
    OptionT(listEncryptionKeys.map(_.find(_.name.contains(keyName)).map(_.publicKey)))

  def findEncryptionKeyIdByFingerprint(fingerprint: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, EncryptionPublicKey] =
    OptionT(listEncryptionKeys.map(_.find(_.publicKey.fingerprint == fingerprint).map(_.publicKey)))

  def publicKeysWithName(implicit
      traceContext: TraceContext
  ): Future[Set[PublicKeyWithName]] =
    for {
      sigKeys <- listSigningKeys
      encKeys <- listEncryptionKeys
    } yield sigKeys.toSet[PublicKeyWithName] ++ encKeys.toSet[PublicKeyWithName]

  def signingKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, SigningPublicKey] =
    retrieveKeyAndUpdateCache(signingKeyMap, readSigningKey(_))(signingKeyId)

  protected def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, SigningPublicKeyWithName]

  def signingKeys(implicit
      traceContext: TraceContext
  ): Future[Set[SigningPublicKey]] =
    retrieveKeysAndUpdateCache(listSigningKeys, signingKeyMap)

  def storeSigningKey(key: SigningPublicKey, name: Option[KeyName] = None)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    writeSigningKey(key, name).map { _ =>
      signingKeyMap.putIfAbsent(key.id, SigningPublicKeyWithName(key, name)).discard
    }

  def encryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, EncryptionPublicKey] =
    retrieveKeyAndUpdateCache(encryptionKeyMap, readEncryptionKey(_))(encryptionKeyId)

  protected def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[Future, EncryptionPublicKeyWithName]

  def encryptionKeys(implicit
      traceContext: TraceContext
  ): Future[Set[EncryptionPublicKey]] =
    retrieveKeysAndUpdateCache(listEncryptionKeys, encryptionKeyMap)

  def storeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName] = None)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    writeEncryptionKey(key, name)
      .map { _ =>
        encryptionKeyMap.putIfAbsent(key.id, EncryptionPublicKeyWithName(key, name)).discard
      }

  protected def deleteKeyInternal(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  private[crypto] def deleteKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    // Remove key from the caches
    encryptionKeyMap.remove(keyId).discard
    signingKeyMap.remove(keyId).discard

    // Remove key from the store
    deleteKeyInternal(keyId)
  }

  private def retrieveKeyAndUpdateCache[KN <: PublicKeyWithName](
      cache: TrieMap[Fingerprint, KN],
      readKey: Fingerprint => OptionT[Future, KN],
  )(keyId: Fingerprint): OptionT[Future, KN#K] =
    cache.get(keyId) match {
      case Some(key) => OptionT.some(key.publicKey)
      case None =>
        readKey(keyId).map { key =>
          cache.putIfAbsent(keyId, key).discard
          key.publicKey
        }
    }

  private def retrieveKeysAndUpdateCache[KN <: PublicKeyWithName](
      keysFromDb: Future[Set[KN]],
      cache: TrieMap[Fingerprint, KN],
  ): Future[Set[KN#K]] =
    for {
      // we always rebuild the cache here just in case new keys have been added by another process
      // this should not be a problem since these operations to get all keys are infrequent and typically
      // typically the number of keys is not very large
      storedKeys <- keysFromDb
      _ = cache ++= storedKeys.map(k => k.publicKey.id -> k)
    } yield storedKeys.map(_.publicKey)
}

object CryptoPublicStore {
  def create(
      storage: Storage,
      releaseProtocolVersion: ReleaseProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): CryptoPublicStore =
    storage match {
      case _: MemoryStorage => new InMemoryCryptoPublicStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbCryptoPublicStore(dbStorage, releaseProtocolVersion, timeouts, loggerFactory)
    }
}

sealed trait CryptoPublicStoreError extends Product with Serializable with PrettyPrinting
object CryptoPublicStoreError {

  final case class FailedToInsertKey(keyId: Fingerprint, reason: String)
      extends CryptoPublicStoreError {
    override def pretty: Pretty[FailedToInsertKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KeyAlreadyExists[K <: PublicKeyWithName: Pretty](
      keyId: Fingerprint,
      existingPublicKey: K,
      newPublicKey: K,
  ) extends CryptoPublicStoreError {
    override def pretty: Pretty[KeyAlreadyExists[K]] =
      prettyOfClass(
        param("keyId", _.keyId),
        param("existingPublicKey", _.existingPublicKey),
        param("newPublicKey", _.newPublicKey),
      )
  }

}
