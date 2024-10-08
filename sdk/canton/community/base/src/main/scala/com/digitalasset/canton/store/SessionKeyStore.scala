// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import com.digitalasset.canton.config.SessionKeyCacheConfig
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.tracing.TraceContext
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

//TODO(#15057) Add stats on cache hits/misses
sealed trait SessionKeyStore {

  /** If the session store is set, we use it to store our session keys and reuse them across transactions.
    * Otherwise, if the 'global' session store is disabled, we create a local cache that is valid only for a single
    * transaction.
    */
  def convertStore: ConfirmationRequestSessionKeyStore =
    this match {
      case SessionKeyStoreDisabled => new SessionKeyStoreWithNoEviction()
      case cache: SessionKeyStoreWithInMemoryCache => cache
    }
}

/** These session key stores are to be used during the processing of a confirmation request.
  * They could either be used for a single transaction (i.e. SessionKeyStoreWithNoEviction) or
  * for all transactions (i.e. SessionKeyStoreWithInMemoryCache). The latter, requires a size limit and
  * an eviction policy to be specified.
  */
sealed trait ConfirmationRequestSessionKeyStore {

  protected val sessionKeysCacheSender: Cache[RecipientGroup, SessionKeyInfo]

  protected val sessionKeysCacheReceiver: Cache[AsymmetricEncrypted[
    SecureRandomness
  ], SecureRandomness]

  protected[canton] def getSessionKeysInfoIfPresent(
      recipients: Seq[RecipientGroup]
  ): Map[RecipientGroup, SessionKeyInfo] =
    sessionKeysCacheSender.getAllPresent(recipients)

  @VisibleForTesting
  protected[canton] def getSessionKeyInfoIfPresent(
      recipients: RecipientGroup
  ): Option[SessionKeyInfo] =
    sessionKeysCacheSender.getIfPresent(recipients)

  protected[canton] def saveSessionKeysInfo(
      toSave: Map[RecipientGroup, SessionKeyInfo]
  ): Unit =
    sessionKeysCacheSender.putAll(toSave)

  protected[canton] def getSessionKeyRandomnessIfPresent(
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness]
  ): Option[SecureRandomness] =
    sessionKeysCacheReceiver.getIfPresent(encryptedRandomness)

  def getSessionKeyRandomness(
      privateCrypto: CryptoPrivateApi,
      keySizeInBytes: Int,
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness],
  )(implicit
      tc: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DecryptionError, SecureRandomness] =
    sessionKeysCacheReceiver.getIfPresent(encryptedRandomness) match {
      case Some(randomness) => EitherT.rightT[FutureUnlessShutdown, DecryptionError](randomness)
      case None =>
        privateCrypto
          .decrypt(encryptedRandomness)(
            SecureRandomness.fromByteString(keySizeInBytes)
          )
          .map { randomness =>
            /* TODO(#15022): to ensure transparency, in the future, we will probably want to cache not just the
             * encrypted randomness for this participant, but for all recipients so that you can also cache the
             * check that everyone can decrypt the randomness if they need it.
             */
            sessionKeysCacheReceiver.put(encryptedRandomness, randomness)
            randomness
          }
    }
}

object SessionKeyStoreDisabled extends SessionKeyStore

final class SessionKeyStoreWithInMemoryCache(sessionKeysCacheConfig: SessionKeyCacheConfig)
    extends SessionKeyStore
    with ConfirmationRequestSessionKeyStore {

  /** This cache keeps track of the session key information for each recipient tree, which is then used to encrypt
    * the view messages.
    *
    * This cache may create interesting eviction strategies during a key roll of a recipient.
    * Whether a key is considered revoked or not depends on the snapshot we're picking.
    *
    * So, consider two concurrent transaction submissions:
    *
    * - tx1 and tx2 pick a snapshot where the key is still valid
    * - tx3 and tx4 pick a snapshot where the key is invalid
    *
    * However, due to concurrency, they interleave for the encrypted view message factory as tx1, tx3, tx2, tx4
    * - tx1 populates the cache for the recipients' tree with a new session key;
    * - tx3 notices that the key is no longer valid, produces a new session key and replaces the old one;
    * - tx2 finds the session key from tx3, but considers it invalid because the key is not active. So create a new session key and evict the old on;
    * - tx4 installs again a new session key
    *
    * Since key rolls are rare and everything still remains consistent we accept this as an expected behavior.
    */
  override protected lazy val sessionKeysCacheSender: Cache[RecipientGroup, SessionKeyInfo] =
    sessionKeysCacheConfig.senderCache
      .buildScaffeine()
      .build()

  /** This cache keeps track of the matching encrypted randomness for the session keys and their correspondent unencrypted value.
    * This way we can save on the amount of asymmetric decryption operations.
    */
  override protected lazy val sessionKeysCacheReceiver
      : Cache[AsymmetricEncrypted[SecureRandomness], SecureRandomness] =
    sessionKeysCacheConfig.receiverCache
      .buildScaffeine()
      .build()

}

/** This cache stores session key information for each recipient tree, which is later used to encrypt view messages.
  * However, in this implementation, the session keys have neither a size limit nor an eviction time.
  * Therefore, this cache MUST only be used when it is local to each transaction and short-lived.
  */
final class SessionKeyStoreWithNoEviction extends ConfirmationRequestSessionKeyStore {

  override protected lazy val sessionKeysCacheSender: Cache[RecipientGroup, SessionKeyInfo] =
    Scaffeine().build()

  override protected lazy val sessionKeysCacheReceiver
      : Cache[AsymmetricEncrypted[SecureRandomness], SecureRandomness] =
    Scaffeine().build()

}

object SessionKeyStore {

  def apply(sessionKeyCacheConfig: SessionKeyCacheConfig): SessionKeyStore =
    if (sessionKeyCacheConfig.enabled)
      new SessionKeyStoreWithInMemoryCache(sessionKeyCacheConfig)
    else SessionKeyStoreDisabled

  // Defines a recipients tree and the crypto scheme used to generate the session key for that group
  final case class RecipientGroup(
      recipients: Recipients,
      cryptoScheme: SymmetricKeyScheme,
  )

}
