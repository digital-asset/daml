// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CacheConfigWithTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.github.blemale.scaffeine.Cache

import scala.concurrent.{ExecutionContext, Future}

//TODO(#15057) Add stats on cache hits/misses
sealed trait SessionKeyStore {

  protected[canton] def getSessionKeyInfoIfPresent(
      recipients: RecipientGroup
  ): Option[SessionKeyInfo]

  protected[canton] def saveSessionKeyInfo(
      recipients: RecipientGroup,
      sessionKeyInfo: SessionKeyInfo,
  ): Unit

  protected[canton] def getSessionKeyRandomnessIfPresent(
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness]
  ): Option[SecureRandomness]

  def getSessionKeyRandomness(
      privateCrypto: CryptoPrivateApi,
      keySizeInBytes: Int,
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness],
  )(implicit
      tc: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, DecryptionError, SecureRandomness]

}

object SessionKeyStoreDisabled extends SessionKeyStore {

  protected[canton] def getSessionKeyInfoIfPresent(
      recipients: RecipientGroup
  ): Option[SessionKeyInfo] = None

  protected[canton] def saveSessionKeyInfo(
      recipients: RecipientGroup,
      sessionKeyInfo: SessionKeyInfo,
  ): Unit = ()

  protected[canton] def getSessionKeyRandomnessIfPresent(
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness]
  ): Option[SecureRandomness] = None

  def getSessionKeyRandomness(
      privateCrypto: CryptoPrivateApi,
      keySizeInBytes: Int,
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness],
  )(implicit
      tc: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, DecryptionError, SecureRandomness] =
    privateCrypto
      .decrypt(encryptedRandomness)(
        SecureRandomness.fromByteString(keySizeInBytes)
      )

}

final class SessionKeyStoreWithInMemoryCache(sessionKeysCacheConfig: CacheConfigWithTimeout)
    extends SessionKeyStore {

  /** This cache keeps track of the session key information for each recipient group, which is then used to encrypt the randomness that is
    * part of the encrypted view messages.
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
    * - tx1 populates the cache for the recipients with a new session key;
    * - tx3 notices that the key is no longer valid, produces a new session key and replaces the old one;
    * - tx2 finds the session key from tx3, but considers it invalid because the key is not active. So create a new session key and evict the old on;
    * - tx4 installs again a new session key
    *
    * Since key rolls are rare and everything still remains consistent we accept this as an expected behavior.
    */
  private lazy val sessionKeysCacheSender: Cache[RecipientGroup, SessionKeyInfo] =
    sessionKeysCacheConfig
      .buildScaffeine()
      .build()

  protected[canton] def getSessionKeyInfoIfPresent(
      recipients: RecipientGroup
  ): Option[SessionKeyInfo] =
    sessionKeysCacheSender.getIfPresent(recipients)

  protected[canton] def saveSessionKeyInfo(
      recipients: RecipientGroup,
      sessionKeyInfo: SessionKeyInfo,
  ): Unit =
    sessionKeysCacheSender.put(recipients, sessionKeyInfo)

  /** This cache keeps track of the matching encrypted randomness for the session keys and their correspondent unencrypted value.
    * This way we can save on the amount of asymmetric decryption operations.
    */
  private lazy val sessionKeysCacheRecipient
      : Cache[AsymmetricEncrypted[SecureRandomness], SecureRandomness] =
    sessionKeysCacheConfig
      .buildScaffeine()
      .build()

  protected[canton] def getSessionKeyRandomnessIfPresent(
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness]
  ): Option[SecureRandomness] =
    sessionKeysCacheRecipient.getIfPresent(encryptedRandomness)

  def getSessionKeyRandomness(
      privateCrypto: CryptoPrivateApi,
      keySizeInBytes: Int,
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness],
  )(implicit
      tc: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, DecryptionError, SecureRandomness] =
    sessionKeysCacheRecipient.getIfPresent(encryptedRandomness) match {
      case Some(randomness) => EitherT.rightT[Future, DecryptionError](randomness)
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
            sessionKeysCacheRecipient.put(encryptedRandomness, randomness)
            randomness
          }
    }

}

object SessionKeyStore {

  def apply(cacheConfig: CacheConfigWithTimeout): SessionKeyStore =
    if (cacheConfig.expireAfterTimeout == config.NonNegativeFiniteDuration.Zero)
      SessionKeyStoreDisabled
    else new SessionKeyStoreWithInMemoryCache(cacheConfig)

  // Defines a set of recipients and the crypto scheme used to generate the session key for that group
  final case class RecipientGroup(
      recipients: NonEmpty[Set[ParticipantId]],
      cryptoScheme: SymmetricKeyScheme,
  )
}
