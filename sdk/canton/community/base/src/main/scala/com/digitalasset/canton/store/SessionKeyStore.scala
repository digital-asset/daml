// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.EitherT
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, Threading}
import com.digitalasset.canton.config.{ProcessingTimeout, SessionEncryptionKeyCacheConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.Recipients
import com.digitalasset.canton.store.SessionKeyStore.RecipientGroup
import com.digitalasset.canton.tracing.TraceContext
import com.github.benmanes.caffeine.cache.Scheduler
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

//TODO(#15057) Add stats on cache hits/misses
sealed trait SessionKeyStore extends AutoCloseable {

  /** If the session store is set, we use it to store our session keys and reuse them across
    * transactions. Otherwise, if the 'global' session store is disabled, we create a local cache
    * that is valid only for a single transaction.
    */
  def convertStore(implicit
      executionContext: ExecutionContext
  ): ConfirmationRequestSessionKeyStore =
    this match {
      case SessionKeyStoreDisabled => new SessionKeyStoreWithNoEviction()
      case cache: SessionKeyStoreWithInMemoryCache => cache
    }
}

/** These session key stores are to be used during the processing of a confirmation request. They
  * could either be used for a single transaction (i.e. SessionKeyStoreWithNoEviction) or for all
  * transactions (i.e. SessionKeyStoreWithInMemoryCache). The latter, requires a size limit and an
  * eviction policy to be specified.
  */
sealed trait ConfirmationRequestSessionKeyStore {

  protected val sessionKeysCacheRecipients: Cache[RecipientGroup, SessionKeyInfo]

  protected val transparencyCheckCache: Cache[Hash, Unit]

  protected val sessionKeysCacheDecryptions: Cache[AsymmetricEncrypted[
    SecureRandomness
  ], SecureRandomness]

  private[canton] def getSessionKeysInfoIfPresent(
      recipients: Seq[RecipientGroup]
  ): Map[RecipientGroup, SessionKeyInfo] =
    sessionKeysCacheRecipients.getAllPresent(recipients)

  @VisibleForTesting
  private[canton] def getSessionKeyInfoIfPresent(
      recipients: RecipientGroup
  ): Option[SessionKeyInfo] =
    sessionKeysCacheRecipients.getIfPresent(recipients)

  private[canton] def saveSessionKeysInfo(
      toSave: Map[RecipientGroup, SessionKeyInfo]
  ): Unit =
    sessionKeysCacheRecipients.putAll(toSave)

  private[canton] def getSessionKeyRandomnessIfPresent(
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness]
  ): Option[SecureRandomness] =
    sessionKeysCacheDecryptions.getIfPresent(encryptedRandomness)

  private[canton] def getSessionKeyRandomness(
      privateCrypto: CryptoPrivateApi,
      keySizeInBytes: Int,
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness],
  )(implicit
      tc: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DecryptionError, SecureRandomness] =
    sessionKeysCacheDecryptions.getIfPresent(encryptedRandomness) match {
      case Some(randomness) => EitherT.rightT[FutureUnlessShutdown, DecryptionError](randomness)
      case None =>
        privateCrypto
          .decrypt(encryptedRandomness)(
            SecureRandomness.fromByteString(keySizeInBytes)
          )
          .map { randomness =>
            sessionKeysCacheDecryptions.put(encryptedRandomness, randomness)
            randomness
          }
    }

}

object SessionKeyStoreDisabled extends SessionKeyStore {
  override def close(): Unit = ()
}

final class SessionKeyStoreWithInMemoryCache(
    sessionKeysCacheConfig: SessionEncryptionKeyCacheConfig,
    timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends SessionKeyStore
    with ConfirmationRequestSessionKeyStore
    with NamedLogging {

  private val scheduledExecutorService = Threading.singleThreadScheduledExecutor(
    "session-encryption-key-cache",
    noTracingLogger,
  )

  /** This cache keeps track of the session key information for each recipient tree, which is then
    * used to encrypt the view messages.
    *
    * This cache may create interesting eviction strategies during a key roll of a recipient.
    * Whether a key is considered revoked or not depends on the snapshot we're picking.
    *
    * So, consider two concurrent transaction submissions:
    *
    *   - tx1 and tx2 pick a snapshot where the key is still valid
    *   - tx3 and tx4 pick a snapshot where the key is invalid
    *
    * However, due to concurrency, they interleave for the encrypted view message factory as tx1,
    * tx3, tx2, tx4
    *   - tx1 populates the cache for the recipients' tree with a new session key;
    *   - tx3 notices that the key is no longer valid, produces a new session key and replaces the
    *     old one;
    *   - tx2 finds the session key from tx3, but considers it invalid because the key is not
    *     active. So create a new session key and evict the old on;
    *   - tx4 installs again a new session key
    *
    * Since key rolls are rare and everything still remains consistent we accept this as an expected
    * behavior.
    */
  override protected lazy val sessionKeysCacheRecipients: Cache[RecipientGroup, SessionKeyInfo] =
    sessionKeysCacheConfig.senderCache
      .buildScaffeine()
      .scheduler(Scheduler.forScheduledExecutorService(scheduledExecutorService))
      .build()

  /** Cache of hashes of the session key randomness encrypted for all recipients. For each session
    * key, the randomness is encrypted for every recipient, and the resulting list of encryptions is
    * hashed and stored here. Reusing the cached hash during transparency checks avoids repeating
    * expensive encryption operations, while keeping memory usage minimal.
    */
  override protected lazy val transparencyCheckCache: Cache[Hash, Unit] =
    sessionKeysCacheConfig.receiverCache
      .buildScaffeine()
      .build()

  /** This cache keeps track of the matching encrypted randomness for the session keys and their
    * correspondent unencrypted value. This way we can save on the amount of asymmetric decryption
    * operations.
    */
  override protected lazy val sessionKeysCacheDecryptions
      : Cache[AsymmetricEncrypted[SecureRandomness], SecureRandomness] =
    sessionKeysCacheConfig.receiverCache
      .buildScaffeine()
      .scheduler(Scheduler.forScheduledExecutorService(scheduledExecutorService))
      .build()

  override def close(): Unit =
    LifeCycle.close(
      {
        // Invalidate all cache entries and run pending maintenance tasks
        sessionKeysCacheRecipients.invalidateAll()
        sessionKeysCacheRecipients.cleanUp()
        sessionKeysCacheDecryptions.invalidateAll()
        sessionKeysCacheDecryptions.cleanUp()
        ExecutorServiceExtensions(scheduledExecutorService)(logger, timeouts)
      }
    )(logger)
}

/** This cache stores session key information for each recipient tree, which is later used to
  * encrypt view messages. However, in this implementation, the session keys have neither a size
  * limit nor an eviction time. Therefore, this cache MUST only be used when it is local to each
  * transaction and short-lived.
  */
final class SessionKeyStoreWithNoEviction(implicit executionContext: ExecutionContext)
    extends ConfirmationRequestSessionKeyStore {

  override protected lazy val sessionKeysCacheRecipients: Cache[RecipientGroup, SessionKeyInfo] =
    Scaffeine().executor(executionContext.execute(_)).build()

  override protected lazy val transparencyCheckCache: Cache[Hash, Unit] =
    Scaffeine().executor(executionContext.execute(_)).build()

  override protected lazy val sessionKeysCacheDecryptions
      : Cache[AsymmetricEncrypted[SecureRandomness], SecureRandomness] =
    Scaffeine().executor(executionContext.execute(_)).build()

}

object SessionKeyStore {

  def apply(
      sessionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): SessionKeyStore =
    if (sessionKeyCacheConfig.enabled)
      new SessionKeyStoreWithInMemoryCache(sessionKeyCacheConfig, timeouts, loggerFactory)
    else SessionKeyStoreDisabled

  // Defines a recipients tree and the crypto scheme used to generate the session key for that group
  final case class RecipientGroup(
      recipients: Recipients,
      cryptoScheme: SymmetricKeyScheme,
  )

}
