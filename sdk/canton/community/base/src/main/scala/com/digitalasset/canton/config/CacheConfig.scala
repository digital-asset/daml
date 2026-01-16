// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.util.BytesUnit
import com.github.blemale.scaffeine.Scaffeine
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.{Executor, RejectedExecutionException}
import scala.concurrent.ExecutionContext

/** Configurations settings for a single cache
  *
  * @param maximumSize
  *   the maximum size of the cache
  * @param expireAfterAccess
  *   how quickly after last access items should be expired from the cache
  */
final case class CacheConfig(
    maximumSize: PositiveNumeric[Long],
    expireAfterAccess: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
) {

  def buildScaffeine(
      loggerFactory: NamedLoggerFactory
  )(implicit ec: ExecutionContext): Scaffeine[Any, Any] =
    buildScaffeineWithoutExecutor()
      .executor(new FallbackExecutor(ec, loggerFactory))

  /** Can be used if there's no reasonable ExecutionContext instance to pass
    */
  def buildScaffeineWithoutExecutor(): Scaffeine[Any, Any] =
    Scaffeine()
      .maximumSize(maximumSize.value)
      .expireAfterAccess(expireAfterAccess.underlying)
}

final case class CacheConfigWithMemoryBounds(
    maximumMemory: PositiveNumeric[BytesUnit],
    expireAfterAccess: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
) {
  def buildScaffeine(
      loggerFactory: NamedLoggerFactory
  )(implicit ec: ExecutionContext): Scaffeine[Any, Any] =
    Scaffeine()
      .maximumWeight(maximumMemory.value.bytes)
      .expireAfterAccess(expireAfterAccess.underlying)
      .executor(new FallbackExecutor(ec, loggerFactory))
}

/** Configurations settings for a single cache where elements are evicted after a certain time as
  * elapsed (regardless of access).
  *
  * @param maximumSize
  *   the maximum size of the cache
  * @param expireAfterTimeout
  *   how quickly after creation items should be expired from the cache
  */
final case class CacheConfigWithTimeout(
    maximumSize: PositiveNumeric[Long],
    expireAfterTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(10),
) {

  def buildScaffeine(
      loggerFactory: NamedLoggerFactory
  )(implicit executionContext: ExecutionContext): Scaffeine[Any, Any] =
    Scaffeine()
      .maximumSize(maximumSize.value)
      .expireAfterWrite(expireAfterTimeout.underlying)
      .executor(new FallbackExecutor(executionContext, loggerFactory))

}

/** Configuration settings for a cache where elements are only evicted when the maximum size is
  * reached.
  *
  * @param maximumSize
  *   the maximum size of the cache
  */
final case class CacheConfigWithSizeOnly(
    maximumSize: PositiveNumeric[Long]
) {
  def buildScaffeine(
      loggerFactory: NamedLoggerFactory
  )(implicit executionContext: ExecutionContext): Scaffeine[Any, Any] =
    Scaffeine()
      .maximumSize(maximumSize.value)
      .executor(new FallbackExecutor(executionContext, loggerFactory))
}

/** Configuration settings for a cache that stores: (a) the public asymmetric encryptions of the
  * session keys for the sender and (b) the decrypting results in the receiver. This reduces the
  * amount of asymmetric operations that need to be performed for each of the views that share the
  * same participant recipient group (i.e. use the same session key).
  *
  * @param enabled
  *   enable/disable caching of the session key. Caching is enabled by default, offering a trade-off
  *   between secrecy and performance
  * @param senderCache
  *   configuration for the sender's cache that stores the encryptions of the session keys
  * @param receiverCache
  *   configuration for the receiver's cache that stores the decryptions of the session keys
  */
final case class SessionEncryptionKeyCacheConfig(
    enabled: Boolean = true,
    senderCache: CacheConfigWithTimeout = CacheConfigWithTimeout(
      maximumSize = PositiveNumeric.tryCreate(10000),
      expireAfterTimeout = PositiveFiniteDuration.ofSeconds(10),
    ),
    receiverCache: CacheConfigWithTimeout = CacheConfigWithTimeout(
      maximumSize = PositiveNumeric.tryCreate(10000),
      expireAfterTimeout = PositiveFiniteDuration.ofSeconds(10),
    ),
)

/** Configuration settings for various internal caches
  *
  * @param indexedStrings
  *   cache size configuration for the static string index cache
  * @param contractStore
  *   cache size configuration for the contract store
  * @param topologySnapshot
  *   cache size configuration for topology snapshots
  * @param keyCache
  *   cache configuration for keys in the topology snapshots to avoid loading redundant keys from
  *   the database.
  * @param finalizedMediatorConfirmationRequests
  *   cache size for the finalized mediator confirmation requests such the mediator does not have to
  *   perform a db round-trip if we have slow responders.
  */
final case class CachingConfigs(
    indexedStrings: CacheConfig = CachingConfigs.defaultStaticStringCache,
    contractStore: CacheConfig = CachingConfigs.defaultContractStoreCache,
    topologySnapshot: CacheConfig = CachingConfigs.defaultTopologySnapshotCache,
    synchronizerClientMaxTimestamp: CacheConfig =
      CachingConfigs.defaultSynchronizerClientMaxTimestampCache,
    partyCache: CacheConfig = CachingConfigs.defaultPartyCache,
    participantCache: CacheConfig = CachingConfigs.defaultParticipantCache,
    keyCache: CacheConfig = CachingConfigs.defaultKeyCache,
    sessionEncryptionKeyCache: SessionEncryptionKeyCacheConfig = SessionEncryptionKeyCacheConfig(),
    publicKeyConversionCache: CacheConfig = CachingConfigs.defaultPublicKeyConversionCache,
    packageVettingCache: CacheConfig = CachingConfigs.defaultPackageVettingCache,
    packageUpgradeCache: CacheConfigWithSizeOnly = CachingConfigs.defaultPackageUpgradeCache,
    memberCache: CacheConfig = CachingConfigs.defaultMemberCache,
    kmsMetadataCache: CacheConfig = CachingConfigs.defaultKmsMetadataCache,
    finalizedMediatorConfirmationRequests: CacheConfig =
      CachingConfigs.defaultFinalizedMediatorConfirmationRequestsCache,
    sequencerPayloadCache: CacheConfigWithMemoryBounds = CachingConfigs.defaultSequencerPayloadCache,
)

object CachingConfigs {

  val defaultStaticStringCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultContractStoreCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultSynchronizerClientMaxTimestampCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(100))
  val defaultTopologySnapshotCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(100))
  val defaultPartyCache: CacheConfig = CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultParticipantCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultKeyCache: CacheConfig = CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultPublicKeyConversionCache: CacheConfig = CacheConfig(
    maximumSize = PositiveNumeric.tryCreate(10000),
    expireAfterAccess = NonNegativeFiniteDuration.ofMinutes(60),
  )
  val defaultPackageVettingCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultPackageUpgradeCache: CacheConfigWithSizeOnly = CacheConfigWithSizeOnly(
    maximumSize = PositiveNumeric.tryCreate(10000)
  )
  val defaultMemberCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultKmsMetadataCache: CacheConfig =
    CacheConfig.apply(maximumSize = PositiveNumeric.tryCreate(20))
  val defaultFinalizedMediatorConfirmationRequestsCache =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultSequencerPayloadCache: CacheConfigWithMemoryBounds =
    CacheConfigWithMemoryBounds(
      maximumMemory = PositiveNumeric.tryCreate(BytesUnit.MB(64L)),
      expireAfterAccess = NonNegativeFiniteDuration.ofMinutes(10),
    )
  @VisibleForTesting
  val testing =
    CachingConfigs(contractStore = CacheConfig(maximumSize = PositiveNumeric.tryCreate(100)))

}

/** A side effect of cache operations, including get, put, invalidate and cleanup is that tasks are
  * scheduled on the executor. Sometime, due to race conditions on shutdown, or due to calls made
  * during shutdown (e.g. cleanup) the underlying executor may already be shut down. In this
  * situation we fall back to the direct execution context. The main task observed during testing is
  * the BoundedLocalCache.PerformCleanupTask.
  */
class FallbackExecutor(context: ExecutionContext, loggerFactory: NamedLoggerFactory)
    extends Executor {
  private val tracedLogger = loggerFactory.getTracedLogger(getClass)
  override def execute(command: Runnable): Unit =
    try {
      context.execute(command)
    } catch {
      case _: RejectedExecutionException =>
        tracedLogger.underlying.info(s"Falling back to direct execution for $command")
        DirectExecutionContext(tracedLogger).execute(command)
        tracedLogger.underlying.info(s"Execution complete for: $command")
    }
}
