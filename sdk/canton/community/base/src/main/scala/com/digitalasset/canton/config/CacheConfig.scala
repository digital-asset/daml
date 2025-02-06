// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.util.BytesUnit
import com.github.blemale.scaffeine.Scaffeine
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext

/** Configurations settings for a single cache
  *
  * @param maximumSize the maximum size of the cache
  * @param expireAfterAccess how quickly after last access items should be expired from the cache
  */
final case class CacheConfig(
    maximumSize: PositiveNumeric[Long],
    expireAfterAccess: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
) {

  def buildScaffeine()(implicit ec: ExecutionContext): Scaffeine[Any, Any] =
    Scaffeine()
      .maximumSize(maximumSize.value)
      .expireAfterAccess(expireAfterAccess.underlying)
      .executor(ec.execute(_))
}

final case class CacheConfigWithMemoryBounds(
    maximumMemory: PositiveNumeric[BytesUnit],
    expireAfterAccess: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(1),
) {
  def buildScaffeine()(implicit ec: ExecutionContext): Scaffeine[Any, Any] =
    Scaffeine()
      .maximumWeight(maximumMemory.value.bytes)
      .expireAfterAccess(expireAfterAccess.underlying)
      .executor(ec.execute(_))

}

/** Configurations settings for a single cache where elements are evicted after a certain time as elapsed
  * (regardless of access).
  *
  * @param maximumSize the maximum size of the cache
  * @param expireAfterTimeout how quickly after creation items should be expired from the cache
  */
final case class CacheConfigWithTimeout(
    maximumSize: PositiveNumeric[Long],
    expireAfterTimeout: PositiveFiniteDuration = PositiveFiniteDuration.ofMinutes(10),
) {

  def buildScaffeine()(implicit executionContext: ExecutionContext): Scaffeine[Any, Any] =
    Scaffeine()
      .maximumSize(maximumSize.value)
      .expireAfterWrite(expireAfterTimeout.underlying)
      .executor(executionContext.execute(_))

}

/** Configuration settings for a cache that stores: (a) the public asymmetric encryptions of the session keys for the sender
  * and (b) the decrypting results in the receiver. This reduces the amount of asymmetric operations that need to
  * to be performed for each of the views that share the same participant recipient group (i.e. use the same session key).
  *
  * @param enabled enable/disable caching of the session key. Caching is enabled by default, offering
  *                               a trade-off between secrecy and performance
  * @param senderCache  configuration for the sender's cache that stores the encryptions of the session keys
  * @param receiverCache configuration for the receiver's cache that stores the decryptions of the session keys
  */
final case class SessionEncryptionKeyCacheConfig(
    enabled: Boolean,
    senderCache: CacheConfigWithTimeout,
    receiverCache: CacheConfigWithTimeout,
)

/** Configuration settings for various internal caches
  *
  * @param indexedStrings cache size configuration for the static string index cache
  * @param contractStore cache size configuration for the contract store
  * @param topologySnapshot cache size configuration for topology snapshots
  * @param keyCache cache configuration for keys in the topology snapshots to avoid loading redundant keys
  *                 from the database.
  * @param finalizedMediatorConfirmationRequests cache size for the finalized mediator confirmation requests such the mediator does not have to
  *                                  perform a db round-trip if we have slow responders.
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
    sessionEncryptionKeyCache: SessionEncryptionKeyCacheConfig =
      CachingConfigs.defaultSessionEncryptionKeyCacheConfig,
    packageVettingCache: CacheConfig = CachingConfigs.defaultPackageVettingCache,
    memberCache: CacheConfig = CachingConfigs.defaultMemberCache,
    kmsMetadataCache: CacheConfig = CachingConfigs.kmsMetadataCache,
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
  val defaultSessionEncryptionKeyCacheConfig: SessionEncryptionKeyCacheConfig =
    SessionEncryptionKeyCacheConfig(
      enabled = true,
      senderCache = CacheConfigWithTimeout(
        maximumSize = PositiveNumeric.tryCreate(10000),
        expireAfterTimeout = PositiveFiniteDuration.ofSeconds(10),
      ),
      receiverCache = CacheConfigWithTimeout(
        maximumSize = PositiveNumeric.tryCreate(10000),
        expireAfterTimeout = PositiveFiniteDuration.ofSeconds(10),
      ),
    )
  val defaultPackageVettingCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultMemberCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val kmsMetadataCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(20))
  val defaultFinalizedMediatorConfirmationRequestsCache =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultSequencerPayloadCache: CacheConfigWithMemoryBounds =
    CacheConfigWithMemoryBounds(maximumMemory = PositiveNumeric.tryCreate(BytesUnit.MB(200L)))
  @VisibleForTesting
  val testing =
    CachingConfigs(contractStore = CacheConfig(maximumSize = PositiveNumeric.tryCreate(100)))

}
