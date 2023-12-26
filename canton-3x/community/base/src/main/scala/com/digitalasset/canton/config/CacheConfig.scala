// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
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

final case class CacheConfigWithTimeout(
    maximumSize: PositiveNumeric[Long],
    expireAfterTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMinutes(10),
) {

  def buildScaffeine(): Scaffeine[Any, Any] =
    Scaffeine().maximumSize(maximumSize.value).expireAfterWrite(expireAfterTimeout.underlying)

}

/** Configuration settings for various internal caches
  *
  * @param indexedStrings cache size configuration for the static string index cache
  * @param contractStore cache size configuration for the contract store
  * @param topologySnapshot cache size configuration for topology snapshots
  * @param finalizedMediatorRequests cache size for the finalized mediator requests such the mediator does not have to
  *                                  perform a db round-trip if we have slow responders.
  */
final case class CachingConfigs(
    indexedStrings: CacheConfig = CachingConfigs.defaultStaticStringCache,
    contractStore: CacheConfig = CachingConfigs.defaultContractStoreCache,
    topologySnapshot: CacheConfig = CachingConfigs.defaultTopologySnapshotCache,
    partyCache: CacheConfig = CachingConfigs.defaultPartyCache,
    participantCache: CacheConfig = CachingConfigs.defaultParticipantCache,
    keyCache: CacheConfig = CachingConfigs.defaultKeyCache,
    sessionKeyCache: CacheConfigWithTimeout = CachingConfigs.defaultSessionKeyCache,
    packageVettingCache: CacheConfig = CachingConfigs.defaultPackageVettingCache,
    mySigningKeyCache: CacheConfig = CachingConfigs.defaultMySigningKeyCache,
    trafficStatusCache: CacheConfig = CachingConfigs.defaultTrafficStatusCache,
    memberCache: CacheConfig = CachingConfigs.defaultMemberCache,
    kmsMetadataCache: CacheConfig = CachingConfigs.kmsMetadataCache,
    finalizedMediatorRequests: CacheConfig = CachingConfigs.defaultFinalizedMediatorRequestsCache,
)

object CachingConfigs {
  val defaultStaticStringCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultContractStoreCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultTopologySnapshotCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(100))
  val defaultPartyCache: CacheConfig = CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultParticipantCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultKeyCache: CacheConfig = CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val defaultSessionKeyCache: CacheConfigWithTimeout =
    CacheConfigWithTimeout(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultPackageVettingCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultMySigningKeyCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(5))
  val defaultTrafficStatusCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(10000))
  val defaultMemberCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  val kmsMetadataCache: CacheConfig =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(20))
  val defaultFinalizedMediatorRequestsCache =
    CacheConfig(maximumSize = PositiveNumeric.tryCreate(1000))
  @VisibleForTesting
  val testing =
    CachingConfigs(contractStore = CacheConfig(maximumSize = PositiveNumeric.tryCreate(100)))

}
