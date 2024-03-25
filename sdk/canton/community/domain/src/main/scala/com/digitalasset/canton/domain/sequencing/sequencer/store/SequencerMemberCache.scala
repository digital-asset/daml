// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store

import com.digitalasset.canton.topology.{Member, UnauthenticatedMemberId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.concurrent.{ExecutionContext, Future}

/** Read through async cache with no eviction (as members should be immutable, except for unauthenticated members
  * that can be evicted when they get unregistered).
  * Members will only be cached if entries are found.
  */
class SequencerMemberCache(populate: Traced[Member] => Future[Option[RegisteredMember]])(implicit
    executionContext: ExecutionContext
) {
  // Using a AsyncLoadingCache seemed to be problematic with ScalaTest and it would rarely not read-through even if empty
  private val cache: Cache[Member, RegisteredMember] = Scaffeine()
    .recordStats()
    .build()

  /** Lookup an existing member id for the given member.
    * Tries local cache before querying data store.
    * Return [[scala.None]] if no id exists.
    */
  def apply(
      member: Member
  )(implicit traceContext: TraceContext): Future[Option[RegisteredMember]] = {
    def lookupFromStore: Future[Option[RegisteredMember]] =
      for {
        result <- populate(Traced(member))
        _ = result foreach (cache.put(member, _))
      } yield result

    cache.getIfPresent(member).fold(lookupFromStore)(result => Future.successful(Option(result)))
  }

  /** Evicts an unauthenticated member from the cache. Used when unregistering unauthenticated members.
    * @param member member to evict from the cache
    */
  def evict(member: UnauthenticatedMemberId): Unit = {
    cache.invalidate(member)
  }
}
