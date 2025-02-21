// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.store

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.github.blemale.scaffeine.{Cache, Scaffeine}

import scala.concurrent.ExecutionContext

/** Read through async cache with no eviction (as members should be immutable). Members will only be
  * cached if entries are found.
  */
class SequencerMemberCache(
    populate: Traced[Member] => FutureUnlessShutdown[Option[RegisteredMember]]
)(implicit
    executionContext: ExecutionContext
) {
  // Using a AsyncLoadingCache seemed to be problematic with ScalaTest and it would rarely not read-through even if empty
  private val cache: Cache[Member, RegisteredMember] = Scaffeine()
    .executor(executionContext.execute(_))
    .recordStats()
    .build()

  /** Lookup an existing member id for the given member. Tries local cache before querying data
    * store. Return [[scala.None]] if no id exists.
    */
  def apply(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[RegisteredMember]] = {
    def lookupFromStore: FutureUnlessShutdown[Option[RegisteredMember]] =
      for {
        result <- populate(Traced(member))
        _ = result foreach (cache.put(member, _))
      } yield result

    cache
      .getIfPresent(member)
      .fold(lookupFromStore)(result => FutureUnlessShutdown.pure(Option(result)))
  }

  def invalidate(member: Member): Unit = cache.invalidate(member)
}
