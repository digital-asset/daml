// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.store.db.DbTrafficConsumedStore
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficConsumedStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

object TrafficConsumedStore {
  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): TrafficConsumedStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryTrafficConsumedStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbTrafficConsumedStore(dbStorage, timeouts, loggerFactory)
    }

}

/** Maintains the history of traffic consumed by sequencer members.
  */
trait TrafficConsumedStore extends AutoCloseable {

  /** Stores the traffic consumed.
    * Updates for which there is already a traffic consumed for that member with the same sequencing timestamp are ignored.
    */
  def store(
      trafficConsumed: TrafficConsumed
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Looks up the traffic consumed entries for a member.
    */
  def lookup(
      member: Member
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficConsumed]]

  /** Looks up the last traffic consumed for a member.
    */
  def lookupLast(
      member: Member
  )(implicit
      traceContext: TraceContext
  ): Future[Option[TrafficConsumed]]

  /** Looks up the latest traffic consumed for all members, that were sequenced before
    * the given timestamp (inclusive).
    */
  def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficConsumed]]

  /** Looks up the latest traffic consumed for a specific member, that was sequenced before
    * the given timestamp (inclusive).
    */
  def lookupLatestBeforeInclusiveForMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[TrafficConsumed]]

  /** Looks up the traffic consumed state at the exact timestamp for the member, if found.
    */
  def lookupAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Option[TrafficConsumed]]

  /** Deletes all traffic consumed entries, if their timestamp is strictly lower than the maximum existing timestamp
    * that is lower or equal to the provided timestamp.
    * In practice this means that we will keep enough to provide the correct traffic consumed for any timestamp above or equal the
    * provided timestamp, even if that means not pruning the first timestamp below the provided one.
    * Keeps at least the latest traffic consumed if it exists, even if it's in the pruning window.
    *
    *  @return text information about the data that was pruned
    */
  def pruneBelowExclusive(
      upToExclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): Future[String]
}
