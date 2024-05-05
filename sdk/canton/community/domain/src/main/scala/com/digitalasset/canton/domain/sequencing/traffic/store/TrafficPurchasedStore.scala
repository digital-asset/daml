// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.store.db.DbTrafficPurchasedStore
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficPurchasedStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.sequencing.traffic.TrafficPurchased
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

object TrafficPurchasedStore {
  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      batchAggregatorConfig: BatchAggregatorConfig,
  )(implicit executionContext: ExecutionContext): TrafficPurchasedStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryTrafficPurchasedStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbTrafficPurchasedStore(batchAggregatorConfig, dbStorage, timeouts, loggerFactory)
    }

}

/** Maintains the history of traffic purchased entries of sequencer members.
  */
trait TrafficPurchasedStore extends AutoCloseable {

  /** Stores the traffic purchased entry.
    * Updates for which there is already a balance for that member with the same sequencing timestamp are ignored.
    */
  def store(
      trafficPurchased: TrafficPurchased
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Looks up the traffic purchased entries for a member.
    */
  def lookup(
      member: Member
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficPurchased]]

  /** Looks up the latest traffic purchased entry for all members, that were sequenced before
    * the given timestamp (inclusive).
    */
  def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficPurchased]]

  /** Deletes all balances for a given member, if their timestamp is strictly lower than the maximum existing timestamp
    * for that member that is lower or equal to the provided timestamp.
    * In practice this means that we will keep enough to provide the correct balance for any timestamp above or equal the
    * provided timestamp, even if that means not pruning the first timestamp below the provided one.
    * Keeps at least the latest balance if it exists, even if it's in the pruning window.
    */
  def pruneBelowExclusive(
      member: Member,
      upToExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Returns the maximum timestamp present in a member balance.
    */
  def maxTsO(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]]

  /** Persists the timestamp of the last sequenced event in the snapshot with which the sequencer is initialized.
    * This allows to recover from a crash just after onboarding by reading back this timestamp to tick the balance manager.
    */
  def setInitialTimestamp(cantonTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** Gets the timestamp of the last sequenced event in the snapshot the sequencer is initialized with.
    */
  def getInitialTimestamp(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]]
}
