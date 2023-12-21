// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.domain.sequencing.traffic.store.db.DbTrafficLimitsStore
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficLimitsStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TopUpEvent
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

object TrafficLimitsStore {
  def apply(
      storage: Storage,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ): TrafficLimitsStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryTrafficLimitsStore(loggerFactory)
      case dbStorage: DbStorage =>
        new DbTrafficLimitsStore(dbStorage, protocolVersion, timeouts, loggerFactory)
    }

}

/** Maintains the extra traffic limit for all members in a single consolidated queryable / updatable state
  */
trait TrafficLimitsStore extends AutoCloseable {

  def initialize(
      topUpEvents: Map[Member, TopUpEvent]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit]

  /** Updates the total extra traffic limit for some members.
    *
    * @param partialUpdate members to update, this is expected to contain only members which should be updated, not the whole set of all known members.
    */
  def updateTotalExtraTrafficLimit(
      partialUpdate: Map[Member, TopUpEvent]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit]

  def updateTotalExtraTrafficLimit(
      member: Member,
      event: TopUpEvent,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] = updateTotalExtraTrafficLimit(Map(member -> event))

  /** Return the extra traffic limits for a member. This will return all known traffic limits in the store.
    * Further processing is required to compute the valid limit at a given timestamp
    *
    * @return list of traffic limits in order
    */
  def getExtraTrafficLimits(member: Member)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Seq[TopUpEvent]]

  /** Prune the store, removing all top ups below the one with this serial.
    */
  private[traffic] def pruneBelowSerial(member: Member, upToExclusive: PositiveInt)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Unit]
}
