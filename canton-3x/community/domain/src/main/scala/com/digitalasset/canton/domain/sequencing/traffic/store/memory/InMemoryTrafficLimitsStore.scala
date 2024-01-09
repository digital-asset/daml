// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store.memory

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficLimitsStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.traffic.TopUpEvent

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}

/** In memory implementation of the traffic control store
  * This exists mostly for testing purposes, it's not very useful since the SequencerRateLimitManager already
  * keeps the top ups in memory for each member.
  */
class InMemoryTrafficLimitsStore(loggerFactory: NamedLoggerFactory) extends TrafficLimitsStore {
  private val trafficLimits =
    mutable.TreeMap
      .empty[Member, mutable.TreeSet[TopUpEvent]]

  override def updateTotalExtraTrafficLimit(
      partialUpdate: Map[Member, TopUpEvent]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] = blocking {
    trafficLimits.synchronized {
      Future {
        partialUpdate.foreach { case (member, limit) =>
          trafficLimits
            .updateWith(member) {
              case Some(queue) =>
                queue
                  .find(te => te.serial == limit.serial && te.limit != limit.limit)
                  .map { e =>
                    throw new IllegalStateException(
                      s"Member [$member] has existing extra_traffic_limit value of [${e.limit}] but we are attempting to insert [$limit]"
                    )
                  }
                  .getOrElse {
                    Some(queue.addOne(limit))
                  }
              case None =>
                Some(mutable.TreeSet.from(Seq(limit)))
            }
            .discard
        }
      }
    }
  }

  override def getExtraTrafficLimits(member: Member)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Seq[TopUpEvent]] = blocking {
    trafficLimits.synchronized {
      Future.successful(trafficLimits.get(member).toList.flatten.sorted)
    }
  }

  override def pruneBelowSerial(member: Member, upToExclusive: PositiveInt)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Future[Unit] = blocking {
    trafficLimits.synchronized {
      Future.successful {
        trafficLimits.get(member).foreach(_.filterInPlace(_.serial >= upToExclusive))
      }
    }
  }

  override def close(): Unit = trafficLimits.clear()

  override def initialize(
      topUpEvents: Map[Member, TopUpEvent]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[Unit] = blocking {
    trafficLimits.synchronized {
      Future.successful(
        trafficLimits.addAll(topUpEvents.view.mapValues(v => mutable.TreeSet.from(Seq(v)))).discard
      )
    }
  }

}
