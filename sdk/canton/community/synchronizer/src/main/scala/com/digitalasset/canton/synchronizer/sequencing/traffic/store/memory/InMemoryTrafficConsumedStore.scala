// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.blocking

/** In memory implementation of the traffic balance store
  */
class InMemoryTrafficConsumedStore(override protected val loggerFactory: NamedLoggerFactory)
    extends TrafficConsumedStore
    with NamedLogging {
  implicit private val trafficConsumedOrdering: Ordering[TrafficConsumed] =
    Ordering.by(_.sequencingTimestamp)
  private val trafficConsumedMap = TrieMap.empty[Member, NonEmpty[SortedSet[TrafficConsumed]]]
  // Clearing the table can prevent memory leaks
  override def close(): Unit = trafficConsumedMap.clear()
  override def store(trafficUpdates: Seq[TrafficConsumed])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    trafficUpdates.foreach { trafficConsumed =>
      logger.debug(s"Storing traffic balance $trafficConsumed")
      this.trafficConsumedMap
        .updateWith(trafficConsumed.member) {
          case Some(old) => Some(old.incl(trafficConsumed))
          case None => Some(NonEmpty.mk(SortedSet, trafficConsumed))
        }
        .discard
    }
  }

  override def lookup(
      member: Member
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[TrafficConsumed]] =
    FutureUnlessShutdown.pure(this.trafficConsumedMap.get(member).toList.flatten)

  override def lookupLast(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficConsumed]] =
    FutureUnlessShutdown.pure(this.trafficConsumedMap.get(member).toList.flatten.lastOption)

  override def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[TrafficConsumed]] = {
    import cats.syntax.functorFilter.*

    val latestBalances = trafficConsumedMap.toSeq.mapFilter { case (member, balances) =>
      val balancesByTs = balances.map(balance => balance.sequencingTimestamp -> balance).toMap
      val tsBeforeO =
        balances.forgetNE.map(_.sequencingTimestamp).maxBefore(timestamp.immediateSuccessor)
      tsBeforeO.map(ts => balancesByTs(ts))
    }

    FutureUnlessShutdown.pure(latestBalances)
  }

  def lookupLatestBeforeInclusiveForMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficConsumed]] = {
    import cats.syntax.functorFilter.*

    val latestBalance = trafficConsumedMap.toSeq.mapFilter {
      case (member, balances) if member == member =>
        val balanceByTs = balances.map(balance => balance.sequencingTimestamp -> balance).toMap
        val tsBeforeO =
          balances.forgetNE.map(_.sequencingTimestamp).maxBefore(timestamp.immediateSuccessor)
        tsBeforeO.map(ts => balanceByTs(ts))
      case _ => None
    }

    FutureUnlessShutdown.pure(latestBalance.headOption)
  }

  override def lookupAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[TrafficConsumed]] = FutureUnlessShutdown.pure {
    trafficConsumedMap.get(member).flatMap(_.find(_.sequencingTimestamp == timestamp))
  }

  override def pruneBelowExclusive(
      upToExclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[String] = FutureUnlessShutdown.pure {
    blocking {
      synchronized {
        val pruned = this.trafficConsumedMap.keySet.map { member =>
          val before = this.trafficConsumedMap.get(member).map(_.size).getOrElse(0)
          this.trafficConsumedMap
            .updateWith(member) {
              case Some(balances) =>
                val maxBelowTimestamp =
                  balances.forgetNE
                    .map(_.sequencingTimestamp)
                    .maxBefore(upToExclusive.immediateSuccessor)
                val (belowTimestamp, aboveTimestamp) =
                  balances.partition(b => maxBelowTimestamp.forall(b.sequencingTimestamp < _))
                val keptConsumptions =
                  if (aboveTimestamp.isEmpty)
                    NonEmpty.from(SortedSet.from(belowTimestamp.lastOption.toList))
                  else NonEmpty.from(aboveTimestamp)
                keptConsumptions
              case None => None
            }
            .map(before - _.size)
            .getOrElse(0)
        }.sum
        s"Removed $pruned traffic consumed entries"
      }
    }
  }
}
