// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.traffic.TrafficPurchased
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.{Future, blocking}

/** In memory implementation of the traffic purchased entry store
  */
class InMemoryTrafficPurchasedStore(override protected val loggerFactory: NamedLoggerFactory)
    extends TrafficPurchasedStore
    with NamedLogging {
  implicit private val trafficPurchasedOrdering: Ordering[TrafficPurchased] =
    Ordering.by(_.sequencingTimestamp)
  private val trafficPurchaseds = TrieMap.empty[Member, NonEmpty[SortedSet[TrafficPurchased]]]
  private val initTimestamp: AtomicReference[Option[CantonTimestamp]] = new AtomicReference(None)
  // Clearing the table can prevent memory leaks
  override def close(): Unit = trafficPurchaseds.clear()
  override def store(trafficPurchased: TrafficPurchased)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.successful {
    logger.debug(s"Storing traffic purchased entry $trafficPurchased")
    this.trafficPurchaseds
      .updateWith(trafficPurchased.member) {
        // If the update has the same timestamp than the last one, we keep the one with the highest serial
        case Some(old)
            if old.lastOption.exists(b =>
              b.sequencingTimestamp == trafficPurchased.sequencingTimestamp && b.serial < trafficPurchased.serial
            ) =>
          Some(NonEmpty.mk(SortedSet, trafficPurchased, old.dropRight(1).toSeq*))
        case Some(old) => Some(old.incl(trafficPurchased))
        case None => Some(NonEmpty.mk(SortedSet, trafficPurchased))
      }
      .discard
  }

  override def lookup(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[
    TrafficPurchased
  ]] = {
    Future.successful(this.trafficPurchaseds.get(member).toList.flatMap(_.toList).sorted)
  }

  override def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficPurchased]] = {
    import cats.syntax.functorFilter.*

    val latestBalances = trafficPurchaseds.toSeq.mapFilter { case (member, balances) =>
      val balancesByTs = balances.map(balance => balance.sequencingTimestamp -> balance).toMap
      val tsBeforeO =
        balances.forgetNE.map(_.sequencingTimestamp).maxBefore(timestamp.immediateSuccessor)
      tsBeforeO.map(ts => balancesByTs(ts))
    }

    Future.successful(latestBalances)
  }

  override def pruneBelowExclusive(
      upToExclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): Future[String] = Future.successful {
    blocking {
      synchronized {
        val pruned = this.trafficPurchaseds.keySet.map { member =>
          val before = this.trafficPurchaseds.get(member).map(_.size).getOrElse(0)
          this.trafficPurchaseds
            .updateWith(member) {
              case Some(balances) =>
                val maxBelowTimestamp =
                  balances.forgetNE
                    .map(_.sequencingTimestamp)
                    .maxBefore(upToExclusive.immediateSuccessor)
                val (belowTimestamp, aboveTimestamp) =
                  balances.partition(b => maxBelowTimestamp.forall(b.sequencingTimestamp < _))
                val keptPurchases =
                  if (aboveTimestamp.isEmpty)
                    NonEmpty.from(SortedSet.from(belowTimestamp.lastOption.toList))
                  else NonEmpty.from(aboveTimestamp)
                keptPurchases
              case None => None
            }
            .map(before - _.size)
            .getOrElse(0)
        }.sum
        s"Removed $pruned traffic purchased entries"
      }
    }
  }

  override def maxTsO(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] = {
    val maxTsO = trafficPurchaseds.map { case (_, balances) =>
      balances.max1.sequencingTimestamp
    }.maxOption

    Future.successful(maxTsO)
  }

  override def setInitialTimestamp(cantonTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.successful(
    initTimestamp.updateAndGet {
      // TODO(i17640): figure out if / how we really want to handle multiple initial timestamps
      // Only update if the new timestamp is more recent
      case Some(ts) if ts >= cantonTimestamp => Some(ts)
      case None => Some(cantonTimestamp)
      case other => other
    }.discard
  )

  override def getInitialTimestamp(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(initTimestamp.get())
}
