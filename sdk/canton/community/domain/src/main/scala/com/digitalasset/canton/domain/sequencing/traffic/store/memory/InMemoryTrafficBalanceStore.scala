// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.TrafficBalance
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficBalanceStore
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.Future

/** In memory implementation of the traffic balance store
  */
class InMemoryTrafficBalanceStore(override protected val loggerFactory: NamedLoggerFactory)
    extends TrafficBalanceStore
    with NamedLogging {
  implicit private val trafficBalanceOrdering: Ordering[TrafficBalance] =
    Ordering.by(_.sequencingTimestamp)
  private val trafficBalances = TrieMap.empty[Member, NonEmpty[SortedSet[TrafficBalance]]]
  private val initTimestamp: AtomicReference[Option[CantonTimestamp]] = new AtomicReference(None)
  // Clearing the table can prevent memory leaks
  override def close(): Unit = trafficBalances.clear()
  override def store(trafficBalance: TrafficBalance)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.successful {
    logger.debug(s"Storing traffic balance $trafficBalance")
    this.trafficBalances
      .updateWith(trafficBalance.member) {
        // If the update has the same timestamp than the last one, we keep the one with the highest serial
        case Some(old)
            if old.lastOption.exists(b =>
              b.sequencingTimestamp == trafficBalance.sequencingTimestamp && b.serial < trafficBalance.serial
            ) =>
          Some(NonEmpty.mk(SortedSet, trafficBalance, old.dropRight(1).toSeq*))
        case Some(old) => Some(old.incl(trafficBalance))
        case None => Some(NonEmpty.mk(SortedSet, trafficBalance))
      }
      .discard
  }

  override def lookup(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Seq[
    TrafficBalance
  ]] = {
    Future.successful(this.trafficBalances.get(member).toList.flatMap(_.toList).sorted)
  }

  override def lookupLatestBeforeInclusive(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TrafficBalance]] = {
    import cats.syntax.functorFilter.*

    val latestBalances = trafficBalances.toSeq.mapFilter { case (member, balances) =>
      val balancesByTs = balances.map(balance => balance.sequencingTimestamp -> balance).toMap
      val tsBeforeO =
        balances.forgetNE.map(_.sequencingTimestamp).maxBefore(timestamp.immediateSuccessor)
      tsBeforeO.map(ts => balancesByTs(ts))
    }

    Future.successful(latestBalances)
  }

  override def pruneBelowExclusive(
      member: Member,
      upToExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.successful {
    this.trafficBalances
      .updateWith(member) {
        case Some(balances) =>
          val maxBelowTimestamp =
            balances.forgetNE.map(_.sequencingTimestamp).maxBefore(upToExclusive.immediateSuccessor)
          val (belowTimestamp, aboveTimestamp) =
            balances.partition(b => maxBelowTimestamp.forall(b.sequencingTimestamp < _))
          val prunedBalances =
            if (aboveTimestamp.isEmpty)
              NonEmpty.from(SortedSet.from(belowTimestamp.lastOption.toList))
            else NonEmpty.from(aboveTimestamp)
          prunedBalances
        case None => None
      }
      .discard
  }

  override def maxTsO(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] = {
    val maxTsO = trafficBalances.map { case (_, balances) =>
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
