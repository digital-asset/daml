// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.dvp

import com.daml.ledger.javaapi.data.Party
import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.crypto.PseudoRandom
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.performance.model.java.orchestration.PartyGrowth
import com.digitalasset.canton.performance.model.java.orchestration.partygrowth.{
  LocalGrowth,
  RemoteGrowth,
}
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{FutureUtil, Mutex}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

sealed trait ByStanderAllocation {
  def config: PartyGrowth
  def next()(implicit traceContext: TraceContext): Party
}

class NoOpBystanderAllocation(val config: PartyGrowth, base: Party) extends ByStanderAllocation {
  override def next()(implicit traceContext: TraceContext): Party = base
}

abstract class ByStanderAllocationImpl(base: Party, poolSize: Long, ratio: Long)
    extends ByStanderAllocation {

  def config: PartyGrowth

  private val requested = new AtomicLong(0)
  private val pool = ArrayBuffer[Party]()
  protected val lock = new Mutex()

  def next()(implicit traceContext: TraceContext): Party =
    if (ratio == 0) base
    else {
      lock.exclusive {
        reducePoolIfRequired(poolSize.toInt)
        if (requested.getAndIncrement() % ratio == 0)
          requestNext(poolSize.toInt)
        // if pool is empty, just return the base
        if (pool.isEmpty)
          base
        else {
          // fish one out of the pool
          val randomIdx = PseudoRandom.randomUnsigned(pool.size)
          pool(randomIdx)
        }
      }
    }

  private def reducePoolIfRequired(size: Int): Unit =
    if (pool.sizeIs > size) {
      pool.drop(pool.size - size).discard
    }

  protected def addToPool(party: Party): Unit =
    if (poolSize > pool.size) {
      pool.append(party)
    } else {
      val randomIdx = PseudoRandom.randomUnsigned(pool.size)
      pool.update(randomIdx, party)
    }

  protected def requestNext(poolSize: Int)(implicit traceContext: TraceContext): Unit

}

class RemoteByStanderAllocation(
    base: Party,
    client: LedgerClient,
    metric: Gauge[Long],
    val config: RemoteGrowth,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ByStanderAllocationImpl(base, config.poolSize, config.ratio)
    with NamedLogging {

  private val picked = new AtomicLong(0)

  override protected def requestNext(poolSize: Int)(implicit traceContext: TraceContext): Unit = {
    val idx = picked.getAndIncrement()
    val prefix = config.prefix + idx.toString
    val findPartyF = client.partyManagementClient
      .listKnownParties(filterParty = prefix + UniqueIdentifier.delimiter)
      .thereafter {
        case Success((results, _)) if results.nonEmpty =>
          results.foreach { details =>
            logger.debug(s"Found new party ${details.party}")
            addToPool(new com.daml.ledger.javaapi.data.Party(details.party))
          }
          metric.updateValue(x => Math.max(x, idx))
        case Success((_, _)) =>
          logger.debug(s"Didn't find any party with prefix $prefix")
          picked.decrementAndGet().discard
        case Failure(_) =>
          picked.decrementAndGet().discard
      }
    FutureUtil.doNotAwait(findPartyF, s"failed to find bystander party $prefix")
  }

}

class LocalByStanderAllocation(
    base: Party,
    name: String,
    client: LedgerClient,
    metric: Gauge[Long],
    val config: LocalGrowth,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ByStanderAllocationImpl(base, config.poolSize, config.ratio)
    with NamedLogging {

  metric.updateValue(0)
  private val allocated = new AtomicLong(0)
  private val pending = new AtomicInteger(0)

  override protected def requestNext(poolSize: Int)(implicit traceContext: TraceContext): Unit =
    if (pending.get() < config.maxPending && allocated.get() < config.maxNum) {
      val idx = allocated.getAndIncrement()
      // add some randonmess into the name so we don't get duplicate additions errors after restart
      // (with a probability of 1e5)
      val hint = Some(s"$name-bystander-$idx")
      val count = pending.incrementAndGet()
      val allocateF = client.partyManagementClient
        .allocateParty(hint)
        .map(details => new com.daml.ledger.javaapi.data.Party(details.party))
        .map { party =>
          lock.exclusive {
            logger.debug(s"Allocated new bystander party $party as pending=$count")
            metric.updateValue(x => Math.max(x, idx))
            addToPool(party)
          }

        }
        .transform {
          // ignore duplicate parties (poor error code ...)
          case Failure(exception) if exception.getMessage.contains("Party already exists") =>
            logger.debug(s"Party already exists: $hint")
            Success(())
          case other => other
        }
        .thereafter(_ => pending.decrementAndGet().discard)
      FutureUtil.doNotAwait(allocateF, s"failed to allocate bystander party $idx")
    }

}
