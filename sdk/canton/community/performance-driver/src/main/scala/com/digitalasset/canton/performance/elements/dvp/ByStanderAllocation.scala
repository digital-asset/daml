// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.dvp

import com.daml.ledger.javaapi.data.Party
import com.daml.metrics.api.MetricHandle.Gauge
import com.digitalasset.canton.crypto.PseudoRandom
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, Mutex, TryUtil}
import io.grpc.{Status, StatusRuntimeException}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class ByStanderAllocation(
    base: Party,
    name: String,
    client: LedgerClient,
    metric: Gauge[Long],
    maxPending: Int,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  metric.updateValue(0)
  private val randName = PseudoRandom.randomUnsigned(100000)
  private val requested = new AtomicLong(0)
  private val allocated = new AtomicLong(0)
  private val pending = new AtomicInteger(0)
  private val pool = ArrayBuffer[Party]()
  private val lock = new Mutex()

  def next(ratio: Long, poolSize: Int)(implicit traceContext: TraceContext): Party =
    if (ratio == 0) base
    else {
      lock.exclusive {
        reducePoolIfRequired(poolSize)
        if (requested.getAndIncrement() % ratio == 0 && pending.get() < maxPending)
          requestNext(poolSize)
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

  private def requestNext(poolSize: Int)(implicit traceContext: TraceContext): Unit = {
    val idx = allocated.getAndIncrement()
    // add some randonmess into the name so we don't get duplicate additions errors after restart
    // (with a probability of 1e5)
    val hint = Some(s"$name-bystander-$idx-$randName")

    val count = pending.incrementAndGet()
    val allocateF = client.partyManagementClient
      .allocateParty(hint)
      .map(details => new com.daml.ledger.javaapi.data.Party(details.party))
      .map { party =>
        (lock.exclusive {
          logger.debug(s"Allocated new bystander party $party as pending=$count")
          metric.updateValue(x => Math.max(x, idx))
          if (poolSize > pool.size) {
            pool.append(party)
          } else {
            val randomIdx = PseudoRandom.randomUnsigned(pool.size)
            pool.update(randomIdx, party)
          }
        })
      }
      .transform {
        case Failure(x: StatusRuntimeException)
            if x.getStatus.getCode == Status.Code.UNAVAILABLE && x.getStatus.getDescription
              .contains("Channel shutdownNow invoked") =>
          pending.decrementAndGet().discard
          TryUtil.unit
        case Success(_) =>
          pending.decrementAndGet().discard
          TryUtil.unit
        case Failure(x) =>
          pending.decrementAndGet().discard
          TryUtil.unit
      }
    FutureUtil.doNotAwait(allocateF, s"failed to allocate bystander party $idx")
  }

}
