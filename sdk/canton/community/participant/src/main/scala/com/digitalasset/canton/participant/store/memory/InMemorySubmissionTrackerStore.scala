// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.SubmissionTrackerStore
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemorySubmissionTrackerStore(
    override protected val loggerFactory: NamedLoggerFactory
)(implicit
    val ec: ExecutionContext
) extends SubmissionTrackerStore
    with InMemoryPrunableByTime
    with NamedLogging {
  // Actual persisted submission tracker. Entries are original requests submitted by this participant.
  private val freshSubmittedTransactions: concurrent.Map[RootHash, (RequestId, CantonTimestamp)] =
    TrieMap[RootHash, (RequestId, CantonTimestamp)]()

  override def close(): Unit = ()

  override def registerFreshRequest(
      rootHash: RootHash,
      requestId: RequestId,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] = {
    freshSubmittedTransactions.putIfAbsent(rootHash, (requestId, maxSequencingTime)).discard

    // Check whether `rootHash` is associated to `requestId`.
    // We could determine this by checking whether the above call returns None, but we need idempotence for
    // crash recovery and to match the behavior of the DB-backed store.
    val isFresh = freshSubmittedTransactions.get(rootHash) match {
      case Some((`requestId`, _)) => true
      case _ => false
    }

    FutureUnlessShutdown.pure(isFresh)
  }

  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Future[Int] = {
    val counter = new AtomicInteger(0)
    Future.successful {
      freshSubmittedTransactions.filterInPlace {
        case (_rootHash, (_requestId, maxSequencingTime)) =>
          if (maxSequencingTime > beforeAndIncluding)
            true
          else {
            counter.incrementAndGet()
            false
          }
      }
      counter.get()
    }
  }

  override def size(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown.pure(freshSubmittedTransactions.size)

  @VisibleForTesting
  def clear(): Unit = freshSubmittedTransactions.clear()

  override def deleteSince(
      including: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      freshSubmittedTransactions.filterInPlace {
        case (_rootHash, (requestId, _maxSequencingTime)) =>
          requestId.unwrap < including
      }
    }
}
