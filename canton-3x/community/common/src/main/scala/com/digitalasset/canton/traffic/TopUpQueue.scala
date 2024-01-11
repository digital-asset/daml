// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.blocking

/** Stores top ups in memory and provides methods to retrieve them and prune them as they become obsolete.
  *
  * @param initialTopUps initial list of top ups to start with
  */
class TopUpQueue(
    initialTopUps: Seq[TopUpEvent]
) extends AutoCloseable {
  private val timestampedTopUps = {
    val queue = new mutable.PriorityQueue[TopUpEvent]()(
      // Reverse because higher timestamp means "younger", and we want to dequeue from oldest to youngest
      TopUpEvent.ordering.reverse
    )
    initialTopUps.foreach(queue.addOne)
    queue
  }

  // All writes are synchronized so this is safe
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var currentLimit: Option[TopUpEvent] = None

  // NOTE: Not thread-safe
  private def dequeueUntil(
      timestamp: CantonTimestamp,
      queue: mutable.PriorityQueue[TopUpEvent] = timestampedTopUps,
  ): Option[TopUpEvent] = {
    def dequeueAsLong[A](queue: mutable.PriorityQueue[A], pred: A => Boolean): Option[A] = {
      @tailrec
      def go(prev: Option[A]): Option[A] = {
        if (queue.headOption.exists(pred))
          go(Some(queue.dequeue()))
        else
          prev
      }

      go(None)
    }

    dequeueAsLong[TopUpEvent](
      queue,
      _.validFromInclusive <= timestamp,
    )
  }

  /** Add a new top up
    */
  def addOne(topUp: TopUpEvent): Unit = {
    blocking {
      timestampedTopUps.synchronized {
        timestampedTopUps
          .addOne(topUp)
          .discard
      }
    }
  }

  /** Prune until currentTimestamp and return the current and all following top ups
    */
  def pruneUntilAndGetAllTopUpsFor(cantonTimestamp: CantonTimestamp): List[TopUpEvent] = blocking {
    timestampedTopUps.synchronized {
      pruneUntilAndGetTopUpFor(cantonTimestamp)._1.toList ++ timestampedTopUps
        .clone()
        .dequeueAll
        .toList
    }
  }

  /** Return a list of all the top ups without modifying the internal state.
    */
  def getAllTopUps: List[TopUpEvent] = {
    currentLimit.toList ++ timestampedTopUps.clone().dequeueAll.toList
  }

  def getTrafficLimit(timestamp: CantonTimestamp): NonNegativeLong = {
    if (timestampedTopUps.headOption.exists(timestamp >= _.validFromInclusive)) {
      dequeueUntil(timestamp, timestampedTopUps.clone())
    } else currentLimit
  }.asNonNegative

  /** Get the effective top up for the provided timestamp.
    * Earlier top ups will be pruned and won't be accessible anymore
    * Also return a Boolean indicating if the top up has just became effective. Used for pruning.
    */
  def pruneUntilAndGetTopUpFor(
      timestamp: CantonTimestamp
  ): (Option[TopUpEvent], Boolean) =
    blocking {
      timestampedTopUps.synchronized {
        val isNewLimit = dequeueUntil(timestamp).map { newLimit =>
          currentLimit = Some(newLimit)
          newLimit
        }.isDefined
        (currentLimit, isNewLimit)
      }
    }

  /** Same as [[pruneUntilAndGetTopUpFor]] but if no top up is found, will return NonNegativeLong.zero
    */
  def pruneUntilAndGetLimitFor(timestamp: CantonTimestamp): NonNegativeLong =
    pruneUntilAndGetTopUpFor(timestamp)._1.asNonNegative

  override def close(): Unit = blocking {
    timestampedTopUps.synchronized {
      timestampedTopUps.dequeueAll: Unit
      ()
    }
  }
}
