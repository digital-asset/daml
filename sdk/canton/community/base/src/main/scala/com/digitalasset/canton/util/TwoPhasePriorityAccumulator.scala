// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.foldable.*
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.TwoPhasePriorityAccumulator.{
  ItemHandle,
  Priority,
  RegisteredItem,
}
import com.google.common.annotations.VisibleForTesting

import java.util
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec

/** A container with two phases for items with priorities:
  *
  *   1. In the accumulation phase, items can be added with a priority via
  *      [[TwoPhasePriorityAccumulator.accumulate]].
  *   1. In the draining phase, items can be removed in priority order via
  *      [[TwoPhasePriorityAccumulator.drain]]. The order of items with equal priority is
  *      unspecified.
  *
  * [[TwoPhasePriorityAccumulator.stopAccumulating]] switches from the accumulation phase to the
  * draining phase. Items can be removed from the container via the handle returned by
  * [[TwoPhasePriorityAccumulator.accumulate]].
  *
  * @param obsoleteO
  *   Optional function to identify obsolete items. Obsolete items are regularly removed during the
  *   accumulation phase.
  *
  * @tparam A
  *   The type of items to accumulate
  * @tparam B
  *   The type of labels for the draining phase
  */
class TwoPhasePriorityAccumulator[A, B](obsoleteO: Option[A => Boolean]) {

  /** Stores the label of the draining phase. Empty during the accumulation phase. */
  private[this] val phase: SingleUseCell[B] = new SingleUseCell[B]()

  /** Counter for generating new handles */
  private[this] val incrementor: AtomicLong = new AtomicLong()

  /** The actual list of items in priority order. Items with equal priority are sorted according to
    * their handles.
    */
  private[this] val items: ConcurrentSkipListMap[RegisteredItem, A] =
    new ConcurrentSkipListMap[RegisteredItem, A](RegisteredItem.orderingRegisteredItem)

  /** Whether this class is still in the accumulation phase */
  def isAccumulating: Boolean = getPhase.isEmpty

  /** Returns [[scala.None$]] in the accumulating phase, and [[scala.Some$]] with the label of the
    * draining phase.
    */
  def getPhase: Option[B] = phase.get

  /** Adds the given item with the given priority, if this container is still in the accumulation
    * phase, and returns a handle to remove it. If the container is already in the draining phase,
    * the item is not added and instead the draining phase label is returned.
    *
    * The same item can be added multiple times, with the same or different priorities, and the
    * container then holds it multiple times. Accordingly, the item will appear as many times during
    * draining as it was added and not removed, unless it has become obsolete.
    */
  def accumulate(item: A, priority: Priority): Either[B, ItemHandle] =
    for {
      _ <- phase.get.toLeft(())
      registeredItem = {
        removeObsoleteTasks()
        val handle = incrementor.getAndIncrement()
        val registeredItem = RegisteredItem(priority, handle)(this)
        val overflow = Option(items.putIfAbsent(registeredItem, item)).isDefined
        // An overflow happens only after 2^64 items having been accumulated, which is unlikely to
        // happen in practice. If it does happen after > 10000 years of continuous accumulation
        // and removal, we just throw to be on the safe side.
        if (overflow)
          throw new IllegalStateException(
            s"Overflow in the TwoPhasePriorityAccumulator at $handle for priority $priority"
          )
        afterRegistration()
        registeredItem
      }
      _ <- phase.get.traverse_ { b =>
        val removed = Option(items.remove(registeredItem)).isDefined
        Either.cond(!removed, (), b)
      }
    } yield registeredItem

  /** Switches from the accumulation phase to the draining phase. Only the first call to this method
    * is effective; all subsequent calls are ignored.
    *
    * @param b
    *   The label for the draining phase
    */
  def stopAccumulating(b: B): Option[B] = phase.putIfAbsent(b)

  /** Returns an iterator over the items to be drained in priority order. Lower priority values are
    * drained first.
    *
    * May only be called during the draining phase.
    *
    * An item is to be drained if all of the following conditions apply:
    *   - It was added during the accumulation phase.
    *   - It has not yet been successfully removed via the handle.
    *   - It has not yet been garbage-collected because it was obsolete at some point since it was
    *     added.
    *
    * If called multiple times during the draining phase, each returned iterator yields only a
    * subset of the items to be drained, respecting the priority order within the subset. All these
    * iterators jointly return all items to be drained with their correct multiplicity.
    *
    * The returned iterator is not thread-safe: it must not be used from multiple threads without
    * explicit synchronization.
    *
    * @throws java.lang.IllegalStateException
    *   if called during the accumulation phase
    */
  def drain()(implicit errorLoggingContext: ErrorLoggingContext): Iterator[(A, Priority)] = {
    if (isAccumulating) ErrorUtil.invalidState("Cannot drain while accumulating")
    new DrainingIterator()
  }

  def stopAndDrain(
      b: B
  )(implicit errorLoggingContext: ErrorLoggingContext): Iterator[(A, Priority)] = {
    stopAccumulating(b).discard
    drain()
  }

  /** Synchronization approach:
    *
    * This iterator may run concurrently to other iterators and to calls to [[accumulate]]] that
    * have gone past the first `phase.get` check.
    *
    * To ensure that each drained item appears in at most one of these iterator, each iterator
    * removes the item from [[items]] and returns it only if the removal actually took place. So
    * each of the removals in [[advance]] is the linearization point for the iterator methods.
    *
    * Each iterator stashes the next element to return in a field [[nextItem]], so that it can
    * correctly answer whether it can deliver more elements.
    *
    * The removal from [[items]] also synchronizes with [[accumulate]]: if [[accumulate]] notices
    * that draining has started some time during the insertion into [[items]], it tries to remove
    * the item again and this removal is the linearization point in this case. Otherwise, there is
    * nothing to linearize because [[accumulate]]'s insertion happens before the draining phase
    * starts.
    */
  private final class DrainingIterator extends Iterator[(A, Priority)] {

    private[this] val iter: util.Iterator[util.Map.Entry[RegisteredItem, A]] =
      items.entrySet().iterator()

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private[this] var nextItem: Option[(A, Priority)] = None
    // Initialize the iterator by grabbing the first element to return
    advance()

    override def hasNext: Boolean = nextItem.isDefined

    override def next(): (A, Priority) = {
      val result = nextItem.getOrElse(throw new NoSuchElementException("next on empty iterator"))
      advance()
      result
    }

    @tailrec private[this] def advance(): Unit =
      if (iter.hasNext) {
        val next = iter.next()
        val removed = Option(items.remove(next.getKey)).isDefined
        if (removed) {
          nextItem = Some((next.getValue, next.getKey.priority))
        } else {
          advance()
        }
      } else {
        nextItem = None
      }
  }

  private def remove(item: RegisteredItem): Boolean =
    Option(items.remove(item)).isDefined

  private def contains(item: RegisteredItem): Boolean =
    items.containsKey(item)

  @VisibleForTesting
  private[util] def removeObsoleteTasks(): Unit =
    obsoleteO.foreach { obsolete =>
      items.entrySet.removeIf(entry => obsolete(entry.getValue))
    }

  /** Hook for testing interesting interleavings */
  @VisibleForTesting
  protected def afterRegistration(): Unit = ()
}

object TwoPhasePriorityAccumulator {
  type Priority = Int

  /** A handle to refer to an accumulated item */
  sealed trait ItemHandle {

    /** Removes the item from the accumulator.
      * @return
      *   Whether the removal actually happened. This does not happen if the item has been removed
      *   or drained previously
      */
    def remove(): Boolean

    /** Returns whether the item is still in the accumulator or about to be drained */
    def accumulated: Boolean
  }

  private final case class RegisteredItem(priority: Priority, handle: Long)(
      private val accumulator: TwoPhasePriorityAccumulator[?, ?]
  ) extends ItemHandle {
    override def remove(): Boolean = accumulator.remove(this)
    override def accumulated: Boolean = accumulator.contains(this)
  }

  private object RegisteredItem {
    implicit val orderingRegisteredItem: Ordering[RegisteredItem] =
      Ordering.by[RegisteredItem, (Int, Long)](task => (task.priority, task.handle))
  }
}
