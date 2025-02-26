// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.TwoPhasePriorityAccumulator.ItemHandle
import com.digitalasset.canton.util.TwoPhasePriorityAccumulatorTest.Item
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

class TwoPhasePriorityAccumulatorTest extends AnyWordSpec with BaseTest {

  private def priority[A](itemWithPriority: (A, Int)): Int = itemWithPriority._2

  s"${classOf[TwoPhasePriorityAccumulator[?, ?]].getSimpleName}" should {
    "drain all items in ascending priority order" in {

      val accumulator = new TwoPhasePriorityAccumulator[String, Unit](None)
      accumulator.isAccumulating shouldBe true

      val items = Seq("first" -> 1, "second" -> 2, "third" -> 0)

      items.foreach { case (item, priority) =>
        accumulator.accumulate(item, priority).value
      }

      accumulator.isAccumulating shouldBe true
      accumulator.stopAccumulating(()) shouldBe None
      accumulator.isAccumulating shouldBe false

      accumulator.drain().toSeq shouldBe items.sortBy(priority)
    }

    "be empty by default" in {
      val accumulator = new TwoPhasePriorityAccumulator[String, Unit](None)
      accumulator.stopAndDrain(()).toSeq shouldBe Seq.empty
    }

    "allow the same item to be added multiple times" in {
      val accumulator = new TwoPhasePriorityAccumulator[String, Unit](None)

      val items = Seq("first" -> 1, "second" -> 2, "first" -> 3, "first" -> 1, "first" -> 0)
      items.foreach((accumulator.accumulate _).tupled(_).value)

      accumulator.stopAccumulating(())
      accumulator.drain().toSeq shouldBe items.sortBy(priority)
    }

    "accumulation stops upon draining" in {
      val accumulator = new TwoPhasePriorityAccumulator[String, Int](None)

      val items = Seq("first" -> 1, "second" -> 2)
      items.foreach((accumulator.accumulate _).tupled(_).value)

      val label = 10
      accumulator.stopAccumulating(label)

      accumulator.accumulate("third", 3) shouldBe Left(label)
      val iter = accumulator.drain()
      accumulator.accumulate("forth", 4) shouldBe Left(label)
      iter.toSeq shouldBe items.sortBy(priority)
    }

    "support removal via the handle" in {
      val accumulator = new TwoPhasePriorityAccumulator[String, Unit](None)

      val items = Seq("first", "second", "third", "forth", "first", "first")
      val handles = items.zipWithIndex.map((accumulator.accumulate _).tupled(_).value)

      handles.zipWithIndex.foreach { case (handle, index) =>
        if (index % 2 == 0) {
          handle.remove() shouldBe true
        }
      }

      accumulator.stopAccumulating(())
      accumulator.drain().toSeq shouldBe items.zipWithIndex.filterNot(_._2 % 2 == 0)
    }

    "removal succeeds only once" in {
      val accumulator = new TwoPhasePriorityAccumulator[String, Unit](None)

      val items = Seq("first", "second", "third", "forth")
      val handles = items.zipWithIndex.map((accumulator.accumulate _).tupled(_).value)

      handles(0).remove() shouldBe true
      handles(0).remove() shouldBe false

      accumulator.stopAccumulating(())
      val iter = accumulator.drain()
      iter.hasNext shouldBe true
      iter.next() shouldBe (items(1) -> 1)

      handles(1).remove() shouldBe false
      val removals34 = Seq(handles(2), handles(3)).map(_.remove())

      val drained34 = iter.toSeq
      drained34.size + removals34.count(identity) shouldBe 2
    }

    "can be stopped only once" in {
      val accumulator = new TwoPhasePriorityAccumulator[String, Int](None)

      val label = 5
      accumulator.stopAccumulating(label) shouldBe None
      accumulator.accumulate("first", 1) shouldBe Left(label)
      accumulator.stopAccumulating(label + 1) shouldBe Some(label)
      accumulator.accumulate("second", 2) shouldBe Left(label)
      accumulator.stopAccumulating(label + 2) shouldBe Some(label)
    }

    "parallel drainings are disjoint and ascending in priority" in {
      val accumulator = new TwoPhasePriorityAccumulator[String, Unit](None)
      val items = Seq(
        "first",
        "second",
        "third",
        "forth",
        "fifth",
        "sixth",
        "seventh",
        "eighth",
        "ninth",
        "tenth",
      )
      items.zipWithIndex.foreach((accumulator.accumulate _).tupled(_).value)

      accumulator.stopAccumulating(())

      val iter1 = accumulator.drain()
      val iter2 = accumulator.drain()
      val iter3 = accumulator.drain()
      val iters = Seq(iter1, iter2, iter3)

      val drainedB = Seq.newBuilder[(Int, (String, TwoPhasePriorityAccumulator.Priority))]

      @tailrec def roundRobin(index: Int, lastDrainedIndex: Int): Unit = {
        val iter = iters(index)
        if (iter.hasNext) {
          drainedB += index -> iter.next()
          roundRobin((index + 1) % iters.size, index)
        } else if (index == lastDrainedIndex)
          // Stop if we've gone through all iterators and they are all empty
          ()
        else roundRobin((index + 1) % iters.size, lastDrainedIndex)
      }
      roundRobin(0, 0)

      val drained = drainedB.result()
      drained
        .map { case (_, itemWithPriority) => itemWithPriority }
        .sortBy { case (_, priority) => priority } shouldBe items.zipWithIndex
      forAll(drained.groupBy { case (index, _) => index }) { case (index, drainedI) =>
        val priorities = drainedI.map { case (_, itemWithPriority) => priority(itemWithPriority) }
        priorities.sorted shouldBe priorities
      }
    }

    "clean up obsolete items" in {
      val accumulator = new TwoPhasePriorityAccumulator[Item, Unit](Some(_.isObsolete))

      val items = Seq("first", "second", "third", "forth", "fifth", "sixth").map(Item.apply)

      val handles = items.map(accumulator.accumulate(_, 0).value)

      val obsoleteIndices = Seq(1, 3, 5)
      obsoleteIndices.foreach { index =>
        items(index).markObsolte()
      }
      accumulator.removeObsoleteTasks()

      val notRemoved = handles.map(_.remove()).zipWithIndex.collect { case (false, index) => index }
      notRemoved shouldBe obsoleteIndices

      accumulator.stopAccumulating(())
      val drained = accumulator.drain()
      drained.toSeq shouldBe Seq.empty
    }

    "behave correctly if races occur" in {
      // We simulate races by hijacking the obsolete item clean-up flag.
      // This makes this unit test an ugly white-box test

      val obsoleteRef = new AtomicReference[Item => Boolean](_ => false)
      val afterRegistrationRef = new AtomicReference[() => Unit](() => ())

      val accumulator =
        new TwoPhasePriorityAccumulator[Item, Unit](Some(item => obsoleteRef.get()(item))) {
          override protected def afterRegistration(): Unit = afterRegistrationRef.get()()
        }

      val items = Seq("first", "second", "third", "forth", "fifth", "sixth").map(Item.apply)
      val priority = 0
      val handles = items.take(3).map(accumulator.accumulate(_, priority).value)

      val drainedRef =
        new AtomicReference[Seq[(Item, TwoPhasePriorityAccumulator.Priority)]](Seq.empty)
      val accumulatedRef = TrieMap.empty[Item, Either[Unit, ItemHandle]]

      afterRegistrationRef.set { () =>
        // This runs while we're accumulating items(3)
        afterRegistrationRef.set { () =>
          // This runs while we're accumulating items(3,4)
          obsoleteRef.set { item =>
            // This runs while we're accumulating items(3,4,5)
            if (item == items(2)) {
              // Use item(2) as a flag to ensure this executes only once
              handles(2).remove()
              afterRegistrationRef.set(() => ())
              obsoleteRef.set(_ => false)
              // Completely drain the accumulator
              // This will run before the accumulator is inserted,
              // and so we expect the result of the accumulation to be a left
              drainedRef.set(accumulator.stopAndDrain(()).toSeq)
            }
            false
          }
          val item = items(5)
          val handle = accumulator.accumulate(item, priority)
          accumulatedRef.put(item, handle)
        }
        val item = items(4)
        val handle = accumulator.accumulate(item, priority)
        accumulatedRef.put(item, handle)
      }
      accumulator.accumulate(items(3), priority).value

      drainedRef.get().toSet shouldBe
        Seq(items(0), items(1), items(3), items(4)).map(_ -> priority).toSet
      accumulatedRef.keySet shouldBe Set(items(4), items(5))
      // items(4) gets drained and therefore should be a handle
      accumulatedRef(items(4)).value
      // items(5) does not get drained and must therefore be a Left
      accumulatedRef(items(5)) shouldBe Left(())
    }

  }
}

object TwoPhasePriorityAccumulatorTest {
  private final case class Item(name: String) {
    val obsoleteFlag: AtomicBoolean = new AtomicBoolean()
    def isObsolete: Boolean = obsoleteFlag.get()
    def markObsolte(): Unit = obsoleteFlag.set(true)
  }
}
