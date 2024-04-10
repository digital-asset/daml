// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.PeanoQueue.{BeforeHead, InsertedValue, NotInserted}
import org.scalatest.wordspec.AnyWordSpec

trait PeanoQueueTest extends BaseTest { this: AnyWordSpec =>

  def peanoQueue[Discr](mk: Counter[Discr] => PeanoQueue[Counter[Discr], String]): Unit = {
    import scala.language.implicitConversions
    implicit def toCounter(i: Long): Counter[Discr] = Counter[Discr](i)

    val testCases =
      Table[String, Counter[Discr], Seq[
        (Seq[(Counter[Discr], String)], Counter[Discr], Seq[String])
      ]](
        ("name", "initial head", "inserts, expected front, expected polled values"),
        ("empty", 0, Seq.empty),
        ("start with 5", 5, Seq.empty),
        ("start with MinValue", Counter.MinValue, Seq.empty),
        (
          "insert",
          0,
          Seq(
            (Seq((1, "one"), (0, "zero"), (3, "three")), 2, Seq("zero", "one")),
            (Seq((2, "two")), 4, Seq("two", "three")),
          ),
        ),
        (
          "complex",
          2,
          Seq(
            (Seq((10, "ten"), (12, "twelve")), 2, Seq.empty),
            (Seq((2, "two"), (5, "five"), (3, "three"), (4, "four")), 6, Seq.empty),
            (Seq((8, "eight"), (7, "seven"), (6, "six")), 9, Seq("two")),
            (
              Seq((9, "nine")),
              11,
              Seq("three", "four", "five", "six", "seven", "eight", "nine", "ten"),
            ),
            (Seq.empty, 11, Seq.empty),
            (Seq((11, "eleven")), 13, Seq("eleven", "twelve")),
          ),
        ),
        (
          "idempotent insert",
          0,
          Seq(
            (
              Seq((1, "one"), (2, "two"), (0, "zero"), (1, "one"), (4, "four")),
              3,
              Seq("zero", "one", "two"),
            ),
            (
              Seq((-10, "negative ten"), (4, "four"), (3, "three"), (5, "five")),
              6,
              Seq("three", "four", "five"),
            ),
          ),
        ),
      )

    forEvery(testCases) { (name, initHead, insertsPolls) =>
      name should {

        val pq = mk(initHead)

        assert(pq.head === initHead, "have head set to the initial value")
        assert(pq.front === initHead, "have front set to the initial value")
        assert(pq.poll().isEmpty, "not allow to poll anything")

        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var polls: Long = 0

        insertsPolls.zipWithIndex foreach { case ((inserts, expFront, expVals), i) =>
          inserts.zipWithIndex foreach { case ((k, v), index) =>
            if (k < initHead) {
              assert(
                pq.alreadyInserted(k),
                s"$name: $k below initial head $initHead is always considered to have been inserted",
              )
            } else {
              val before = inserts.take(index) ++ insertsPolls.take(i).flatMap(_._1)
              if (!before.exists(_._1 == k))
                assert(!pq.alreadyInserted(k), s"$name: $k has not been inserted")
            }

            pq.insert(k, v)
            assert(pq.alreadyInserted(k), s"$name: $k has been inserted")
          }

          assert(pq.head == initHead + polls, "have head set to the previous/initial value")
          assert(pq.front == expFront, "have front set to the first missing key")

          expVals foreach { v =>
            assert(pq.poll() === Some((initHead + polls, v)), "return the values polled")
            polls = polls + 1
            if (Long.MaxValue - polls >= initHead)
              // we have not yet reached Long.MaxValue and polled everything
              assert(pq.head === initHead + polls, "increment the head")
          }

          assert(pq.front === expFront, "leave the front unchanged")
          if (expFront == initHead + polls)
            assert(pq.poll().isEmpty, "stop returning values at the front")
        }
      }
    }

    "inserting MaxValue" should {
      val pq = mk(Long.MaxValue - 1)
      "fail" in {
        assertThrows[IllegalArgumentException](pq.insert(Long.MaxValue, "MAX"))
      }
    }

    "dropUntilFront" should {
      "drop all elements up to the front" in {
        val pq = mk(0)
        pq.insert(0, "zero")
        pq.insert(1, "one")
        pq.insert(3, "three")

        val last = pq.dropUntilFront()
        assert(pq.poll().isEmpty)
        assert(pq.head == pq.front)
        assert(last == Some(Counter(1) -> "one"))
      }

      "return None if nothing moves" in {
        val pq = mk(0)
        pq.insert(1, "one")
        assert(pq.dropUntilFront().isEmpty)
        assert(pq.head == Counter(0))
      }
    }

    "double inserts" should {
      val pq = mk(0L)

      "correctly report inserts or throw exceptions" in {
        assert(pq.insert(1L, "one"))
        assert(pq.insert(2L, "two"))
        assert(pq.insert(4L, "four"))
        assert(!pq.insert(-1L, "minus 1"))
        assert(pq.insert(1L, "one"))
        assert(pq.insert(4L, "four"))
        assertThrows[IllegalArgumentException](pq.insert(4L, "FOUR"))
        assert(pq.insert(0L, "zero"))
        assertThrows[IllegalArgumentException](pq.insert(0L, "ZERO"))
        assert(pq.insert(0L, "zero"))
        assert(pq.poll().contains((Counter(0), "zero")), "polling 0")
        assert(!pq.insert(0L, "zero"))
        assert(!pq.insert(0L, "Zero"))
        assert(pq.poll().contains((Counter(1), "one")), "polling 1")
        assert(pq.poll().contains((Counter(2), "two")), "polling 2")
        assert(pq.poll().isEmpty, "cannot poll 3")
        assert(pq.insert(3L, "three"))
      }
    }

    "get" should {
      val pq = mk(0L)

      Seq((0L, "zero"), (1L, "one"), (3L, "three")).foreach { case (k, v) => pq.insert(k, v) }
      pq.poll()

      val tests = Table(
        ("key", "expected value"),
        (-1L, BeforeHead),
        (0L, BeforeHead),
        (1L, InsertedValue("one")),
        (2L, NotInserted(Some("one"), Some("three"))),
        (3L, InsertedValue("three")),
        (Long.MaxValue, NotInserted(Some("three"), None)),
      )
      tests.foreach { case (key, expected) =>
        assert(pq.get(key) == expected, s"Get($key) should yield $expected")
      }
    }

  }
}

class PeanoTreeQueueTest extends AnyWordSpec with PeanoQueueTest with BaseTest {

  "PeanoTreeQueue" should {
    behave like peanoQueue(PeanoTreeQueue.apply[PeanoTreeQueueTest.Discriminator, String])

    "maintain the invariant" must {
      behave like peanoQueue[PeanoTreeQueueTest.Discriminator](init =>
        new PeanoTreeQueueTest.PeanoTreeQueueHelper[String](init)
      )
    }
  }
}

object PeanoTreeQueueTest {
  case object Discriminator
  type Discriminator = Discriminator.type
  type LocalCounter = Counter[Discriminator]

  class PeanoTreeQueueHelper[V](initHead: LocalCounter)
      extends PeanoTreeQueue[Discriminator, V](initHead) {

    assertInvariant("initialization")

    override def insert(key: LocalCounter, value: V): Boolean = {
      val added = super.insert(key, value)

      assertInvariant(s"insert ($key -> $value)")
      added
    }

    override def poll(): Option[(LocalCounter, V)] = {
      val result = super.poll()

      assertInvariant(s"poll with result $result")

      result
    }

    private def assertInvariant(msg: String): Unit =
      assert(invariant, s"PeanoPriorityQueue invariant violated after $msg")
  }
}

class SynchronizedPeanoTreeQueueTest extends AnyWordSpec with PeanoQueueTest with BaseTest {
  "SynchronizedPeanoTreeQueue" should {
    behave like peanoQueue[PeanoTreeQueueTest.Discriminator](initHead =>
      new SynchronizedPeanoTreeQueue[PeanoTreeQueueTest.Discriminator, String](initHead)
    )
  }
}
