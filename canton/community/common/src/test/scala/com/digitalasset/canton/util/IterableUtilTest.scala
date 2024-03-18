// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.IterableUtilTest.CompareOnlyFirst
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.tailrec

class IterableUtilTest extends AnyWordSpec with BaseTest {
  "spansBy" should {
    "work on a simple example" in {
      val example = List(1, 1, 1, 2, 2, 4, 5, 5).zipWithIndex
      IterableUtil
        .spansBy(example)(_._1)
        .map { case (i, it) => i -> it.map(_._2).toList }
        .toList shouldBe List(
        (1, List(0, 1, 2)),
        (2, List(3, 4)),
        (4, List(5)),
        (5, List(6, 7)),
      )
    }

    "run in linear time" in {
      val size = 100000
      IterableUtil.spansBy((1 to size).toVector)(_ % 2 == 0).map(_._2) shouldBe
        (1 to size).map(NonEmpty(Seq, _))
    }
  }

  "subzipBy" should {
    "stop when empty" in {
      IterableUtil.subzipBy(Iterator(1, 2), Iterator.empty: Iterator[Int]) { (_, _) =>
        Some(1)
      } shouldBe Seq.empty

      IterableUtil.subzipBy(Iterator.empty: Iterator[Int], Iterator(1, 2)) { (_, _) =>
        Some(1)
      } shouldBe Seq.empty
    }

    "skip elements in the second argument" in {
      IterableUtil.subzipBy(Iterator(1, 2, 3), Iterator(0, 1, 1, 2, 0, 2, 3, 4)) { (x, y) =>
        if (x == y) Some(x -> y) else None
      } shouldBe Seq(1 -> 1, 2 -> 2, 3 -> 3)
    }

    "not skip elements in the first argument" in {
      IterableUtil.subzipBy(Iterator(1, 2, 3), Iterator(2, 3, 1)) { (x, y) =>
        if (x == y) Some(x -> y) else None
      } shouldBe Seq(1 -> 1)
    }
  }

  "max" should {
    "find the max elements of a list" in {
      IterableUtil.maxList(List(2, 1, 2, 2, 0, 1, 0, 2)) shouldEqual List(2, 2, 2, 2)
      IterableUtil.maxList(List(1, 2, 3)) shouldEqual List(3)
      IterableUtil.maxList(List(3, 2, 1)) shouldEqual List(3)
      IterableUtil.maxList[Int](List.empty) shouldEqual List.empty

      val onlyFirsts =
        List((2, 2), (4, 2), (4, 1), (4, 3), (2, 1), (3, 5), (0, 4), (4, 4)).map(x =>
          CompareOnlyFirst(x._1, x._2)
        )
      IterableUtil.maxList(onlyFirsts) shouldEqual onlyFirsts
        .filter(x => x.first == 4)
        .reverse
    }
  }

  "min" should {
    "find the min elements of a list" in {
      IterableUtil.minList(List(0, 1, 0, 0, 2, 1, 2, 0)) shouldEqual List(0, 0, 0, 0)
      IterableUtil.minList(List(3, 2, 1)) shouldEqual List(1)
      IterableUtil.minList(List(1, 2, 3)) shouldEqual List(1)
      IterableUtil.minList[Int](List.empty) shouldEqual List.empty

      val onlyFirsts =
        List((0, 2), (1, 3), (0, 1), (3, 3), (0, 5)).map(x => CompareOnlyFirst(x._1, x._2))
      IterableUtil.minList(onlyFirsts) shouldEqual onlyFirsts
        .filter(x => x.first == 0)
        .reverse
    }
  }

  "splitAfter" should {
    "split after the elements" in {
      IterableUtil.splitAfter(1 to 12)(isPrime) shouldBe
        Seq(
          NonEmpty(Seq, 1, 2),
          NonEmpty(Seq, 3),
          NonEmpty(Seq, 4, 5),
          NonEmpty(Seq, 6, 7),
          NonEmpty(Seq, 8, 9, 10, 11),
          NonEmpty(Seq, 12),
        )
    }

    "handle the empty sequence gracefulle" in {
      IterableUtil.splitAfter(Seq.empty[Int])(_ => true) shouldBe Seq.empty
      IterableUtil.splitAfter(Seq.empty[Int])(_ => false) shouldBe Seq.empty
    }

    "work if no elements satify the predicate" in {
      IterableUtil.splitAfter(1 to 10)(_ >= 11) shouldBe Seq(NonEmpty(Seq, 1, 2 to 10: _*))
    }

    "evaluate the predicate only on arguments" in {
      IterableUtil.splitAfter(1 to 10)(x =>
        if (x >= 1 && x <= 10) x == 5
        else throw new IllegalArgumentException(s"Predicate evaluated on $x")
      ) shouldBe Seq(NonEmpty(Seq, 1, 2, 3, 4, 5), NonEmpty(Seq, 6, 7, 8, 9, 10))
    }

    // This should run in a couple of hundreds of milliseconds
    "work for long lists efficiently" in {
      val count = 100000
      IterableUtil.splitAfter((1 to count).toVector)(_ => true) shouldBe (1 to count).map(
        NonEmpty(Seq, _)
      )
    }
  }

  @tailrec
  private def isPrime(i: Int): Boolean = {
    if (i == Integer.MIN_VALUE) false
    else if (i < 0) isPrime(-i)
    else if (i < 2) false
    else (2 to Math.sqrt(i.toDouble).toInt).forall(d => i % d != 0)
  }
}

object IterableUtilTest {
  final case class CompareOnlyFirst(first: Int, second: Int) extends Ordered[CompareOnlyFirst] {
    override def compare(that: CompareOnlyFirst): Int = first.compareTo(that.first)
  }
}
