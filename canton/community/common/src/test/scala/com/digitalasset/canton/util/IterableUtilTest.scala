// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.IterableUtilTest.CompareOnlyFirst
import org.scalatest.wordspec.AnyWordSpec

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
}

object IterableUtilTest {
  final case class CompareOnlyFirst(first: Int, second: Int) extends Ordered[CompareOnlyFirst] {
    override def compare(that: CompareOnlyFirst): Int = first.compareTo(that.first)
  }
}
