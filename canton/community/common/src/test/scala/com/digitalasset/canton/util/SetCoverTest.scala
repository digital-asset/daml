// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class SetCoverTest extends AnyWordSpec with BaseTest {

  "greedy" should {
    "compute a cover" in {

      val testCases = Seq(
        // All sets are needed
        Map("a" -> Set(1, 2), "b" -> Set(2, 3)) -> Seq(Seq("a", "b")),
        // Just one set
        Map("a" -> Set(1, 2)) -> Seq(Seq("a")),
        // Empty universe
        Map.empty[String, Set[Int]] -> Seq(Seq.empty[String]),
        // Empty universe with an empty set
        Map("a" -> Set.empty[Int]) -> Seq(Seq.empty[String]),
        // An example where the greedy algorithm finds the minimal solution
        Map("a" -> Set(1, 2, 3), "b" -> Set(1, 2), "c" -> Set(3)) -> Seq(Seq("a")),
        // An example where the greedy algorithm finds a non-minimal solution
        Map(
          "a" -> Set((1 to 7)*),
          "b" -> Set((8 to 14)*),
          "c" -> Set(1, 8),
          "d" -> Set(2, 3, 9, 10),
          "e" -> Set(4, 5, 6, 7, 11, 12, 13, 14),
        ) -> Seq(Seq("c", "d", "e")),
        // An example where the chosen cover depends on tie breaks in the priority queue
        Map(
          "a" -> Set(1, 2),
          "b" -> Set(2, 3),
          "c" -> Set(1, 3),
        ) -> Seq(Seq("a", "b"), Seq("a", "c"), Seq("b", "c")),
        // Duplicate sets
        Map(
          "a" -> Set(1, 2),
          "b" -> Set(1, 2),
        ) -> Seq(Seq("a"), Seq("b")),
      )

      forAll(testCases) { case (sets, expected) =>
        val cover = SetCover.greedy(sets)
        // Do not return duplicate elements
        cover.distinct shouldBe cover
        expected.map(_.toSet) should contain(cover.toSet)
      }
    }

    "be linear if the sets are disjoint" in {
      // This test should take less than a second.
      // If it runs any longer, there is likely an performance issue with the implementation
      val size = 50000
      val sets = (1 to size).map(i => i -> Set(i)).toMap
      SetCover.greedy(sets) should have size size.toLong
    }

    "be linear if the sets overlap" in {
      // This test should tak eless than a second
      // If it runs any longer, there is likely an performance issue with the implementation
      val rows = 300
      // Creates the following sets ri and ci up to `rows`:
      // r0: 0-0
      // r1: 1-0  1-1 1-1'
      // r2: 2-0  2-1 2-1'  2-2 2-2'
      // r3: 3-0  3-1 3-1'  3-2 3-2' 3-3 3-3'
      // r4: 4-0  4-1 4-1'  4-2 4-2' 4-3 4-3' 4-4 4-4'
      //
      // c1: 1-1 1-1'  2-2 2-2'  3-3 3-3'  4-4 4-4'
      // c2: 1-1 1-1'  2-1 2-1'  3-2 3-2'  4-3 4-3'
      // c3: 1-1 1-1'  2-1 2-1'  3-1 3-1'  4-2 4-2'
      // c4: 1-1 1-1'  2-1 2-1'  3-1 3-1'  4-1 4-1'
      //
      // The greedy algorithm will always include the last available row.
      // In between, it must update all the columns until all of their elements have been removed.

      def row(i: Int): (String, Set[String]) =
        s"r$i" -> (s"$i-0" +: (1 to i).flatMap(j => Seq(s"$i-$j", s"$i-$j'"))).toSet
      def col(j: Int): (String, Set[String]) =
        s"c$j" -> ((1 to j).flatMap(i => Seq(s"$i-1", s"$i-1'")) ++ (j + 1 to rows).flatMap(i =>
          Seq(s"$i-${i - j + 1}", s"$i-${i - j + 1}'")
        )).toSet

      val sets = ((0 to rows).map(row) ++ (1 to rows).map(col)).toMap
      SetCover.greedy(sets) shouldBe ((0 to rows).map(i => s"r$i").reverse)
    }
  }
}
