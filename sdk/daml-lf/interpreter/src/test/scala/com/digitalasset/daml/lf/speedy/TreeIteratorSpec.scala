// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import org.scalatest.{Assertion, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TreeIteratorSpec extends AnyWordSpec with Inside with Matchers {

  "TreeIterator" should {

    "return all reachable nodes in a tree" in {
      def fullBinaryTree(limit: Int)(node: Int): Iterator[Int] =
        Iterator(node * 2, node * 2 + 1).filter(_ <= limit)

      assertSameElements(new TreeIterator(fullBinaryTree(10))(1), 1 to 10)
      assertSameElements(new TreeIterator(fullBinaryTree(1))(1), Seq(1))
    }

    "be stack-safe in depth" in {
      def deepTree(depth: Int)(node: Int): Iterator[Int] =
        if (node < depth) Iterator(node + 1)
        else Iterator.empty

      assertSameElements(new TreeIterator(deepTree(100000))(1), 1 to 100000)
    }

    "be stack-safe in breadth" in {
      def wideTree(width: Int)(node: Int): Iterator[Int] =
        if (node == 0) (2 to width).iterator
        else Iterator.empty

      assertSameElements(new TreeIterator(wideTree(100000))(1), 1 to 100000)
    }

    "support infinitely wide and deep trees" in {
      def allPositiveInts: Iterator[BigInt] = Iterator.iterate(BigInt(1))(_ + 1)

      def infiniteTree(node: List[BigInt]): Iterator[List[BigInt]] =
        allPositiveInts.map(i => i :: node)

      // A depth-first or breadth-first traversal would never reach the node [3, 3].
      // Dove-tailing does.
      new TreeIterator(infiniteTree)(List.empty).find(_ == List(3, 3)) shouldBe Some(List(3, 3))
    }
  }

  def assertSameElements[A: Ordering](iterator: Iterator[A], expected: Seq[A]): Assertion = {
    val actual = iterator.toSeq
    // `should contain theSameElementsInOrderAs` does not work for our large tests
    // because the runtime is quadratic
    actual.sorted shouldBe expected.sorted
  }
}
