// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Eq
import cats.laws.discipline.ApplyTests
import com.digitalasset.canton.BaseTestWordSpec
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable
import scala.util.{Success, Try}

class RoseTreeTest extends AnyWordSpec with BaseTestWordSpec {

  private def recursiveMap[A, B](tree: RoseTree[A])(f: A => B): RoseTree[B] =
    RoseTree(f(tree.root), tree.children.map(recursiveMap(_)(f))*)

  private def recursivePreorder[A](tree: RoseTree[A]): Seq[A] =
    tree.root +: tree.children.flatMap(recursivePreorder)

  private def recursiveHashcode[A](tree: RoseTree[A]): Int =
    scala.util.hashing.MurmurHash3.productHash(tree)

  private def recursiveSize[A](tree: RoseTree[A]): Int = 1 + tree.children.map(recursiveSize).sum

  private def recursiveZipWith[A, B, C](left: RoseTree[A], right: RoseTree[B])(
      f: (A, B) => C
  ): RoseTree[C] =
    RoseTree(
      f(left.root, right.root),
      left.children.zip(right.children).map { case (l, r) => recursiveZipWith(l, r)(f) }*
    )

  private def recursiveFoldl[A, State, Result](tree: RoseTree[A])(
      init: RoseTree[A] => State
  )(finish: State => Result)(update: (State, Result) => State): Result =
    finish(
      tree.children.foldLeft(init(tree))((acc, child) =>
        update(acc, recursiveFoldl(child)(init)(finish)(update))
      )
    )

  private val width = 15000
  private def wideTree(multiplier: Int): RoseTree[Int] =
    RoseTree(0, (1 to width).map(i => RoseTree(i * multiplier))*)

  private val depth = 15000
  def deepTree(multiplier: Int): RoseTree[Int] =
    (1 to depth).foldRight(RoseTree(0))((i, acc) => RoseTree(i * multiplier, acc))

  val trees = Table(
    "trees",
    RoseTree(1),
    RoseTree(1, RoseTree(2), RoseTree(3)),
    RoseTree(1, RoseTree(2, RoseTree(4), RoseTree(5)), RoseTree(3)),
    RoseTree(1, RoseTree(2, RoseTree(4, RoseTree(6), RoseTree(7)), RoseTree(5)), RoseTree(3)),
  )

  "size" should {
    "correctly count the size" in {
      forEvery(trees) { tree =>
        tree.size shouldEqual recursiveSize(tree)
      }
    }

    "be stack safe for wide trees" in {
      wideTree(1).size shouldBe (width + 1)
    }

    "be stack safe for deep trees" in {
      deepTree(1).size shouldBe (depth + 1)
    }
  }

  "equals" should {
    "be correct" in {
      forEvery(trees.zipWithIndex) { case (first, i) =>
        forEvery(trees.zipWithIndex) { case (second, j) =>
          (first == second) shouldEqual (i == j)
        }
      }

      RoseTree(1) shouldEqual RoseTree(1)
    }

    "be stack safe for wide trees" in {
      (wideTree(1) == wideTree(2)) shouldBe false
      (wideTree(1) == wideTree(1)) shouldBe true
    }

    "be stack safe for deep trees" in {
      (deepTree(1) == deepTree(2)) shouldBe false
      (deepTree(1) == deepTree(1)) shouldBe true
    }
  }

  "hashcode" should {
    "produce different hashes for different trees" in {
      val hashes = trees.map(_.hashCode())
      hashes.distinct shouldBe hashes
    }

    "implement the specification" in {
      trees(0).hashCode() shouldBe recursiveHashcode(trees(0))

      trees.map(_.hashCode()) shouldBe trees.map(recursiveHashcode)
    }

    "be stack safe for wide trees" in {
      Try(wideTree(1).hashCode()) shouldBe a[Success[?]]
    }

    "be stack safe for deep trees" in {
      Try(deepTree(1).hashCode()) shouldBe a[Success[?]]
    }
  }

  "preorder" should {
    "be correct" in {
      forEvery(trees) { tree =>
        tree.preorder.toSeq shouldEqual recursivePreorder(tree)
      }
    }

    "be stack safe for wide trees" in {
      wideTree(1).preorder.toSeq shouldEqual (0 to width)
    }

    "be stack safe for deep trees" in {
      deepTree(1).preorder.toSeq shouldEqual ((1 to depth) :+ 0)
    }
  }

  "toString" should {
    "be reasonable" in {
      RoseTree(1).toString shouldBe "RoseTree(1)"
      RoseTree(
        1,
        RoseTree(2),
        RoseTree(3),
      ).toString shouldBe "RoseTree(1, RoseTree(2), RoseTree(3))"
    }

    "be stack safe for wide trees" in {
      wideTree(1).toString should startWith("RoseTree(0, RoseTree(1), RoseTree(2), RoseTree(3)")
    }

    "be stack safe for deep trees" in {
      deepTree(1).toString should startWith("RoseTree(1, RoseTree(2, RoseTree(3, RoseTree(4")
    }
  }

  "foldl" should {
    "be correct" in {
      forEvery(trees) { tree =>
        def init(builder: mutable.Builder[String, Seq[String]])(t: RoseTree[Int]): Int = {
          builder += s"Init($t)"
          1
        }
        def finish(builder: mutable.Builder[String, Seq[String]])(i: Int): Int = {
          builder += s"Fisnish($i)"
          i + 1
        }
        def update(builder: mutable.Builder[String, Seq[String]])(acc: Int, res: Int): Int = {
          builder += s"Update($acc, $res)"
          acc + res
        }

        val callsB = Seq.newBuilder[String]
        val specsB = Seq.newBuilder[String]

        val folded = tree.foldl(init(callsB))(finish(callsB))(update(callsB))
        val calls = callsB.result()

        val speced = recursiveFoldl(tree)(init(specsB))(finish(specsB))(update(specsB))
        val specs = specsB.result()

        folded shouldEqual speced
        calls shouldEqual specs
      }
    }

    "be stack safe for wide trees" in {
      wideTree(1).foldl(_ => 1)(Predef.identity)(_ + _) shouldEqual width + 1
    }

    "be stack safe for deep trees" in {
      deepTree(1).foldl(_ => 1)(Predef.identity)(_ + _) shouldEqual depth + 1
    }
  }

  "map" should {
    "be correct" in {
      forEvery(trees) { tree =>
        val mapped = tree.map(_ * 2)
        mapped shouldEqual recursiveMap(tree)(_ * 2)
        mapped.size shouldEqual tree.size
      }
    }

    "visit the nodes in preorder" in {
      forEvery(trees) { tree =>
        val visited = Seq.newBuilder[Int]
        def visit(i: Int): Int = {
          visited += i
          i
        }
        tree.map(visit) shouldBe tree
        visited.result() shouldBe tree.preorder.toSeq
      }
    }

    "be stack safe for wide trees" in {
      wideTree(1).map(_ * 2) shouldEqual wideTree(2)
    }

    "be stack safe for deep trees" in {
      deepTree(1).map(_ * 2) shouldEqual deepTree(2)
    }
  }

  "zipWith" should {
    "be correct" in {
      forEvery(trees) { left =>
        forEvery(trees) { right =>
          val zipped = left.zipWith(right)(_ + _)
          val spec = recursiveZipWith(left, right)(_ + _)
          zipped shouldEqual spec
          zipped.size shouldEqual spec.size
        }
      }
    }

    "be stack safe for wide trees" in {
      wideTree(1).zipWith(wideTree(2))(_ + _) shouldEqual wideTree(3)
    }

    "be stack safe for deep trees" in {
      deepTree(1).zipWith(deepTree(2))(_ + _) shouldEqual deepTree(3)
    }
  }

  "Apply" should {
    import RoseTreeTest.*
    checkAllLaws("Apply", ApplyTests[RoseTree].apply[Int, Int, String])
  }

}

object RoseTreeTest {
  implicit def eqRoseTree[A]: Eq[RoseTree[A]] = Eq.fromUniversalEquals

  private def arbitraryBoundedRoseTree[A: Arbitrary](
      depth: Int,
      width: Int,
  ): Arbitrary[RoseTree[A]] = Arbitrary {
    for {
      root <- Arbitrary.arbitrary[A]
      children <- Gen.listOfN(width, arbitraryBoundedRoseTree[A](depth - 1, width - 1).arbitrary)
    } yield RoseTree(root, children*)
  }

  implicit def arbitraryRoseTree[A: Arbitrary]: Arbitrary[RoseTree[A]] =
    arbitraryBoundedRoseTree(3, 3)
}
