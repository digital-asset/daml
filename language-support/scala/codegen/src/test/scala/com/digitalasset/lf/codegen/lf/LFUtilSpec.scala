// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen
package lf

import com.daml.lf.data.Ref
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.foldable1._
import scalaz.syntax.monad._

class LFUtilSpec extends AnyWordSpec with Matchers with Inside with ScalaCheckPropertyChecks {
  import LFUtilSpec._

  "escapeReservedName" should {
    lazy val reservedNames: Gen[Ref.Name] =
      Gen
        .oneOf(
          Gen.oneOf("_root_", "asInstanceOf", "notifyAll", "wait", "toString"),
          Gen.lzy(reservedNames).map(n => s"${n}_"),
        )
        .map(Ref.Name.assertFromString)

    "reserve names injectively" in forAll(reservedNames, Gen.chooseNum(1, 100)) { (name, n) =>
      1.to(n).foldLeft((Set(name), name)) { case ((names, name), _) =>
        inside(LFUtil.escapeReservedName(name)) {
          case -\/(newName) => // escaping never un-reserves the name
            names should not contain newName
            (names + newName, newName)
        }
      }
    }

    // right-hand behavior is guaranteed by FP and the return type
  }

  private[this] val tupleNestingSamples = Table(
    ("root", "subtrees", "flat", "nested"),
    (5, 5, NonEmptyList(1, 2, 3, 4, 5, 6, 7), "(1, 2, 3, 4, (5, 6, 7))"),
    (4, 3, NonEmptyList(1, 2, 3, 4, 5, 6, 7), "(1, 2, (3, 4), (5, 6, 7))"),
    (2, 2, NonEmptyList(1, 2, 3, 4, 5, 6, 7), "((1, (2, 3)), ((4, 5), (6, 7)))"),
  )

  "tupleNesting" when {
    import LFUtil.{TupleNesting, tupleNesting}
    import Gen.{choose, containerOfN}

    val reasonableMax = 22
    val anyArity = choose(2, reasonableMax)

    def nelOf[A](choice: Gen[Int], vals: Gen[A] = arbitrary[Int]) =
      for {
        s <- choice
        elt <- vals
        c <- containerOfN[Seq, A](s - 1, vals)
      } yield NonEmptyList(elt, c: _*)

    "value is a sample" should {
      "group flatly, but with right-bias" in forEvery(tupleNestingSamples) {
        (root, subtrees, flat, nested) =>
          tupleNesting(flat, root, subtrees)
            .fold(_.toString)(_.list.toList.mkString("(", ", ", ")")) shouldBe nested
      }
    }

    "value fits in root size" should {
      val rsf = for {
        r <- anyArity
        s <- anyArity
        f <- nelOf(choose(1, r))
      } yield TupleNestingCall(r, s, f)

      "never nest" in forAll(rsf) { case TupleNestingCall(r, s, f) =>
        tupleNesting(f, r, s) shouldBe TupleNesting[Int](f map \/.left)
      }
    }

    "value is of any size" should {
      val rsf = for {
        r <- anyArity
        s <- anyArity
        f <- nelOf(choose(1, reasonableMax * 8))
      } yield TupleNestingCall(r, s, f)

      "preserve all values, in order" in forAll(rsf) { case TupleNestingCall(r, s, f) =>
        tupleNesting(f, r, s).fold(NonEmptyList(_))(_.join) shouldBe f
      }

      "produce levels <= max sizes" in forAll(rsf) { case TupleNestingCall(r, s, f) =>
        def visit(nesting: TupleNesting[Int], max: Int): Unit = {
          nesting.run.size should be <= max
          nesting.run.foreach(_.fold(_ => (), visit(_, s)))
        }
        visit(tupleNesting(f, r, s), r)
      }

      "preserve minimum tree height" in forAll(rsf) { case TupleNestingCall(r, s, f) =>
        val height = tupleNesting(f, r, s).fold(_ => 0)(_.maximum1 + 1)
        val capacityFloor =
          if (height == 1) 0 else r * math.pow(s.toDouble, (height - 2).toDouble).toInt
        capacityFloor should be < f.size
      }
    }
  }
}

object LFUtilSpec {
  import org.scalacheck.Shrink

  final case class TupleNestingCall[A](r: Int, s: Int, f: NonEmptyList[A])

  implicit def shrTnc: Shrink[TupleNestingCall[Int]] =
    Shrink { tnc =>
      Shrink.shrink(tnc.f.zipWithIndex.map(_._2)) map (newF => tnc.copy(f = newF))
    }

  private[this] implicit def shrNel[A]: Shrink[NonEmptyList[A]] = // suppressing A's shrink
    Shrink { nela =>
      Shrink.shrink((nela.head, nela.tail.toVector)) map { case (h, t) => NonEmptyList(h, t: _*) }
    }
}
