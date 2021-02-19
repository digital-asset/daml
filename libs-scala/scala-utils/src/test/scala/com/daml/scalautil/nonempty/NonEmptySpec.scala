// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import shapeless.test.illTyped

class NonEmptySpec extends AnyWordSpec with Matchers {
  import scala.{collection => col}, col.{mutable => mut}, col.{immutable => imm}

  "unapply" should {
    "compile on immutable maps" in {
      val NonEmpty(m) = imm.Map(1 -> 2)
      (m: NonEmpty[imm.Map[Int, Int]]) should ===(imm.Map(1 -> 2))
    }

    "compile on immutable seqs and maps" in {
      val NonEmpty(s) = imm.Seq(3)
      (s: NonEmpty[imm.Seq[Int]]) should ===(imm.Seq(3))
    }

    "reject empty maps" in {
      imm.Map.empty[Int, Int] match {
        case NonEmpty(_) => fail("empty")
        case _ => succeed
      }
    }

    "reject empty seqs" in {
      imm.Seq.empty[Int] match {
        case NonEmpty(_) => fail("empty")
        case _ => succeed
      }
    }
  }

  "groupBy1" should {
    import NonEmptyReturningOps._

    // wrapping with Set in a variable is a nice trick to disable subtyping and
    // implicit conversion (strong and weak conformance, SLS §3.5.2-3), so you
    // only see what an expression really infers as exactly

    "produce Lists for Lists" in {
      val g = Set(List(1) groupBy1 identity)
      g: Set[imm.Map[Int, NonEmpty[List[Int]]]]
    }

    "produce Vectors for Vectors" in {
      val g = Set(Vector(1) groupBy1 identity)
      g: Set[imm.Map[Int, NonEmpty[Vector[Int]]]]
    }

    "produce Sets for Sets" in {
      val g = Set(imm.Set(1) groupBy1 identity)
      g: Set[imm.Map[Int, NonEmpty[imm.Set[Int]]]]
    }

    "produce Seqs for Seqs" in {
      val g = Set(imm.Seq(1) groupBy1 identity)
      g: Set[imm.Map[Int, NonEmpty[imm.Seq[Int]]]]
    }

    "reject maybe-mutable structures" in {
      illTyped(
        "col.Seq(1) groupBy1 identity",
        "(?s).*?groupBy1 is not a member of (scala.collection.)?Seq.*",
      )
    }
  }

  "toF" should {
    "destructure the Set type" in {
      val NonEmpty(s) = imm.Set(1)
      val g = Set(s.toF)
      g: Set[NonEmptyF[imm.Set, Int]]
    }

    "destructure the Map type" in {
      val NonEmpty(m) = Map(1 -> 2)
      val g = Set(m.toF)
      g: Set[NonEmptyF[Map[Int, *], Int]]
    }

    "allow underlying NonEmpty operations" in {
      val NonEmpty(s) = imm.Set(1)
      ((s.toF incl 2): NonEmpty[imm.Set[Int]]) should ===(imm.Set(1, 2))
    }

    "allow access to Scalaz methods" in {
      import scalaz.syntax.functor._, scalaz.std.map._
      val NonEmpty(m) = imm.Map(1 -> 2)
      (m.toF.map((3, _)): NonEmptyF[imm.Map[Int, *], (Int, Int)]) should ===(imm.Map(1 -> ((3, 2))))
    }
  }

  "+-:" should {
    val NonEmpty(s) = Vector(1, 2)

    "preserve its tail type" in {
      val h +-: t = s
      ((h, t): (Int, Vector[Int])) should ===((1, Vector(2)))
    }

    "have ±: alias" in {
      val h ±: t = s
      ((h, t): (Int, Vector[Int])) should ===((1, Vector(2)))
    }
  }

  // why we don't allow `scala.collection` types
  "scala.collection.Seq" must {
    "accept that its non-emptiness is ephemeral" in {
      val ms = mut.Buffer(1)
      val cs: col.Seq[Int] = ms
      val csIsNonEmpty = cs.nonEmpty
      ms.clear()
      (csIsNonEmpty, cs) should ===((true, col.Seq.empty))
    }
  }
}
