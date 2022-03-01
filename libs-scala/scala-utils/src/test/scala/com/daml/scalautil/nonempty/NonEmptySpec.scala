// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import shapeless.test.illTyped

class NonEmptySpec extends AnyWordSpec with Matchers {
  import scala.{collection => col}, col.{mutable => mut}, col.{immutable => imm}

  "apply" should {
    "lub arguments" in {
      val s = NonEmpty(imm.Set, Left(1), Right("hi"))
      (s: NonEmpty[imm.Set[Either[Int, String]]]) should ===(s)
    }
  }

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

  "updated" should {
    val m = NonEmpty(imm.HashMap, 1 -> 2)

    "preserve the map type" in {
      (m.updated(1, 2): NonEmpty[imm.HashMap[Int, Int]]) should ===(m)
    }

    "preserve a wider map type" in {
      val nhm = (m: NonEmpty[Map[Int, Int]]).updated(1, 2)
      illTyped(
        "nhm: NonEmpty[imm.HashMap[Int, Int]]",
        "(?s)type mismatch.*?found.*?\\.Map.*?required.*?HashMap.*",
      )
      (nhm: NonEmpty[Map[Int, Int]]) should ===(m)
    }
  }

  "to" should {
    "accept weird type shapes" in {
      val sm = NonEmpty(Map, 1 -> 2).to(imm.HashMap)
      (sm: NonEmpty[imm.HashMap[Int, Int]]) shouldBe an[imm.HashMap[_, _]]
    }
  }

  "+-:" should {
    val NonEmpty(s) = Vector(1, 2)

    "preserve its tail type" in {
      val h +-: t = s
      ((h, t): (Int, Vector[Int])) should ===((1, Vector(2)))
    }

    "restructure when used as a method" in {
      val h +-: t = s
      import NonEmptyReturningOps._
      (h +-: t: NonEmpty[Vector[Int]]) should ===(s)
    }

    "have ±: alias" in {
      val h ±: t = s
      ((h, t): (Int, Vector[Int])) should ===((1, Vector(2)))
    }
  }

  "map" should {
    "'work' on sets, so to speak" in {
      val r = NonEmpty(Set, 1, 2) map (_ + 2)
      (r: NonEmpty[Set[Int]]) should ===(NonEmpty(Set, 3, 4))
    }

    "turn Maps into non-Maps" in {
      val m: NonEmpty[Map[Int, Int]] = NonEmpty(Map, 1 -> 2, 3 -> 4)
      val r = m map (_._2)
      ((r: NonEmpty[imm.Iterable[Int]]): imm.Iterable[Int]) should contain theSameElementsAs Seq(
        2,
        4,
      )
    }
  }

  "flatMap" should {
    "'work' on sets, so to speak" in {
      val r = NonEmpty(Set, 1, 2) flatMap (n => NonEmpty(List, n + 3, n + 5))
      (r: NonEmpty[Set[Int]]) should ===(NonEmpty(Set, 1 + 3, 1 + 5, 2 + 3, 2 + 5))
    }

    "reject possibly-empty function returns" in {
      illTyped(
        "(_: NonEmpty[List[Int]]) flatMap (x => List(x))",
        "(?s)type mismatch.*?found.*?List.*?required.*?NonEmpty.*",
      )
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
