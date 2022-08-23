// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import com.daml.scalatest.WordSpecCheckLaws

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scalaz.scalacheck.{ScalazProperties => SZP}
import scalaz.Foldable
import shapeless.test.illTyped

class NonEmptySpec extends AnyWordSpec with Matchers with WordSpecCheckLaws {
  import scala.{collection => col}, col.{mutable => mut}, col.{immutable => imm}
  import NonEmptySpec._

  "apply" should {
    "lub arguments" in {
      val s = NonEmpty(imm.Set, Left(1), Right("hi"))
      (s: NonEmpty[imm.Set[Either[Int, String]]]) should ===(s)
    }
  }

  "mk" should {
    "infer A from expected type" in {
      trait Animal
      trait Ok
      object Elephant extends Animal with Ok
      object Rhino extends Animal with Ok
      illTyped(
        "NonEmpty(imm.Set, Elephant, Rhino): imm.Set[Animal]",
        "(?s).*?cannot be instantiated to expected type.*",
      )
      val s = NonEmpty.mk(imm.Set, Elephant, Rhino)
      val sw = NonEmpty.mk(imm.Set, Elephant, Rhino): NonEmpty[imm.Set[Animal]]
      (s: imm.Set[Animal with Ok]) should ===(sw: imm.Set[Animal])
    }

    "impose enough expected type to cause implicit conversions of elements" in {
      object Foo
      object Bar
      import language.implicitConversions
      @annotation.nowarn("cat=unused&msg=foo")
      implicit def foobar(foo: Foo.type): Bar.type = Bar
      illTyped(
        "NonEmpty(List, Foo, Foo): NonEmpty[List[Bar.type]]",
        "No implicit view available from .*List.type => .*Factory\\[Foo.type,.*Iterable\\[Foo.type] with List\\[Bar.type]].",
      )
      val bars: NonEmpty[List[Bar.type]] = NonEmpty.mk(List, Foo, Foo)
      (bars: List[Bar.type]).head should ===(Bar)
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

  "toNEF" should {
    "destructure the Set type" in {
      val NonEmpty(s) = imm.Set(1)
      val g = Set(s.toNEF)
      g: Set[NonEmptyF[imm.Set, Int]]
    }

    "destructure the Map type" in {
      val NonEmpty(m) = Map(1 -> 2)
      val g = Set(m.toNEF)
      g: Set[NonEmptyF[Map[Int, *], Int]]
    }

    "allow underlying NonEmpty operations" in {
      val NonEmpty(s) = imm.Set(1)
      ((s.toNEF incl 2): NonEmpty[imm.Set[Int]]) should ===(imm.Set(1, 2))
    }

    "allow access to Scalaz methods" in {
      import scalaz.syntax.functor._, scalaz.std.map._
      val NonEmpty(m) = imm.Map(1 -> 2)
      (m.toNEF.map((3, _)): NonEmptyF[imm.Map[Int, *], (Int, Int)]) should ===(
        imm.Map(1 -> ((3, 2)))
      )
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

    "preserve sortedness" in {
      val sm = NonEmpty(imm.SortedMap, 1 -> 2)
      (sm.updated(1, 2): NonEmpty[imm.SortedMap[Int, Int]]) should ===(sm)
    }
  }

  "transform" should {
    val m = NonEmpty(imm.HashMap, 1 -> 2)
    "preserve the map type" in {
      (m.transform((_, _) => 2): NonEmpty[imm.HashMap[Int, Int]]) should ===(m)
    }

    "preserve a wider map type" in {
      val nhm = (m: NonEmpty[Map[Int, Int]]).transform((_, _) => 2)
      illTyped(
        "nhm: NonEmpty[imm.HashMap[Int, Int]]",
        "(?s)type mismatch.*?found.*?\\.Map.*?required.*?HashMap.*",
      )
      (nhm: NonEmpty[Map[Int, Int]]) should ===(m)
    }

    "preserve sortedness" in {
      val sm = NonEmpty(imm.SortedMap, 1 -> 2)
      (sm.transform((_, _) => 2): NonEmpty[imm.SortedMap[Int, Int]]) should ===(sm)
    }
  }

  "keySet" should {
    "retain nonempty" in {
      val m = NonEmpty(imm.HashMap, 1 -> 2)
      (m.keySet: NonEmpty[Set[Int]]) should ===(NonEmpty(Set, 1))
    }

    "retain sortedness" in {
      val sm = NonEmpty(imm.SortedMap, 1 -> 2)
      (sm.keySet: NonEmpty[imm.SortedSet[Int]]) should ===(NonEmpty(imm.SortedSet, 1))
    }
  }

  "unsorted" should {
    "retain nonempty" in {
      val sm = NonEmpty(imm.SortedMap, 1 -> 2)
      (sm.unsorted: NonEmpty[Map[Int, Int]]) should ===(sm)
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

  "++" should {
    "preserve non-emptiness for Sets" in {
      val set = NonEmpty(Set, 1, 2) ++ Iterable.empty
      (set: NonEmpty[Set[Int]]) should ===(NonEmpty(Set, 1, 2))
    }

    "preserve non-emptiness for Seqs" in {
      val seq = NonEmpty(Seq, 1, 2) ++ Iterable(1, 3)
      (seq: NonEmpty[Seq[Int]]) should ===(NonEmpty(Seq, 1, 2, 1, 3))
    }

    "preserve non-emptiness for Maps" in {
      val map = NonEmpty(Map, 1 -> "a") ++ Iterable.empty
      (map: NonEmpty[Map[Int, String]]) should ===(NonEmpty(Map, 1 -> "a"))
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

  "min" should {
    "'work' on sets, so to speak" in {
      val r = NonEmpty(Set, 1, 42, -5)
      r.min1 should ===(-5)
    }
  }

  "max" should {
    "'work' on sets, so to speak" in {
      val r = NonEmpty(Set, 1, 42, -5)
      r.max1 should ===(42)
    }
  }

  "minBy" should {
    "'work' on sets, so to speak" in {
      val r = NonEmpty(Set, "1", "42", "-5")
      r.minBy(_.toInt) should ===("-5")
    }
  }

  "maxBy" should {
    "'work' on sets, so to speak" in {
      val r = NonEmpty(Set, "1", "42", "-5")
      r.maxBy1(_.toInt) should ===("42")
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

  "Foldable" should {
    "prefer the substed version over the derived one" in {
      import scalaz.std.list._
      Foldable[NonEmptyF[List, *]].getClass should be theSameInstanceAs Foldable[List].getClass
    }
  }

  "Foldable1 from Foldable" should {
    import scalaz.std.vector._, scalaz.std.anyVal._
    checkLaws(SZP.foldable1.laws[NonEmptyF[Vector, *]])
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

  // merely checking that too much evidence doesn't result in ambiguous
  // lookup
  object UnambiguousResolutionTests {
    import scalaz.{Foldable, Traverse}
    def foldableTraverse[F[_]: Traverse]: Foldable[F] = Foldable[F]
    def foldableNot1[F[_]: Foldable]: Foldable[F] = Foldable[F]
  }
}

object NonEmptySpec {
  import org.scalacheck.Arbitrary, Arbitrary.arbitrary
  import NonEmptyReturningOps._
  import NonEmptyCollCompat.SeqOps

  implicit def `ne seq arb`[A: Arbitrary, C[X] <: Seq[X] with SeqOps[X, C, C[X]]](implicit
      C: Arbitrary[C[A]]
  ): Arbitrary[NonEmptyF[C, A]] =
    Arbitrary(arbitrary[(A, C[A])] map { case (hd, tl) => (hd +-: tl).toNEF })
}
