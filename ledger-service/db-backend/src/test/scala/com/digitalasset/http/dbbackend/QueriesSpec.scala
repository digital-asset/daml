// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import doobie.implicits._
import com.daml.nonempty.NonEmpty
import org.scalatest.Inspectors.{forAll => cForAll}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks => STSC}
import STSC.{forAll => scForAll}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.{immutable => imm}
import scalaz.\/
import scalaz.std.anyVal._
import scalaz.std.map._
import scalaz.std.set._
import scalaz.std.vector._
import scalaz.syntax.foldable._
import scalaz.syntax.std.map._

class QueriesSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import QueriesSpec._

  "projectedIndex" should {
    import Queries.{projectedIndex, SurrogateTpId}

    val cases = Table(
      ("map", "projection"),
      (
        Seq((SurrogateTpId(55L), fr"foo") -> 0),
        sql"CASE WHEN (tpid = ${55L}) THEN ${0}||''  ELSE NULL END ",
      ),
      (
        Seq((SurrogateTpId(55L), fr"foo") -> 0, (SurrogateTpId(66L), fr"foo") -> 1),
        sql"CASE WHEN (tpid = ${55L}) THEN ${0}||''  WHEN (tpid = ${66L}) THEN ${1}||''  ELSE NULL END ",
      ),
      (
        Seq(
          (SurrogateTpId(55L), fr"foo") -> 0,
          (SurrogateTpId(55L), fr"bar") -> 1,
          (SurrogateTpId(66L), fr"baz") -> 2,
        ),
        sql"CASE WHEN (tpid = ${55L}) THEN " ++
          sql"(CASE WHEN (foo ) THEN ${0}||',' ELSE '' END) || (CASE WHEN (bar ) THEN ${1}||',' ELSE '' END) " ++
          sql" WHEN (tpid = ${66L}) THEN ${2}||''  ELSE NULL END ",
      ),
    )

    "yield expected expressions for sample inputs" in forEvery(cases) { (map, projection) =>
      val frag = projectedIndex(map, sql"tpid")
      frag.toString should ===(projection.toString)
      fragmentElems(frag) should ===(fragmentElems(projection))
    }
  }

  "groupUnsyncedOffsets" should {
    import Queries.groupUnsyncedOffsets
    "not drop duplicate template IDs" in {
      groupUnsyncedOffsets(Set.empty, Vector((0, (1, 2)), (0, (3, 4)))) should ===(
        Map(0 -> Map(1 -> 2, 3 -> 4))
      )
    }

    "add empty maps for template IDs missing from the input" in {
      groupUnsyncedOffsets(Set(0, 1, 2), Vector((0, (1, 2)), (0, (3, 4)))) should ===(
        Map(0 -> Map(1 -> 2, 3 -> 4), 1 -> Map.empty, 2 -> Map.empty)
      )
    }
  }

  "chunkBySetSize" should {
    import org.scalacheck.{Arbitrary, Gen, Shrink}
    import Arbitrary.arbitrary
    import Queries.chunkBySetSize

    type Arg = NonEmpty[Map[Int, NonEmpty[Set[Int]]]]
    val sizes: Gen[Int] = Gen.choose(1, 20)
    implicit val doNotShrinkSize: Shrink[Int] = Shrink.shrinkAny
    val randomArg: Gen[Arg] = Gen
      .nonEmptyMap(
        Gen.zip(
          arbitrary[Int],
          Gen.nonEmptyContainerOf[Set, Int](arbitrary[Int]).map { case NonEmpty(xs) => xs },
        )
      )
      .map { case NonEmpty(xs) => xs }
    implicit def shrinkNE[Self <: imm.Iterable[Any]: Shrink]: Shrink[NonEmpty[Self]] =
      Shrink { nes => Shrink shrink nes.forgetNE collect { case NonEmpty(s) => s } }

    // at 1k each test takes ~500ms; at 10k each ~5s
    implicit val generatorDrivenConfig: STSC.PropertyCheckConfiguration =
      STSC.generatorDrivenConfig.copy(minSuccessful = 1000)

    "include all arguments in the result" in scForAll(sizes, randomArg) { (s, r) =>
      chunkBySetSize(s, r).foldMap1Opt(identity) should ===(Some(r))
    }

    def measuredChunkSize[K, V](chunk: NonEmpty[Map[K, NonEmpty[Set[V]]]]) =
      chunk.toNEF.foldMap(_.size)

    "never exceed size in each chunk" in scForAll(sizes, randomArg) { (s, r) =>
      all(chunkBySetSize(s, r) map measuredChunkSize) should be <= s
    }

    "make chunks as large as possible" in scForAll(sizes, randomArg) { (s, r) =>
      all(chunkBySetSize(s, r).init map measuredChunkSize) should ===(s)
    }

    "make chunks that do not intersect" in scForAll(sizes, randomArg) { (s, r) =>
      cForAll(chunkBySetSize(s, r).sliding(2, 1).toSeq) {
        case Seq(a, b) => all(a.forgetNE.intersectWith(b)(_ intersect _).values) shouldBe empty
        case Seq(_) => succeed
        case _ => fail("impossible sliding or empty output")
      }
    }
  }
}

object QueriesSpec {
  // XXX dedup with ValuePredicateTest
  import cats.data.Chain, doobie.util.fragment.{Elem, Fragment}

  private def fragmentElems(frag: Fragment): Chain[Any \/ Option[Any]] = {
    import language.reflectiveCalls, Elem.{Arg, Opt}
    frag.asInstanceOf[{ def elems: Chain[Elem] }].elems.map {
      case Arg(a, _) => \/.left(a)
      case Opt(o, _) => \/.right(o)
    }
  }
}
