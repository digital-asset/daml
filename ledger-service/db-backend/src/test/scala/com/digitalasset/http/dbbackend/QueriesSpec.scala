// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import doobie.implicits._
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.immutable.Seq
import scalaz.\/

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
