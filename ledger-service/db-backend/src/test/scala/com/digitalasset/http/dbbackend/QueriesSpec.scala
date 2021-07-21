// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import doobie.Fragment
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

    final case class Reordered(s_2_13: Fragment, s_2_12: Fragment)
    def Agnostic(fragment: Fragment) = Reordered(fragment, fragment)

    val cases = Table(
      ("map", "projection"),
      (
        Seq((SurrogateTpId(55L), fr"foo") -> 0),
        Agnostic(sql"CASE WHEN ( tpid = ${55L}) THEN ${0}||''  ELSE NULL END "),
      ),
      (
        Seq((SurrogateTpId(55L), fr"foo") -> 0, (SurrogateTpId(66L), fr"foo") -> 1),
        Reordered(
          s_2_12 =
            sql"CASE WHEN ( tpid = ${55L}) THEN ${0}||''  WHEN ( tpid = ${66L}) THEN ${1}||''  ELSE NULL END ",
          s_2_13 =
            sql"CASE WHEN ( tpid = ${66L}) THEN ${1}||''  WHEN ( tpid = ${55L}) THEN ${0}||''  ELSE NULL END ",
        ),
      ),
      (
        Seq(
          (SurrogateTpId(55L), fr"foo") -> 0,
          (SurrogateTpId(55L), fr"bar") -> 1,
          (SurrogateTpId(66L), fr"baz") -> 2,
        ),
        Reordered(
          s_2_13 = sql"CASE WHEN ( tpid = ${66L}) THEN ${2}||''  WHEN ( tpid = ${55L}) THEN " ++
            sql"(CASE WHEN (foo ) THEN ${0}||',' ELSE '' END) || (CASE WHEN (bar ) THEN ${1}||',' ELSE '' END)  ELSE NULL END ",
          s_2_12 = sql"CASE WHEN ( tpid = ${55L}) THEN " ++
            sql"(CASE WHEN (foo ) THEN ${0}||',' ELSE '' END) || (CASE WHEN (bar ) THEN ${1}||',' ELSE '' END) " ++
            sql" WHEN ( tpid = ${66L}) THEN ${2}||''  ELSE NULL END ",
        ),
      ),
    )

    "yield expected expressions for sample inputs" in forEvery(cases) { (map, projections) =>
      val frag = projectedIndex(map, sql"tpid")
      val projection = scala.util.Properties.releaseVersion.map(_.take(4)) match {
        case Some("2.12") => projections.s_2_12
        case Some(_) | None => projections.s_2_13
      }
      frag.toString should ===(projection.toString)
      fragmentElems(frag) should ===(fragmentElems(projection))
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
