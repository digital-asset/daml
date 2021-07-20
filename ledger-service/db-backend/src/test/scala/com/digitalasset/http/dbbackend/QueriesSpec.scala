// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      (Seq((SurrogateTpId(55L), fr"foo") -> 0),
        sql"CASE WHEN ( tpid = ${55L}) THEN ${0}||''  ELSE NULL END "),
    )

    "yield expected expressions for sample inputs" in forEvery(cases) {(map, projection) =>
      val frag = projectedIndex(map, sql"tpid")
      frag.toString should ===(projection.toString)
      fragmentElems(frag) should ===(fragmentElems(projection))
    }
  }
  /*
  "uniqueSets" should {
    import Queries.uniqueSets
    "return only a map if given and non-empty" in forAll { (m: Map[Int, Int], c: (Int, Int)) =>
      val nem = m + c
      uniqueSets(nem) should ===(Seq(nem))
    }

    "return empty for empty" in {
      uniqueSets(Seq.empty[(Int, Int)]) should ===(Seq.empty)
    }

    "return n elements in order if all given the same key" in forAll { xs: Seq[Int] =>
      uniqueSets(xs map ((42, _))) should ===(xs map (x => Map(42 -> x)))
    }

    "always return all elements, albeit in different order" in forAll { xs: Seq[(Byte, Int)] =>
      uniqueSets(xs).flatten should contain theSameElementsAs xs
    }

    "never produce an empty map" in forAll { xs: Seq[(Int, Int)] =>
      uniqueSets(xs) shouldNot contain(Map.empty)
    }
  }
   */
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