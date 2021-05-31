// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.collection.immutable.Seq

class QueriesSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
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
}
