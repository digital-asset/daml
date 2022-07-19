// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class RelationTest extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {

  import Relation.{empty, flatten, invert, union}

  // an empty map and a map with exclusively empty values represent
  // the same relationship but the underlying structure is different
  private val nonEmptyRelations: Gen[Relation[Int, Char]] =
    arbitrary[Relation[Int, Char]].suchThat(_.values.forall(_.nonEmpty))

  property("invert andThen invert == identity for non empty relations") {
    forAll(nonEmptyRelations) { nonEmpty: Relation[Int, Char] =>
      nonEmpty shouldEqual invert(invert(nonEmpty))
    }
  }

  property("union commutative") {
    forAll { (m1: Relation[Int, Char], m2: Relation[Int, Char]) =>
      union(m1, m2) shouldEqual union(m2, m1)
    }
  }

  property("union associative") {
    forAll { (m1: Relation[Int, Char], m2: Relation[Int, Char], m3: Relation[Int, Char]) =>
      union(union(m1, m2), m3) shouldEqual union(m1, union(m2, m3))
    }
  }

  property("union has unit") {
    forAll { m: Relation[Int, Char] =>
      union(m, empty[Int, Char]) shouldEqual m
      union(empty[Int, Char], m) shouldEqual m
    }
  }

  property("flattening is the inverse of grouping for non empty relati`ons") {
    forAll(nonEmptyRelations) { nonEmpty =>
      flatten(nonEmpty).toSeq
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2).toSet)
        .toMap shouldEqual nonEmpty
    }
  }
}
