// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class RelationTest extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {

  // an empty map and a map with exclusively empty values represent
  // the same relationship but the underlying structure is different
  private val nonEmptyRelations: Gen[Relation[Int, Char]] =
    arbitrary[Map[Int, Set[Char]]].suchThat(_.values.forall(_.nonEmpty))

  property("invert andThen invert == identity for non empty relations") {
    forAll(nonEmptyRelations) { nonEmpty: Map[Int, Set[Char]] =>
      nonEmpty shouldEqual Relation.invert(Relation.invert(nonEmpty))
    }
  }

  property("union commutative") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]]) =>
      Relation.union(m1, m2) shouldEqual Relation.union(m2, m1)
    }
  }

  property("union associative") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]], m3: Map[Int, Set[Char]]) =>
      Relation.union(Relation.union(m1, m2), m3) shouldEqual Relation.union(
        m1,
        Relation.union(m2, m3),
      )
    }
  }

  property("union has unit") {
    forAll { m: Map[Int, Set[Char]] =>
      Relation.union(m, Relation.empty[Int, Char]) shouldEqual m
      Relation.union(Relation.empty[Int, Char], m) shouldEqual m
    }
  }

  property("flattening is the inverse of grouping for non empty relati`ons") {
    forAll(nonEmptyRelations) { nonEmpty =>
      Relation
        .flatten(nonEmpty)
        .toSeq
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2).toSet)
        .toMap shouldEqual nonEmpty
    }
  }
}
