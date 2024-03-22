// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class RelationTest extends AnyPropSpec with Matchers with ScalaCheckPropertyChecks {

  import Relation.Relation._

  // an empty map and a map with exclusively empty values represent
  // the same relationship but the underlying structure is different
  private val nonEmptyRelations: Gen[Map[Int, Set[Char]]] =
    arbitrary[Map[Int, Set[Char]]].suchThat(_.values.forall(_.nonEmpty))

  property("invert andThen invert == identity for non empty relations") {
    forAll(nonEmptyRelations) { nonEmpty: Map[Int, Set[Char]] =>
      nonEmpty shouldEqual invert(invert(nonEmpty))
    }
  }

  property("union commutative") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]]) =>
      union(m1, m2) shouldEqual union(m2, m1)
    }
  }

  property("union associative") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]], m3: Map[Int, Set[Char]]) =>
      union(union(m1, m2), m3) shouldEqual union(m1, union(m2, m3))
    }
  }

  property("union has unit") {
    forAll { m: Map[Int, Set[Char]] =>
      union(m, Map.empty[Int, Set[Char]]) shouldEqual m
      union(Map.empty[Int, Set[Char]], m) shouldEqual m
    }
  }

  property("flattening is the inverse of grouping for non empty relations") {
    forAll(nonEmptyRelations) { nonEmpty =>
      flatten(nonEmpty).toSeq
        .groupBy(_._1)
        .view
        .mapValues(_.map(_._2).toSet)
        .toMap shouldEqual nonEmpty
    }
  }

  property("diff is idempotent") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]]) =>
      diff(m1, m2) shouldEqual diff(diff(m1, m2), m2)
    }
  }

  property("diff by empty doesn't affect non-empty relations") {
    forAll(nonEmptyRelations) { m =>
      diff(m, Map.empty[Int, Set[Char]]) shouldEqual m
    }
  }

  property("diff: no item in the right operand appears in the result") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]]) =>
      val result = flatten(diff(m1, m2)).toList
      val right = flatten(m2).toList
      result should contain noElementsOf right
    }
  }

  property("diff: items in the result should be a subset of the ones in the left operand") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]]) =>
      val result = flatten(diff(m1, m2)).toSet
      val left = flatten(m1).toSet
      assert(result.subsetOf(left))
    }
  }

  property("diff is equivalent to flatten-and-diff") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]]) =>
      flatten(diff(m1, m2)).toSet shouldEqual flatten(m1).toSet.diff(flatten(m2).toSet)
    }
  }
}
