// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
class RelationTest extends PropSpec with Matchers with PropertyChecks {
  import Relation.Relation._
  property("invert andThen invert == identity for non empty relations") {
    forAll { m: Map[Int, Set[Char]] =>
      {
        // see notes in Relation.scala
        // Map() and Map(0 -> Set(), 1 -> Set())
        // are the same relations, but not the same scala maps
        whenever(m.values.forall(_.nonEmpty)) {
          m shouldEqual invert(invert(m))
        }
      }
    }
  }

  property("union commutative") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]]) =>
      {
        union(m1, m2) shouldEqual union(m2, m1)
      }
    }
  }

  property("union associative") {
    forAll { (m1: Map[Int, Set[Char]], m2: Map[Int, Set[Char]], m3: Map[Int, Set[Char]]) =>
      {
        union(union(m1, m2), m3) shouldEqual union(m1, union(m2, m3))
      }
    }
  }

  property("union has unit") {
    forAll { (m: Map[Int, Set[Char]]) =>
      {
        union(m, Map.empty[Int, Set[Char]]) shouldEqual m
        union(Map.empty[Int, Set[Char]], m) shouldEqual m
      }
    }
  }
}
