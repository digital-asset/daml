// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.nonempty

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import shapeless.test.illTyped

class NonEmptySpec extends AnyWordSpec with Matchers {
  import scala.{collection => col}, col.{immutable => imm}

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
    import NonEmptyColl.RefinedOps._

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
}
