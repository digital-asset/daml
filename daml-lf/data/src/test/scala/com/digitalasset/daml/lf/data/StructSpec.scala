// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, WordSpec}

class StructSpec extends WordSpec with Matchers with PropertyChecks {

  private[this] val List(f1, f2, f3, f4) =
    List("f1", "f2", "f3", "f4").map(Ref.Name.assertFromString)

  "SortMap.toSeq" should {

    "sort fields" in {

      val testCases =
        Table(
          "list",
          List(),
          List(f1 -> 1),
          List(f1 -> 1, f2 -> 2, f3 -> 3),
          List(f2 -> 1, f3 -> 2, f1 -> 3),
          List(f3 -> 2, f2 -> 3, f1 -> 1),
        )

      forEvery(testCases) { list =>
        val struct = Struct.assertFromSeq(list)
        (struct.names zip struct.names.drop(1)).foreach {
          case (x, y) => (x: String) shouldBe <(y: String)
        }
      }

    }

    """reject struct with  duplicate name.""" in {

      val testCases =
        Table(
          "list",
          List(f1 -> 1, f1 -> 1),
          List(f1 -> 1, f1 -> 2),
          List(f1 -> 1, f2 -> 2, f3 -> 3, f1 -> 2),
          List(f2 -> 2, f3 -> 3, f2 -> 1, f3 -> 4, f3 -> 0),
        )

      forEvery(testCases) { list =>
        Struct.fromSeq(list) shouldBe 'left
      }

    }
  }

  "Struct" should {
    "be equal if built in different order" in {
      Struct.fromSeq(List(f1 -> 1, f2 -> 2)) shouldBe
        Struct.fromSeq(List(f2 -> 2, f1 -> 1))
      Struct.fromSeq(List(f1 -> 1, f2 -> 2, f3 -> 3)) shouldBe
        Struct.fromSeq(List(f2 -> 2, f1 -> 1, f3 -> 3))
      Struct.fromSeq(List(f1 -> 1, f2 -> 2, f3 -> 3)) shouldBe
        Struct.fromSeq(List(f3 -> 3, f1 -> 1, f2 -> 2))
    }
  }

  "Struct.structOrderInstance" should {

    "order as expected (first consider all names, then consider values)" in {

      import Struct.structOrderInstance
      import scalaz.Scalaz._

      val testCases =
        Table(
          "value" -> "index",
          List(
            List(),
            List(f1 -> 1),
            List(f1 -> 2),
            List(f1 -> 1, f2 -> 1),
            List(f1 -> 1, f2 -> 2),
            List(f1 -> 2, f2 -> 1),
            List(f1 -> 2, f2 -> 2),
            List(f1 -> 1, f2 -> 2, f3 -> 3),
            List(f2 -> 2),
            List(f2 -> 2, f3 -> 3),
            List(f3 -> 3)
          ).map(Struct.assertFromSeq).zipWithIndex: _*,
        )

      forEvery(testCases) { (x, i) =>
        forEvery(testCases) { (y, j) =>
          (x ?|? y) shouldBe (i ?|? j)
        }
      }
    }
  }

  "Struct.indexOf" should {
    "work for empty" in {
      struct().indexOf(f1) shouldBe -1
    }

    "work for one" in {
      struct(f1 -> 1).indexOf(f1) shouldBe 0
      struct(f1 -> 1).indexOf(f2) shouldBe -1
    }

    "work for many" in {
      val s = struct(f1 -> 1, f2 -> 2, f3 -> 3)
      s.indexOf(f1) shouldBe 0
      s.indexOf(f2) shouldBe 1
      s.indexOf(f3) shouldBe 2
      s.indexOf(f4) shouldBe -1
    }
  }

  "Struct.lookup" should {
    "return None if not found" in {
      struct(f1 -> 1).lookup(f2) shouldBe None
    }

    "should lookup successfully" in {
      struct(f1 -> 1).lookup(f1) shouldBe Some(1)
    }
  }

  def struct(ps: (Ref.Name, Int)*): Struct[Int] = Struct.assertFromSeq(ps.toList)
}
