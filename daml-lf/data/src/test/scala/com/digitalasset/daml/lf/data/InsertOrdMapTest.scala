// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InsertOrdMapTest extends AnyWordSpec with Matchers {
  "toSeq" should {
    "preserve order" in {
      (InsertOrdMap.empty + (1 -> "a") + (2 -> "b")).toSeq shouldEqual Seq(1 -> "a", 2 -> "b")
      (InsertOrdMap.empty + (2 -> "b") + (1 -> "a")).toSeq shouldEqual Seq(2 -> "b", 1 -> "a")
    }
  }

  "updated" should {

    "insert at the end if key not present" in {
      InsertOrdMap(1 -> "a", 2 -> "b").updated(3, "c").toSeq shouldEqual Seq(
        1 -> "a",
        2 -> "b",
        3 -> "c")
    }

    "insert without changing order if key is present" in {
      InsertOrdMap(1 -> "a", 2 -> "b").updated(1, "c").toSeq shouldEqual Seq(1 -> "c", 2 -> "b")
    }

  }

  "apply" should {
    "preserve order" in {
      InsertOrdMap(1 -> "a", 2 -> "b").toSeq shouldEqual Seq(1 -> "a", 2 -> "b")
      InsertOrdMap(2 -> "b", 1 -> "a").toSeq shouldEqual Seq(2 -> "b", 1 -> "a")
    }

    "drop duplicate" in {
      InsertOrdMap(1 -> "a", 2 -> "b", 1 -> "c").toSeq shouldEqual Seq(1 -> "c", 2 -> "b")
    }

  }
}
