// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.{Matchers, WordSpec}

class InsertOrdMapTest extends WordSpec with Matchers {
  "toSeq" should {
    "preserve order" in {
      (InsertOrdMap.empty + (1 -> "a") + (2 -> "b")).toSeq shouldEqual Seq(1 -> "a", 2 -> "b")

      (InsertOrdMap.empty + (2 -> "b") + (1 -> "a")).toSeq shouldEqual Seq(2 -> "b", 1 -> "a")
    }
  }

  "fromSeq" should {
    "preserve order" in {
      InsertOrdMap.fromSeq(Seq(1 -> "a", 2 -> "b")).toSeq shouldEqual Seq(1 -> "a", 2 -> "b")
      InsertOrdMap.fromSeq(Seq(2 -> "b", 1 -> "a")).toSeq shouldEqual Seq(2 -> "b", 1 -> "a")
    }
  }
}
