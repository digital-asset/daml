// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.{Matchers, WordSpec}

class InsertOrdSetTest extends WordSpec with Matchers {
  "toSeq" should {
    "preserve order" in {
      (InsertOrdSet.empty + "a" + "b").toSeq shouldEqual Seq("a", "b")
      (InsertOrdSet.empty + "b" + "a").toSeq shouldEqual Seq("b", "a")
    }
  }

  "fromSeq" should {
    "preserve order" in {
      InsertOrdSet.fromSeq(Seq("a", "b")).toSeq shouldEqual Seq("a", "b")
      InsertOrdSet.fromSeq(Seq("b", "a")).toSeq shouldEqual Seq("b", "a")
    }
  }

}
