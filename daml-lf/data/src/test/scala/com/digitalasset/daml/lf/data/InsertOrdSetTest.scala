// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import org.scalatest.{Matchers, WordSpec}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
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

  "use CanBuildFrom of InsertOrdSet" should {
    "preserve type" in {
      val ios: InsertOrdSet[String] = InsertOrdSet.fromSeq(Seq("a", "b"))
      ios.map(x => x) shouldBe ios
      ios.map(x => x + "x") shouldBe a[InsertOrdSet[_]]
    }
  }
}
