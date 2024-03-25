// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricDoc
import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.noop.NoOpTimer
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.metrics.MetricDoc.getItems
import org.scalatest.wordspec.AnyWordSpec

class MetricDocTest extends AnyWordSpec with BaseTest {

  lazy val tm = NoOpTimer("test")
  class DocVar {
    @MetricDoc.Tag("varred summary", "varred desc", Debug)
    val varred = NoOpTimer("varred")
  }

  class DocItem {
    @MetricDoc.Tag(
      "top summary",
      "top desc",
      Debug,
      Map("label_key" -> "description", "label_key_2" -> "description_2"),
    )
    val top = NoOpTimer("top")
    val utop = NoOpTimer("utop")
    object nested {
      @MetricDoc.Tag("nested.n1 summary", "n1 desc", Debug)
      val n1 = NoOpTimer("nested.n1")
      val u1 = NoOpTimer("nested.u1")
      object nested2 {
        @MetricDoc.Tag("nested.n2 summary", "n2 desc", Debug)
        val n2 = NoOpTimer("nested.n2")
        val u2 = NoOpTimer("nested.u2")
      }
    }
    val other = new DocVar()

  }

  "embedded docs" should {
    "find nested items" in {

      val itm = new DocItem()
      val items = getItems(itm)

      val expected =
        Seq("varred", "nested.n1", "nested.n2", "top").map(nm => (nm, s"${nm} summary")).toSet
      items.map(x => (x.name, x.tag.summary)).toSet shouldBe expected

      items.flatMap(_.tag.labelsWithDescription) containsSlice Seq(
        "label_key" -> "description",
        "label_key_2" -> "description_2",
      )

    }
  }

}
