// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.v1.{value => v}
import com.daml.script.export.TreeUtils.{valueParties}
import com.google.protobuf.empty.Empty
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class TreeUtilsSpec extends AnyFreeSpec with Matchers {
  private val p1 = v.Value().withParty("p1")
  private val p2 = v.Value().withParty("p2")

  "valueParties" - {
    "record" in {
      val idR = v.Identifier("pkg-id", "M", "R1")
      val r = v
        .Value()
        .withRecord(
          v.Record()
            .withRecordId(idR)
            .withFields(Seq(v.RecordField().withLabel("a").withValue(p1)))
        )
      valueParties(r.sum) should contain only ("p1")
    }
    "variant" in {
      val variantId = v.Identifier("pkg-id", "M", "V")
      val variant =
        v.Value()
          .withVariant(
            v.Variant()
              .withVariantId(variantId)
              .withConstructor("Constr")
              .withValue(p1)
          )
      valueParties(variant.sum) should contain only ("p1")
    }
    "contract id" in {
      val cid = v.Value().withContractId("my-contract-id")
      valueParties(cid.sum) shouldBe empty
    }
    "list" in {
      val l = v.Value().withList(v.List(Seq(p1, p2)))
      valueParties(l.sum) should contain only ("p1", "p2")
    }
    "int64" in {
      valueParties(v.Value().withInt64(42).sum) shouldBe empty
    }
    "numeric" in {
      valueParties(v.Value().withNumeric("1.3000").sum) shouldBe empty
    }
    "text" in {
      valueParties(v.Value.Sum.Text("abc\"def")) shouldBe empty
    }
    "timestamp" in {
      valueParties(
        v.Value().withTimestamp(0L).sum
      ) shouldBe empty
    }
    "party" in {
      valueParties(p1.sum) should contain only ("p1")
    }
    "bool" in {
      valueParties(v.Value.Sum.Bool(true)) shouldBe empty
      valueParties(v.Value.Sum.Bool(false)) shouldBe empty
    }
    "unit" in {
      valueParties(v.Value.Sum.Unit(Empty())) shouldBe empty
    }
    "date" in {
      valueParties(v.Value().withDate(0).sum) shouldBe empty
    }
    "optional" in {
      val none = v.Value().withOptional(v.Optional())
      valueParties(none.sum) shouldBe empty
      val some = v.Value().withOptional(v.Optional().withValue(p1))
      valueParties(some.sum) should contain only ("p1")
    }
    "textmap" in {
      val textmap = v
        .Value()
        .withMap(v.Map().withEntries(Seq(v.Map.Entry().withKey("key").withValue(p1))))
      valueParties(textmap.sum) should contain only ("p1")
    }
    "enum" in {
      val id = v.Identifier("pkg-id", "M", "E")
      valueParties(
        v.Value().withEnum(v.Enum().withEnumId(id).withConstructor("Constr")).sum
      ) shouldBe empty
    }
    "map" in {
      val genmap = v
        .Value()
        .withGenMap(
          v.GenMap().withEntries(Seq(v.GenMap.Entry().withKey(p1).withValue(p2)))
        )
      valueParties(genmap.sum) should contain only ("p1", "p2")
    }
  }
}
