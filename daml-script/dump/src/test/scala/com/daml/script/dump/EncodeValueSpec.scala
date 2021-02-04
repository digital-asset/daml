// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import com.daml.ledger.api.v1.{value => v}
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import com.google.protobuf.empty.Empty
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeValueSpec extends AnyFreeSpec with Matchers {
  private def assertMicrosFromInstant(i: Instant): Long =
    TimeUnit.SECONDS.toMicros(i.getEpochSecond) + TimeUnit.NANOSECONDS.toMicros(i.getNano.toLong)

  import Encode._
  "encodeValue" - {
    "record" in {
      val id1 = v.Identifier("pkg-id", "M", "R1")
      val id2 = v.Identifier("pkg-id", "M", "R2")
      val id3 = v.Identifier("pkg-id", "M", "R3")
      val r = v.Value.Sum.Record(
        v.Record(
          Some(id1),
          Seq(
            v.RecordField("a", Some(v.Value().withInt64(1))),
            v.RecordField(
              "b",
              Some(
                v.Value()
                  .withRecord(
                    v.Record(Some(id2), Seq(v.RecordField("c", Some(v.Value().withInt64(42)))))
                  )
              ),
            ),
            v.RecordField(
              "c",
              Some(v.Value().withRecord(v.Record(Some(id3)))),
            ),
          ),
        )
      )
      encodeValue(Map.empty, Map.empty, r).render(80) shouldBe
        """M.R1 with
        |  a = 1
        |  b = M.R2 with
        |    c = 42
        |  c = M.R3""".stripMargin.replace("\r\n", "\n")
    }
    "variant" in {
      val id = v.Identifier("pkg-id", "M", "V")
      val variant =
        v.Value().withVariant(v.Variant(Some(id), "Constr", Some(v.Value().withInt64(1))))
      encodeValue(Map.empty, Map.empty, variant.sum).render(80) shouldBe "(M.Constr 1)"
    }
    "contract id" in {
      val cid = v.Value().withContractId("my-contract-id")
      encodeValue(Map.empty, Map("my-contract-id" -> "mapped_cid"), cid.sum)
        .render(80) shouldBe "mapped_cid"
    }
    "list" in {
      val l = v.Value().withList(v.List(Seq(v.Value().withInt64(0), v.Value().withInt64(1))))
      encodeValue(Map.empty, Map.empty, l.sum).render(80) shouldBe "[0, 1]"
    }
    "int64" in {
      encodeValue(Map.empty, Map.empty, v.Value().withInt64(42).sum).render(80) shouldBe "42"
    }
    "numeric" in {
      encodeValue(Map.empty, Map.empty, v.Value().withNumeric("1.3000").sum)
        .render(80) shouldBe "1.3000"
    }
    "text" in {
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Text("abc\"def"))
        .render(80) shouldBe "\"abc\\\"def\""
    }
    "party" in {
      encodeValue(Map("unmapped" -> "mapped"), Map.empty, v.Value.Sum.Party("unmapped"))
        .render(80) shouldBe "mapped"
    }
    "bool" in {
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Bool(true)).render(80) shouldBe "True"
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Bool(false)).render(80) shouldBe "False"
    }
    "unit" in {
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Unit(Empty())).render(80) shouldBe "()"
    }
    "timestamp" in {
      val date = OffsetDateTime.of(1999, 11, 16, 13, 37, 42, 0, ZoneOffset.UTC)
      encodeValue(
        Map.empty,
        Map.empty,
        v.Value.Sum.Timestamp(assertMicrosFromInstant(date.toInstant())),
      ).render(80) shouldBe "(time (date 1999 Nov 16) 13 37 42)"
    }
    "date" in {
      val date = LocalDate.of(1999, 11, 16)
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Date(date.toEpochDay().toInt))
        .render(80) shouldBe "(date 1999 Nov 16)"
    }
    "optional" in {
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Optional(v.Optional()))
        .render(80) shouldBe "None"
      encodeValue(
        Map.empty,
        Map.empty,
        v.Value.Sum.Optional(v.Optional(Some(v.Value().withInt64(42)))),
      ).render(80) shouldBe "(Some 42)"
    }
    "textmap" in {
      encodeValue(
        Map.empty,
        Map.empty,
        v.Value.Sum.Map(v.Map(Seq(v.Map.Entry("key", Some(v.Value().withText("value")))))),
      ).render(80) shouldBe
        "(TextMap.fromList [(\"key\", \"value\")])"
    }
    "enum" in {
      val id = v.Identifier("pkg-id", "M", "E")
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Enum(v.Enum(Some(id), "Constr")))
        .render(80) shouldBe "M.Constr"
    }
    "map" in {
      val m = v.Value.Sum.GenMap(
        v.GenMap(
          Seq(v.GenMap.Entry(Some(v.Value().withInt64(42)), Some(v.Value().withText("value"))))
        )
      )
      encodeValue(Map.empty, Map.empty, m).render(80) shouldBe "(Map.fromList [(42, \"value\")])"
    }
  }
}
