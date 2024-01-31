// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.v1.{value => v}
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import com.digitalasset.canton.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.google.protobuf.empty.Empty
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class EncodeValueSpec extends AnyFreeSpec with Matchers {
  private def assertMicrosFromInstant(i: Instant): Long =
    TimeUnit.SECONDS.toMicros(i.getEpochSecond) + TimeUnit.NANOSECONDS.toMicros(i.getNano.toLong)

  import Encode._
  "encodeValue" - {
    val record = v.Record(
      Some(v.Identifier("pkg-id", "M", "R1")),
      Seq(
        v.RecordField("a", Some(v.Value().withInt64(1))),
        v.RecordField(
          "b",
          Some(
            v.Value()
              .withRecord(
                v.Record(
                  Some(v.Identifier("pkg-id", "M", "R2")),
                  Seq(v.RecordField("c", Some(v.Value().withInt64(42)))),
                )
              )
          ),
        ),
        v.RecordField(
          "c",
          Some(v.Value().withRecord(v.Record(Some(v.Identifier("pkg-id", "M", "R3"))))),
        ),
      ),
    )
    "record" in {
      encodeValue(
        Map.empty,
        Map.empty,
        v.Value.Sum.Record(record),
      ).render(
        80
      ) shouldBe "(M.R1 {a = 1, b = (M.R2 {c = 42}), c = M.R3})"
      encodeValue(
        Map.empty,
        Map.empty,
        v.Value.Sum.Record(record),
      ).render(30) shouldBe
        """(M.R1 {a = 1,
          |    b = (M.R2 {c = 42}),
          |    c = M.R3})""".stripMargin.replace("\r\n", "\n")

    }
    "tuple" in {
      def tupleId(n: Int): v.Identifier = v
        .Identifier()
        .withPackageId("40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7")
        .withModuleName("DA.Types")
        .withEntityName(s"Tuple$n")
      def tuple(vals: v.Value*): v.Value = {
        v.Value()
          .withRecord(
            v.Record()
              .withRecordId(tupleId(vals.size))
              .withFields(vals.zipWithIndex.map { case (value, ix) =>
                v.RecordField().withLabel(s"_$ix").withValue(value)
              })
          )
      }
      val int1 = v.Value().withInt64(1)
      val int2 = v.Value().withInt64(2)
      val int3 = v.Value().withInt64(3)
      val tuple1 = tuple(int1, int2)
      val tuple2 = tuple(int1, int2, int3)
      val tuple3 = tuple(int1, tuple1, tuple2)
      encodeValue(Map.empty, Map.empty, tuple3.sum).render(80) shouldBe
        """(1, (1, 2), (1, 2, 3))""".stripMargin.replace("\r\n", "\n")
    }
    "variant" in {
      val id = v.Identifier("pkg-id", "M", "V")
      val variant =
        v.Value().withVariant(v.Variant(Some(id), "Constr", Some(v.Value().withInt64(1))))
      encodeValue(Map.empty, Map.empty, variant.sum).render(80) shouldBe "(M.Constr 1)"

      // Tests a variant constructor declared with record syntax
      val fields = record.withRecordId(id.withEntityName("V.ConstrFields"))
      val variantFields =
        v.Value()
          .withVariant(v.Variant(Some(id), "ConstrFields", Some(v.Value().withRecord(fields))))
      encodeValue(Map.empty, Map.empty, variantFields.sum).render(
        80
      ) shouldBe "(M.ConstrFields {a = 1, b = (M.R2 {c = 42}), c = M.R3})"

      // Tests a variant constructor declared with an argument of a record type.
      val variantRec =
        v.Value().withVariant(v.Variant(Some(id), "ConstrRec", Some(v.Value().withRecord(record))))
      encodeValue(Map.empty, Map.empty, variantRec.sum).render(
        80
      ) shouldBe "(M.ConstrRec (M.R1 {a = 1, b = (M.R2 {c = 42}), c = M.R3}))"
    }
    "contract id" in {
      val cid = v.Value().withContractId("my-contract-id")
      encodeValue(Map.empty, Map(ContractId("my-contract-id") -> "mapped_cid"), cid.sum)
        .render(80) shouldBe "mapped_cid"
    }
    "simple list" in {
      val l = List(0L, 1L).map(v.Value().withInt64(_))
      val value = v.Value().withList(v.List(l))
      encodeValue(Map.empty, Map.empty, value.sum).render(80) shouldBe "[0, 1]"
    }
    "list of record" in {
      val l = List(record, record).map(v.Value().withRecord(_))
      val value = v.Value().withList(v.List(l))
      encodeValue(Map.empty, Map.empty, value.sum).render(
        100
      ) shouldBe "[(M.R1 {a = 1, b = (M.R2 {c = 42}), c = M.R3}), (M.R1 {a = 1, b = (M.R2 {c = 42}), c = M.R3})]"
      encodeValue(Map.empty, Map.empty, value.sum).render(
        30
      ) shouldBe """[(M.R1 {a = 1,
                   |      b = (M.R2 {c = 42}),
                   |      c = M.R3}),
                   |  (M.R1 {a = 1,
                   |      b = (M.R2 {c = 42}),
                   |      c = M.R3})]""".stripMargin.replace("\r\n", "\n")
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
      encodeValue(Map(Party("unmapped") -> "mapped"), Map.empty, v.Value.Sum.Party("unmapped"))
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
      ).render(80) shouldBe "(DA.Time.time (DA.Date.date 1999 DA.Date.Nov 16) 13 37 42)"
    }
    "date" in {
      val date = LocalDate.of(1999, 11, 16)
      encodeValue(Map.empty, Map.empty, v.Value.Sum.Date(date.toEpochDay().toInt))
        .render(80) shouldBe "(DA.Date.date 1999 DA.Date.Nov 16)"
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
        "(DA.TextMap.fromList [(\"key\", \"value\")])"
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
      encodeValue(Map.empty, Map.empty, m).render(80) shouldBe "(DA.Map.fromList [(42, \"value\")])"
    }
  }
}
