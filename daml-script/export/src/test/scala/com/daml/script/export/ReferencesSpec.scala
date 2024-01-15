// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import com.daml.ledger.api.v1.{value => v}
import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.script.export.TreeUtils.{contractsReferences, treesReferences, valueRefs}
import com.google.protobuf.empty.Empty
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class ReferencesSpec extends AnyFreeSpec with Matchers {
  private def assertMicrosFromInstant(i: Instant): Long =
    TimeUnit.SECONDS.toMicros(i.getEpochSecond) + TimeUnit.NANOSECONDS.toMicros(i.getNano.toLong)

  "valueRefs" - {
    val nestedId = v.Identifier("pkg-id", "M", "Nested")
    val nestedVal = v.Value().withRecord(v.Record().withRecordId(nestedId))
    "record" in {
      val idR = v.Identifier("pkg-id", "M", "R1")
      val r = v
        .Value()
        .withRecord(
          v.Record()
            .withRecordId(idR)
            .withFields(Seq(v.RecordField().withLabel("a").withValue(nestedVal)))
        )
      valueRefs(r.sum) should contain only (idR, nestedId)
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
      val tuple1 = tuple(int1, int2)
      val tuple2 = tuple(int1, int2, nestedVal)
      val tuple3 = tuple(int1, tuple1, tuple2)
      valueRefs(tuple3.sum) should contain only (nestedId)
    }
    "variant" in {
      val variantId = v.Identifier("pkg-id", "M", "V")
      val variant =
        v.Value()
          .withVariant(
            v.Variant()
              .withVariantId(variantId)
              .withConstructor("Constr")
              .withValue(nestedVal)
          )
      valueRefs(variant.sum) should contain only (variantId, nestedId)
    }
    "either" in {
      val leftId: v.Identifier = v
        .Identifier()
        .withPackageId("40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7")
        .withModuleName("DA.Types")
        .withEntityName("Left")
      val rightId: v.Identifier = v
        .Identifier()
        .withPackageId("40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7")
        .withModuleName("DA.Types")
        .withEntityName("Right")
      val variant =
        v.Value()
          .withVariant(
            v.Variant()
              .withVariantId(leftId)
              .withConstructor("Left")
              .withValue(
                v.Value()
                  .withVariant(
                    v.Variant()
                      .withVariantId(rightId)
                      .withConstructor("Right")
                      .withValue(nestedVal)
                  )
              )
          )
      valueRefs(variant.sum) should contain only (leftId, rightId, nestedId)
    }
    "contract id" in {
      val cid = v.Value().withContractId("my-contract-id")
      valueRefs(cid.sum) shouldBe empty
    }
    "list" in {
      val l = v.Value().withList(v.List(Seq(nestedVal)))
      valueRefs(l.sum) should contain only (nestedId)
    }
    "int64" in {
      valueRefs(v.Value().withInt64(42).sum) shouldBe empty
    }
    "numeric" in {
      valueRefs(v.Value().withNumeric("1.3000").sum) shouldBe empty
    }
    "text" in {
      valueRefs(v.Value.Sum.Text("abc\"def")) shouldBe empty
    }
    "party" in {
      valueRefs(v.Value.Sum.Party("unmapped")) shouldBe empty
    }
    "bool" in {
      valueRefs(v.Value.Sum.Bool(true)) shouldBe empty
      valueRefs(v.Value.Sum.Bool(false)) shouldBe empty
    }
    "unit" in {
      valueRefs(v.Value.Sum.Unit(Empty())) shouldBe empty
    }
    "timestamp" in {
      val dateId: v.Identifier = v
        .Identifier()
        .withModuleName("DA.Date")
        .withEntityName("Date")
      val timeId: v.Identifier = v
        .Identifier()
        .withModuleName("DA.Time")
        .withEntityName("Time")
      val date = OffsetDateTime.of(1999, 11, 16, 13, 37, 42, 0, ZoneOffset.UTC)
      valueRefs(
        v.Value.Sum.Timestamp(assertMicrosFromInstant(date.toInstant()))
      ) should contain only (dateId, timeId)
    }
    "date" in {
      val dateId: v.Identifier = v
        .Identifier()
        .withModuleName("DA.Date")
        .withEntityName("Date")
      val date = LocalDate.of(1999, 11, 16)
      valueRefs(v.Value.Sum.Date(date.toEpochDay().toInt)) should contain only (dateId)
    }
    "optional" in {
      val none = v.Value().withOptional(v.Optional())
      valueRefs(none.sum) shouldBe empty
      val some = v.Value().withOptional(v.Optional().withValue(nestedVal))
      valueRefs(some.sum) should contain only (nestedId)
    }
    "textmap" in {
      val textmapId: v.Identifier = v
        .Identifier()
        .withModuleName("DA.TextMap")
        .withEntityName("TextMap")
      val textmap = v
        .Value()
        .withMap(v.Map().withEntries(Seq(v.Map.Entry().withKey("key").withValue(nestedVal))))
      valueRefs(textmap.sum) should contain only (textmapId, nestedId)
    }
    "enum" in {
      val id = v.Identifier("pkg-id", "M", "E")
      valueRefs(
        v.Value().withEnum(v.Enum().withEnumId(id).withConstructor("Constr")).sum
      ) should contain only (id)
    }
    "map" in {
      val genmapId: v.Identifier = v
        .Identifier()
        .withModuleName("DA.Map")
        .withEntityName("Map")
      val anotherId = v.Identifier("pkg-id", "M", "Another")
      val anotherVal = v.Value().withRecord(v.Record().withRecordId(anotherId))
      val genmap = v
        .Value()
        .withGenMap(
          v.GenMap().withEntries(Seq(v.GenMap.Entry().withKey(anotherVal).withValue(nestedVal)))
        )
      valueRefs(genmap.sum) should contain only (anotherId, nestedId, genmapId)
    }
  }
  "package references" - {
    "missing package id" - {
      val noId = v.Identifier().withModuleName("Module").withEntityName("EntityA")
      val noIdVal = v.Value().withRecord(v.Record().withRecordId(noId))
      val someId =
        v.Identifier().withPackageId("package").withModuleName("Module").withEntityName("EntityB")
      val someIdVal = v.Value().withRecord(v.Record().withRecordId(someId))
      "contractsReferences" in {
        val acs = TestData
          .ACS(
            Seq(
              TestData.Created(
                contractId = ContractId("cid1"),
                createArguments = Seq(
                  v.RecordField().withLabel("field1").withValue(noIdVal),
                  v.RecordField().withLabel("field2").withValue(someIdVal),
                ),
                submitters = Seq(Party("Alice")),
              )
            )
          )
          .toCreatedEvents
        contractsReferences(acs) should contain only ("package")
      }
      "treesReferences" in {
        val tree = TestData
          .Tree(
            Seq(
              TestData.Created(
                contractId = ContractId("cid1"),
                createArguments = Seq(
                  v.RecordField().withLabel("field1").withValue(noIdVal),
                  v.RecordField().withLabel("field2").withValue(someIdVal),
                ),
              )
            )
          )
          .toTransactionTree
        treesReferences(Seq(tree)) should contain only ("package")
      }
    }
  }
}
