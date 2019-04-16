// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.{Value, ValueVersion}
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.KeyHasher

class KeyHasherSpec extends WordSpec with Matchers {
  private[this] def templateId(module: String, name: String) = Identifier(
    PackageId.assertFromString("package"),
    QualifiedName(
      ModuleName(ImmArray(module)),
      DottedName(ImmArray(name))
    )
  )

  private[this] def complexValue = {
    val builder = ImmArray.newBuilder[(Option[String], Value[AbsoluteContractId])]
    builder += None -> ValueInt64(0)
    builder += None -> ValueInt64(123456)
    builder += None -> ValueInt64(-1)
    builder += None -> ValueDecimal(BigDecimal(0))
    builder += None -> ValueDecimal(BigDecimal(1) / BigDecimal(3))
    builder += None -> ValueBool(true)
    builder += None -> ValueBool(false)
    builder += None -> ValueDate(Time.Date.assertFromDaysSinceEpoch(0))
    builder += None -> ValueDate(Time.Date.assertFromDaysSinceEpoch(123456))
    builder += None -> ValueTimestamp(Time.Timestamp.assertFromLong(0))
    builder += None -> ValueTimestamp(Time.Timestamp.assertFromLong(123456))
    builder += None -> ValueText("")
    builder += None -> ValueText("abcd-äöü€")
    builder += None -> ValueParty(SimpleString.assertFromString("Alice"))
    builder += None -> ValueUnit
    builder += None -> ValueOptional(None)
    builder += None -> ValueOptional(Some(ValueText("Some")))
    builder += None -> ValueList(FrontStack(ValueText("A"), ValueText("B"), ValueText("C")))
    builder += None -> ValueVariant(None, "Variant", ValueInt64(0))
    builder += None -> ValueRecord(
      None,
      ImmArray(
        None -> ValueText("field1"),
        None -> ValueText("field2")
      ))
    builder += None -> ValueMap(
      SortedLookupList(
        Map(
          "keyA" -> ValueText("valueA"),
          "keyB" -> ValueText("valueB")
        )))
    val fields = builder.result()

    ValueRecord(None, fields)
  }

  "KeyHasher" should {

    "be stable" in {
      // Hashing function must not change
      val value = VersionedValue(ValueVersion("4"), complexValue)
      val hash = "4f44a1674ef37e1019fcfd1c74047aef97a4cf34991bb9ee3368166254b7c77c"

      KeyHasher.hashKeyString(GlobalKey(templateId("module", "name"), value)) shouldBe hash
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector
        .range(0, 1000)
        .map(_ =>
          GlobalKey(templateId("module", "name"), VersionedValue(ValueVersion("4"), complexValue)))
        .par
        .map(key => KeyHasher.hashKeyString(key))

      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = VersionedValue(ValueVersion("4"), ValueText("A"))

      val hash1 = KeyHasher.hashKeyString(GlobalKey(templateId("AA", "A"), value))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(templateId("A", "AA"), value))

      hash1.equals(hash2) shouldBe false
    }

    // Note: value version is given by the template ID, this check is not necessary
    /*
    "not produce collision in value version" in {
      // Same value but different value version should produce a different hash
      val value1 = VersionedValue(ValueVersion("3"), ValueText("A"))
      val value2 = VersionedValue(ValueVersion("4"), ValueText("A"))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }
     */

    "not produce collision in list of text" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      val value1 =
        VersionedValue(ValueVersion("4"), ValueList(FrontStack(ValueText("AA"), ValueText("A"))))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueList(FrontStack(ValueText("A"), ValueText("AA"))))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of decimals" in {
      // Testing whether decimals are delimited: [10, 10] vs [101, 0]
      val value1 =
        VersionedValue(ValueVersion("4"), ValueList(FrontStack(ValueDecimal(10), ValueDecimal(10))))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueList(FrontStack(ValueDecimal(101), ValueDecimal(0))))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of lists" in {
      // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
      val value1 = VersionedValue(
        ValueVersion("4"),
        ValueList(
          FrontStack(
            ValueList(FrontStack(ValueUnit)),
            ValueList(FrontStack(ValueUnit, ValueUnit))
          )))
      val value2 = VersionedValue(
        ValueVersion("4"),
        ValueList(
          FrontStack(
            ValueList(FrontStack(ValueUnit, ValueUnit)),
            ValueList(FrontStack(ValueUnit))
          )))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Variant constructor" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueVariant(None, "A", ValueUnit))
      val value2 = VersionedValue(ValueVersion("4"), ValueVariant(None, "B", ValueUnit))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Variant value" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueVariant(None, "A", ValueInt64(0L)))
      val value2 = VersionedValue(ValueVersion("4"), ValueVariant(None, "A", ValueInt64(1L)))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map keys" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        ValueMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(0)
            ))))
      val value2 = VersionedValue(
        ValueVersion("4"),
        ValueMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "C" -> ValueInt64(0)
            ))))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map values" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        ValueMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(0)
            ))))
      val value2 = VersionedValue(
        ValueVersion("4"),
        ValueMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(1)
            ))))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Bool" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueBool(true))
      val value2 = VersionedValue(ValueVersion("4"), ValueBool(false))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Int64" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueInt64(0L))
      val value2 = VersionedValue(ValueVersion("4"), ValueInt64(1L))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Decimal" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueDecimal(0))
      val value2 = VersionedValue(ValueVersion("4"), ValueDecimal(1))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Date" in {
      val value1 =
        VersionedValue(ValueVersion("4"), ValueDate(Time.Date.assertFromDaysSinceEpoch(0)))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueDate(Time.Date.assertFromDaysSinceEpoch(1)))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Timestamp" in {
      val value1 =
        VersionedValue(ValueVersion("4"), ValueTimestamp(Time.Timestamp.assertFromLong(0)))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueTimestamp(Time.Timestamp.assertFromLong(1)))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Optional" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueOptional(None))
      val value2 = VersionedValue(ValueVersion("4"), ValueOptional(Some(ValueUnit)))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Record" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        ValueRecord(
          None,
          ImmArray(
            None -> ValueText("A"),
            None -> ValueText("B")
          )))
      val value2 = VersionedValue(
        ValueVersion("4"),
        ValueRecord(
          None,
          ImmArray(
            None -> ValueText("A"),
            None -> ValueText("C")
          )))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }
  }
}
