// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

class ValueHasherSpec extends WordSpec with Matchers {
  private[this] def templateId(module: String, name: String) = Identifier(
    PackageId.assertFromString("package"),
    QualifiedName(
      ModuleName.assertFromString(module),
      DottedName.assertFromString(name)
    )
  )

  private[this] def complexValue = {
    val builder = ImmArray.newBuilder[(Option[Name], Value[AbsoluteContractId])]
    builder += None -> ValueInt64(0)
    builder += None -> ValueInt64(123456)
    builder += None -> ValueInt64(-1)
    builder += None -> ValueNumeric(decimal(0))
    builder += None -> ValueNumeric(decimal(BigDecimal("0.3333333333")))
    builder += None -> ValueTrue
    builder += None -> ValueFalse
    builder += None -> ValueDate(Time.Date.assertFromDaysSinceEpoch(0))
    builder += None -> ValueDate(Time.Date.assertFromDaysSinceEpoch(123456))
    builder += None -> ValueTimestamp(Time.Timestamp.assertFromLong(0))
    builder += None -> ValueTimestamp(Time.Timestamp.assertFromLong(123456))
    builder += None -> ValueText("")
    builder += None -> ValueText("abcd-äöü€")
    builder += None -> ValueParty(Party.assertFromString("Alice"))
    builder += None -> ValueUnit
    builder += None -> ValueNone
    builder += None -> ValueOptional(Some(ValueText("Some")))
    builder += None -> ValueList(FrontStack(ValueText("A"), ValueText("B"), ValueText("C")))
    builder += None -> ValueVariant(None, Name.assertFromString("Variant"), ValueInt64(0))
    builder += None -> ValueRecord(
      None,
      ImmArray(
        None -> ValueText("field1"),
        None -> ValueText("field2")
      ))
    builder += None -> ValueTextMap(
      SortedLookupList(
        Map(
          "keyA" -> ValueText("valueA"),
          "keyB" -> ValueText("valueB")
        )))
    val fields = builder.result()

    ValueRecord(None, fields)
  }

  "hashValue" should {

    import ValueHasher._

    "be stable" in {
      val tmpId = templateId("module", "name")
      hashValueString(complexValue) shouldBe "b83cca003b2f90ea4ce9986c5ad1d654d6e2b345f4665474c1567d154412942a"
      hashContractString(tmpId, complexValue) shouldBe "07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
      hashContractKeyString(tmpId, complexValue) shouldBe "59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector.fill(1000)(complexValue).par.map(hashValueString)
      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = ValueText("A")

      val hashes = Ref.LedgerString.Array(
        hashContractString(templateId("AA", "A"), value),
        hashContractString(templateId("A", "AA"), value),
        hashContractKeyString(templateId("AA", "A"), value),
        hashContractKeyString(templateId("A", "AA"), value),
      )

      for {
        i <- hashes.indices
        j <- hashes.indices
        if (i < j)
      } hashes(i) shouldNot be(hashes(j))

    }

    // Note: value version is given by the template ID, this check is not necessary
    /*
    "not produce collision in value version" in {
      // Same value but different value version should produce a different hash
      val value1 = ValueText("A")
      val value2 = ValueText("A")

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashValueString(value1, tid)
      val hash2 = KeyHasher.hashValueString(value2, tid)

      hash1.equals(hash2) shouldBe false
    }
     */

    "not produce collision in list of text" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      val value1 =
        ValueList(FrontStack(ValueText("AA"), ValueText("A")))
      val value2 =
        ValueList(FrontStack(ValueText("A"), ValueText("AA")))

      hashValueString(value1) shouldNot be(hashValue(value2))

    }

    "not produce collision in list of decimals" in {
      // Testing whether decimals are delimited: [10, 10] vs [101, 0]
      val value1 = ValueList(FrontStack(ValueNumeric(decimal(10)), ValueNumeric(decimal(10))))
      val value2 = ValueList(FrontStack(ValueNumeric(decimal(101)), ValueNumeric(decimal(0))))

      hashValueString(value1) shouldNot be(hashValue(value2))
    }

    "not produce collision in list of lists" in {
      // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
      val value1 = ValueList(
        FrontStack(
          ValueList(FrontStack(ValueUnit)),
          ValueList(FrontStack(ValueUnit, ValueUnit))
        ))
      val value2 = ValueList(
        FrontStack(
          ValueList(FrontStack(ValueUnit, ValueUnit)),
          ValueList(FrontStack(ValueUnit))
        ))

      hashValueString(value1) shouldNot be(hashValue(value2))

    }

    "not produce collision in Variant constructor" in {
      val value1 =
        ValueVariant(None, Name.assertFromString("A"), ValueUnit)
      val value2 =
        ValueVariant(None, Name.assertFromString("B"), ValueUnit)

      hashValueString(value1) shouldNot be(hashValue(value2))

    }

    "not produce collision in Variant value" in {
      val value1 = ValueVariant(None, Name.assertFromString("A"), ValueInt64(0L))
      val value2 = ValueVariant(None, Name.assertFromString("A"), ValueInt64(1L))

      hashValueString(value1) shouldNot be(hashValue(value2))

    }

    "not produce collision in Map keys" in {
      val value1 = ValueTextMap(
        SortedLookupList(
          Map(
            "A" -> ValueInt64(0),
            "B" -> ValueInt64(0)
          )))
      val value2 = ValueTextMap(
        SortedLookupList(
          Map(
            "A" -> ValueInt64(0),
            "C" -> ValueInt64(0)
          )))

      hashValueString(value1) shouldNot be(hashValue(value2))

    }

    "not produce collision in Map values" in {
      val value1 = ValueTextMap(
        SortedLookupList(
          Map(
            "A" -> ValueInt64(0),
            "B" -> ValueInt64(0)
          )))
      val value2 = ValueTextMap(
        SortedLookupList(
          Map(
            "A" -> ValueInt64(0),
            "B" -> ValueInt64(1)
          )))

      hashValueString(value1) shouldNot be(hashValue(value2))

    }

    "not produce collision in Bool" in {
      val value1 = ValueTrue
      val value2 = ValueFalse

      hashValueString(value1) shouldNot be(hashValue(value2))

    }

    "not produce collision in Int64" in {
      val value1 = ValueInt64(0L)
      val value2 = ValueInt64(1L)

      hashValueString(value1) shouldNot be(hashValue(value2))
    }

    "not produce collision in Decimal" in {
      val value1 = ValueNumeric(decimal(0))
      val value2 = ValueNumeric(decimal(1))

      hashValueString(value1) shouldNot be(hashValue(value2))
    }

    "not produce collision in Date" in {
      val value1 = ValueDate(Time.Date.assertFromDaysSinceEpoch(0))
      val value2 = ValueDate(Time.Date.assertFromDaysSinceEpoch(1))

      hashValueString(value1) shouldNot be(hashValue(value2))
    }

    "not produce collision in Timestamp" in {
      val value1 = ValueTimestamp(Time.Timestamp.assertFromLong(0))
      val value2 = ValueTimestamp(Time.Timestamp.assertFromLong(1))

      hashValueString(value1) shouldNot be(hashValue(value2))
    }

    "not produce collision in Optional" in {
      val value1 = ValueNone
      val value2 = ValueOptional(Some(ValueUnit))

      hashValueString(value1) shouldNot be(hashValue(value2))
    }

    "not produce collision in Record" in {
      val value1 = ValueRecord(
        None,
        ImmArray(
          None -> ValueText("A"),
          None -> ValueText("B")
        ))
      val value2 = ValueRecord(
        None,
        ImmArray(
          None -> ValueText("A"),
          None -> ValueText("C")
        ))

      hashValueString(value1) shouldNot be(hashValue(value2))
    }
  }

  private implicit def decimal(x: BigDecimal): Numeric =
    Numeric.assertFromBigDecimal(Decimal.scale, x)

}
