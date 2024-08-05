// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Numeric, Ref, SortedLookupList, Time}
import com.digitalasset.daml.lf.value.test.TypedValueGenerators.{ValueAddend => VA}
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import shapeless.record.{Record => HRecord}
import shapeless.syntax.singleton._
import shapeless.{HNil, Coproduct => HSum}

import scala.language.implicitConversions

class HashSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  private val packageId0 = Ref.PackageId.assertFromString("package")
  private val packageName0 = Ref.PackageName.assertFromString("package-name-0")

  def assertHashContractKey(templateId: Ref.Identifier, key: Value): Hash = {
    Hash.assertHashContractKey(templateId, packageName0, key)
  }

  private val complexRecordT =
    VA.record(
      defRef(name = "ComplexRecord"),
      HRecord(
        fInt0 = VA.int64,
        fInt1 = VA.int64,
        fInt2 = VA.int64,
        fNumeric0 = VA.numeric(Numeric.Scale.assertFromInt(10)),
        fNumeric1 = VA.numeric(Numeric.Scale.assertFromInt(10)),
        fBool0 = VA.bool,
        fBool1 = VA.bool,
        fDate0 = VA.date,
        fDate1 = VA.date,
        fTime0 = VA.timestamp,
        fTime1 = VA.timestamp,
        fText0 = VA.text,
        fTest1 = VA.text,
        fPArty = VA.party,
        fUnit = VA.unit,
        fOpt0 = VA.optional(VA.text),
        fOpt1 = VA.optional(VA.text),
        fList = VA.list(VA.text),
        fVariant = VA
          .variant(
            defRef(name = "Variant"),
            HRecord(Variant = VA.int64),
          )
          ._2,
        fRecord = VA
          .record(
            defRef(name = "Record"),
            HRecord(field1 = VA.text, field2 = VA.text),
          )
          ._2,
        fTextMap = VA.map(VA.text),
      ),
    )._2

  private val complexRecordV: complexRecordT.Inj =
    HRecord(
      fInt0 = 0L,
      fInt1 = 123456L,
      fInt2 = -1L,
      fNumeric0 = Numeric.assertFromString("0.0000000000"),
      fNumeric1 = Numeric.assertFromString("0.3333333333"),
      fBool0 = true,
      fBool1 = false,
      fDate0 = Time.Date.assertFromDaysSinceEpoch(0),
      fDate1 = Time.Date.assertFromDaysSinceEpoch(123456),
      fTime0 = Time.Timestamp.assertFromLong(0),
      fTime1 = Time.Timestamp.assertFromLong(123456),
      fText0 = "",
      fTest1 = "abcd-äöü€",
      fPArty = Ref.Party.assertFromString("Alice"),
      fUnit = (),
      fOpt0 = None,
      fOpt1 = Some("Some"),
      fList = Vector("A", "B", "C"),
      fVariant = HSum(Symbol("Variant") ->> 0L),
      fRecord = HRecord(field1 = "field1", field2 = "field2"),
      fTextMap = SortedLookupList(Map("keyA" -> "valueA", "keyB" -> "valueB")),
    )

  "KeyHasher" should {

    "be stable" in {
      val hash = "c65eee0b19b2ec0d8ea727e3cc399c266e63fa174fe728857265fceca563d881"
      val value = complexRecordT.inj(complexRecordV)
      val name = defRef("module", "name")
      assertHashContractKey(name, value).toHexString shouldBe hash
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector
        .fill(1000)(defRef("module", "name") -> complexRecordT.inj(complexRecordV))
        .map(Function.tupled(assertHashContractKey))

      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = VA.text.inj("A")

      val hash1 = assertHashContractKey(defRef("AA", "A"), value)
      val hash2 = assertHashContractKey(defRef("A", "AA"), value)

      hash1 should !==(hash2)
    }

    "not produce collision in list of texts (1)" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      def list(elements: String*) = VA.list(VA.text).inj(elements.toVector)
      val value1 = list("AA", "A")
      val value2 = list("A", "AA")

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in list of decimals" in {
      // Testing whether numeric are delimited: [10, 10] vs [101, 0]
      def list(elements: String*) =
        VA.list(VA.numeric(Numeric.Scale.assertFromInt(10)))
          .inj(elements.map(Numeric.assertFromString).toVector)
      val value1 = list("10.0000000000", "10.0000000000")
      val value2 = list("101.0000000000", "0.0000000000")

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in list of lists" in {
      // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
      def list(elements: Vector[Unit]*) = VA.list(VA.list(VA.unit)).inj(elements.toVector)
      val value1 = list(Vector(()), Vector((), ()))
      val value2 = list(Vector((), ()), Vector(()))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in list of records (1)" in {
      val recordT =
        VA.record(
          defRef(name = "Tuple2"),
          HRecord(_1 = VA.text, _2 = VA.text),
        )
      val value1 = ValueList(
        FrontStack(
          recordT._2.inj(HRecord(_1 = "A", _2 = "B")),
          recordT._2.inj(HRecord(_1 = "", _2 = "")),
        )
      )

      val value2 = ValueList(
        FrontStack(
          recordT._2.inj(HRecord(_1 = "A", _2 = "")),
          recordT._2.inj(HRecord(_1 = "", _2 = "B")),
        )
      )

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in list of records (2)" in {
      val emptyRecord = ValueRecord(None, ImmArray.empty)
      val nonEmptyRecord = ValueRecord(None, ImmArray(None -> ValueInt64(1L)))
      val value1 = ValueList(FrontStack(emptyRecord, nonEmptyRecord))
      val value2 = ValueList(FrontStack(nonEmptyRecord, emptyRecord))
      val tid = defRef("module", "name")
      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)
      hash1 should !==(hash2)
    }

    "not produce collision in Variant constructor" in {
      val variantT =
        VA.variant(
          defRef(name = "Variant"),
          HRecord(A = VA.unit, B = VA.unit),
        )._2
      val value1 = variantT.inj(HSum[variantT.Inj](Symbol("A") ->> (())))
      val value2 = variantT.inj(HSum[variantT.Inj](Symbol("B") ->> (())))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Variant value" in {
      val variantT = VA.variant(defRef(name = "Variant"), HRecord(A = VA.int64))._2
      val value1 = variantT.inj(HSum(Symbol("A") ->> 0L))
      val value2 = variantT.inj(HSum(Symbol("A") ->> 1L))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in TextMap keys" in {
      def textMap(elements: (String, Long)*) =
        VA.map(VA.int64).inj(SortedLookupList(elements.toMap))
      val value1 = textMap("A" -> 0, "B" -> 0)
      val value2 = textMap("A" -> 0, "C" -> 0)

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in TextMap values" in {
      def textMap(elements: (String, Long)*) =
        VA.map(VA.int64).inj(SortedLookupList(elements.toMap[String, Long]))
      val value1 = textMap("A" -> 0, "B" -> 0)
      val value2 = textMap("A" -> 0, "B" -> 1)

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in TextMap of records" in {
      def ref4(x4: Long) = {
        ValueRecord(
          None,
          ImmArray(
            None -> ValueUnit,
            None -> ValueUnit,
            None -> ValueUnit,
            None -> ValueUnit,
            None -> ValueUnit,
            None -> ValueUnit,
            None -> ValueUnit,
            None -> ValueInt64(x4),
          ),
        )
      }

      def genmap(elements: (String, Value)*) =
        ValueGenMap(ImmArray.from(elements.map { case (k, v) => ValueText(k) -> v }))
      val value1 = genmap(
        "0" -> // '00000001' + '30'
          ref4(0x3030303030303030L), // '00000008' + '3030303030303030'
        "00000000" -> // '00000008' + '3030303030303030'
          ref4(0), // <empty>
      )
      val value2 = genmap(
        "0" -> // '00000001' + '30'
          ref4(0), // <empty>
        "00000000" -> // '00000004' + '30303030'
          ref4(0x3030303030303030L), // '00000004' + '30303030'
      )

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)
      hash1 should !==(hash2)
    }

    "not produce collision in GenMap keys" in {
      def genMap(elements: (String, Long)*) =
        VA.genMap(VA.text, VA.int64).inj(elements.toMap[String, Long])
      val value1 = genMap("A" -> 0, "B" -> 0)
      val value2 = genMap("A" -> 0, "C" -> 0)

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in GenMap values" in {
      def genMap(elements: (String, Long)*) =
        VA.genMap(VA.text, VA.int64).inj(elements.toMap[String, Long])
      val value1 = genMap("A" -> 0, "B" -> 0)
      val value2 = genMap("A" -> 0, "B" -> 1)

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Bool" in {
      val value1 = ValueTrue
      val value2 = ValueFalse

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Int64" in {
      val value1 = ValueInt64(0L)
      val value2 = ValueInt64(1L)

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Numeric" in {
      val value1 = ValueNumeric(Numeric.assertFromString("0."))
      val value2 = ValueNumeric(Numeric.assertFromString("1."))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Date" in {
      val value1 = ValueDate(Time.Date.assertFromDaysSinceEpoch(0))
      val value2 = ValueDate(Time.Date.assertFromDaysSinceEpoch(1))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Timestamp" in {
      val value1 = ValueTimestamp(Time.Timestamp.assertFromLong(0))
      val value2 = ValueTimestamp(Time.Timestamp.assertFromLong(1))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Optional" in {
      val value1 = ValueNone
      val value2 = ValueOptional(Some(ValueUnit))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }

    "not produce collision in Record" in {
      val recordT =
        VA.record(
          defRef(name = "Tuple2"),
          HRecord(_1 = VA.text, _2 = VA.text),
        )._2
      val value1 = recordT.inj(HRecord(_1 = "A", _2 = "B"))
      val value2 = recordT.inj(HRecord(_1 = "A", _2 = "C"))

      val tid = defRef("module", "name")

      val hash1 = assertHashContractKey(tid, value1)
      val hash2 = assertHashContractKey(tid, value2)

      hash1 should !==(hash2)
    }
  }

  val stabilityTestCases: List[Value] = {
    val pkgId = Ref.PackageId.assertFromString("pkgId")

    implicit def toTypeConName(s: String): Ref.TypeConName =
      Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

    implicit def toName(s: String): Ref.Name =
      Ref.Name.assertFromString(s)

    val units = List(ValueUnit)
    val bools = List(true, false).map(VA.bool.inj(_))
    val ints = List(-1L, 0L, 1L).map(VA.int64.inj(_))
    val numeric10s = List("-10000.0000000000", "0.0000000000", "10000.0000000000")
      .map(Numeric.assertFromString)
      .map(VA.numeric(Numeric.Scale.assertFromInt(10)).inj(_))
    val numeric0s = List("-10000.", "0.", "10000.")
      .map(Numeric.assertFromString)
      .map(VA.numeric(Numeric.Scale.MinValue).inj(_))
    val texts = List("", "someText", "a¶‱😂").map(VA.text.inj(_))
    val dates =
      List(
        Time.Date.assertFromDaysSinceEpoch(0),
        Time.Date.assertFromString("1969-07-21"),
        Time.Date.assertFromString("2019-12-16"),
      ).map(VA.date.inj(_))
    val timestamps =
      List(
        Time.Timestamp.assertFromLong(0),
        Time.Timestamp.assertFromString("1969-07-21T02:56:15.000000Z"),
        Time.Timestamp.assertFromString("2019-12-16T11:17:54.940779363Z"),
      ).map(VA.timestamp.inj(_))
    val parties =
      List(
        Ref.Party.assertFromString("alice"),
        Ref.Party.assertFromString("bob"),
      ).map(VA.party.inj(_))
    val contractIds =
      List(
        "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5",
        "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b",
      ).map { str =>
        import org.scalacheck.{Arbitrary, Gen}
        VA.contractId(Arbitrary(Gen.fail)).inj(ContractId.V1 assertFromString str)
      }
    val enumT1 = VA.enumeration("Color", List("Red", "Green"))._2
    val enumT2 = VA.enumeration("ColorBis", List("Red", "Green"))._2

    val enums = List(
      enumT1.inj(enumT1.get("Red").get),
      enumT1.inj(enumT1.get("Green").get),
      enumT2.inj(enumT2.get("Green").get),
    )

    val record0T1 = VA.record("Unit", HNil)._2
    val record0T2 = VA.record("UnitBis", HNil)._2

    val records0 =
      List(
        record0T1.inj(HRecord()),
        record0T2.inj(HRecord()),
      )

    val record2T1 =
      VA.record("Tuple", HRecord(_1 = VA.bool, _2 = VA.bool))._2
    val record2T2 =
      VA.record("TupleBis", HRecord(_1 = VA.bool, _2 = VA.bool))._2

    val records2 =
      List(
        record2T1.inj(HRecord(_1 = false, _2 = false)),
        record2T1.inj(HRecord(_1 = true, _2 = false)),
        record2T1.inj(HRecord(_1 = false, _2 = true)),
        record2T2.inj(HRecord(_1 = false, _2 = false)),
      )

    val variantT1 =
      VA.variant("Either", HRecord(Left = VA.bool, Right = VA.bool))._2
    val variantT2 = VA
      .variant("EitherBis", HRecord(Left = VA.bool, Right = VA.bool))
      ._2

    val variants = List(
      variantT1.inj(HSum[variantT1.Inj](Symbol("Left") ->> false)),
      variantT1.inj(HSum[variantT1.Inj](Symbol("Left") ->> true)),
      variantT1.inj(HSum[variantT1.Inj](Symbol("Right") ->> false)),
      variantT2.inj(HSum[variantT1.Inj](Symbol("Left") ->> false)),
    )

    def list(elements: Boolean*) = VA.list(VA.bool).inj(elements.toVector)

    val lists = List(
      list(),
      list(false),
      list(true),
      list(false, false),
      list(false, true),
      list(true, false),
    )

    def textMap(entries: (String, Boolean)*) =
      VA.map(VA.bool).inj(SortedLookupList(entries.toMap))

    val textMaps = List[Value](
      textMap(),
      textMap("a" -> false),
      textMap("a" -> true),
      textMap("b" -> false),
      textMap("a" -> false, "b" -> false),
      textMap("a" -> true, "b" -> false),
      textMap("a" -> false, "b" -> true),
      textMap("a" -> false, "c" -> false),
    )

    def genMap(entries: (String, Boolean)*) =
      VA.genMap(VA.text, VA.bool).inj(entries.toMap[String, Boolean])

    val genMaps = List[Value](
      genMap(),
      genMap("a" -> false),
      genMap("a" -> true),
      genMap("b" -> false),
      genMap("a" -> false, "b" -> false),
      genMap("a" -> true, "b" -> false),
      genMap("a" -> false, "b" -> true),
      genMap("a" -> false, "c" -> false),
    )

    val optionals =
      List(None, Some(false), Some(true)).map(VA.optional(VA.bool).inj(_)) ++
        List(Some(None), Some(Some(false))).map(VA.optional(VA.optional(VA.bool)).inj(_))

    units ++ bools ++ ints ++ numeric10s ++ numeric0s ++ dates ++ timestamps ++ texts ++ parties ++ contractIds ++ optionals ++ lists ++ textMaps ++ genMaps ++ enums ++ records0 ++ records2 ++ variants
  }

  "KeyHasher.addTypeValue" should {

    "stable " in {

      val expectedOut =
        """ValueUnit
          | faee935763044f124d7526755a5058a33f9402a595994d59eddd4be8546ff201
          |ValueBool(true)
          | fbb59ed10e9cd4ff45a12c5bb92cbd80df984ba1fe60f26a30febf218e2f0f5e
          |ValueBool(false)
          | faee935763044f124d7526755a5058a33f9402a595994d59eddd4be8546ff201
          |ValueInt64(-1)
          | 8c6461aec2028ecd3880ad2243b6e0fdb4033ab46ce1702f5289819fb45f8a93
          |ValueInt64(0)
          | 13c6a7b85fcb0443c1d31dafe22561aac714fbaa99d3b9a56474d8dda0c9aee0
          |ValueInt64(1)
          | 36dd3485b6affcd5d59600c58aca5c1cdc2c01bb0a2956bfaa690d157bc9b2be
          |ValueNumeric(-10000.0000000000)
          | 19d45e6d088423c70208cf7f87cc10429e66ef343c4c608ba69f675562d4be1e
          |ValueNumeric(0E-10)
          | ea87c0c1539dfbbd804c58717ecf30f5b50b946638a39fdaf6712253b952ab40
          |ValueNumeric(10000.0000000000)
          | 1e4b9819cb11e44c0a5a826b7b81e756f3dfaf3061d9f9baa345c1a6c2d7c284
          |ValueNumeric(-10000)
          | 02dfadf86a4fbb948e165e20350e472d087072695feb613f9c9562eccda56be8
          |ValueNumeric(0)
          | ea87c0c1539dfbbd804c58717ecf30f5b50b946638a39fdaf6712253b952ab40
          |ValueNumeric(10000)
          | 5a97286594af94c406d9354d35bf515a12e9d46b61f6dd6d4679e85395fde5f6
          |ValueDate(1970-01-01)
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueDate(1969-07-21)
          | f6538d4cb9a7663f9aeac6cd8b1cb5ddba62337ca3ca2b21b29297d85ec53ae5
          |ValueDate(2019-12-16)
          | 4a5ce4f9f37be5e93ccd1360f5e3d0b93ac4445be1532bfdbe2e0805dc0fc133
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | 13c6a7b85fcb0443c1d31dafe22561aac714fbaa99d3b9a56474d8dda0c9aee0
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | c4f34225847a9d0d7a788df5c9f5aa3ac6e98ae7ad68d29b650411d6c95aaddb
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | 18f13afe32a31b84d8f0e24eba45e710a0dbef47282c81b0be4a361f8aacbb01
          |ValueText()
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueText(someText)
          | 657c0cf2531d5219dc34b4e03f94278f78efc7c90cc0f03b48049bf66572d070
          |ValueText(a¶‱😂)
          | 88ee87e8038f8aa94057d5809adee8d12f9cb6657338171942695fad51fb8df1
          |ValueParty(alice)
          | 274830656c6f7de1daf729d11c57c40ef271a101a831d89e45f034ce7bd71d9d
          |ValueParty(bob)
          | dc1f0fc026d3200a1781f0989dd1801022e028e8afe5d953a033e6d35e8ea50b
          |ValueContractId(ContractId(0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | 0649b1e1e7f34be457c44146e449299109167b9199101349873142ed05878b96
          |ValueContractId(ContractId(0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 0b8c0cc8ebbd56e275b60cf73133387322a42448986dc3858b31eef23098e8e8
          |ValueOptional(None)
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueOptional(Some(ValueBool(false)))
          | ea87c0c1539dfbbd804c58717ecf30f5b50b946638a39fdaf6712253b952ab40
          |ValueOptional(Some(ValueBool(true)))
          | 5b5ca90960b8594498cc778421a40ff2aed14d788d06ede5d4a41207933d3e13
          |ValueOptional(Some(ValueOptional(None)))
          | 86c779d69df35dd466459fa498249d58d0cff42d4a65f112842d0a81d93c3774
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | d9ef2f4d617d921548e1e01da5af2b7ff67e7ed24a0cbd2e29fd30f4cce6ac4e
          |ValueList(FrontStack())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueList(FrontStack(ValueBool(false)))
          | ea87c0c1539dfbbd804c58717ecf30f5b50b946638a39fdaf6712253b952ab40
          |ValueList(FrontStack(ValueBool(true)))
          | 5b5ca90960b8594498cc778421a40ff2aed14d788d06ede5d4a41207933d3e13
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | 8f5dff2ff3f971b847284fb225522005587449fad2746879a0280bbd036f1abc
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | 4f6de867c24682cee05db95d48e1ea47cf5f8b6e74fe07582d3cd8cecaea84b7
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | 768c5b90ed7ae5b727381e331fac83d7defd397d040f46ba067c80ec2af3eb33
          |ValueTextMap(SortedLookupList())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | 4c4384399821a8ed7526d8b29dc6f76ad87014ade285386e7d05d71e61d86c7c
          |ValueTextMap(SortedLookupList((a,ValueBool(true))))
          | 23c43da46c9b2cdc82d54385808ae5b3ffe3606ae516231d2869fea82067c204
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | b0c45256eea6bf29c0390e82ce89efe2974db7af5dad8f14d25dad6a92cf3faf
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | 132fba96cd6130c57d63f8eb2b9a245deaa8a618c4cb9793af32f1190624e6bd
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | 954d9283d02236a4f1cd6d1cdf8f8c8a0ced4fc18f14a8380574c4d09485ec60
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | da9ab333c3de358c2e5aead8a9ced5cbe5dda7fc454ade82180596120c5abdc6
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | 5ac45cbc29a66cd2f10dad87daf37dbb5fa905f5647586fc5f2eafca5d349bac
          |ValueGenMap()
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueGenMap((ValueText(a),ValueBool(false)))
          | 4c4384399821a8ed7526d8b29dc6f76ad87014ade285386e7d05d71e61d86c7c
          |ValueGenMap((ValueText(a),ValueBool(true)))
          | 23c43da46c9b2cdc82d54385808ae5b3ffe3606ae516231d2869fea82067c204
          |ValueGenMap((ValueText(b),ValueBool(false)))
          | b0c45256eea6bf29c0390e82ce89efe2974db7af5dad8f14d25dad6a92cf3faf
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(false)))
          | 132fba96cd6130c57d63f8eb2b9a245deaa8a618c4cb9793af32f1190624e6bd
          |ValueGenMap((ValueText(a),ValueBool(true)),(ValueText(b),ValueBool(false)))
          | 954d9283d02236a4f1cd6d1cdf8f8c8a0ced4fc18f14a8380574c4d09485ec60
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(true)))
          | da9ab333c3de358c2e5aead8a9ced5cbe5dda7fc454ade82180596120c5abdc6
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(c),ValueBool(false)))
          | 5ac45cbc29a66cd2f10dad87daf37dbb5fa905f5647586fc5f2eafca5d349bac
          |ValueEnum(Some(pkgId:Mod:Color),Red)
          | 048b20422b487b8eeba059a219589ad477e5f11eb769c7fea658b63f1bb1d405
          |ValueEnum(Some(pkgId:Mod:Color),Green)
          | ff89416f14a9369d7ef3f9a23057878320aa7b777c7233a79f2b0cab812a3e7a
          |ValueEnum(Some(pkgId:Mod:ColorBis),Green)
          | ff89416f14a9369d7ef3f9a23057878320aa7b777c7233a79f2b0cab812a3e7a
          |ValueRecord(Some(pkgId:Mod:Unit),ImmArray())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 8f5dff2ff3f971b847284fb225522005587449fad2746879a0280bbd036f1abc
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 768c5b90ed7ae5b727381e331fac83d7defd397d040f46ba067c80ec2af3eb33
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 4f6de867c24682cee05db95d48e1ea47cf5f8b6e74fe07582d3cd8cecaea84b7
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 8f5dff2ff3f971b847284fb225522005587449fad2746879a0280bbd036f1abc
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(false))
          | 41edeaec86ac919e3c184057b021753781bd2ac1d60b8d4329375f60df953097
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(true))
          | 31d69356947365e8a3dd9706774182e86774af1aa6550055efc56a22bb594745
          |ValueVariant(Some(pkgId:Mod:Either),Right,ValueBool(false))
          | bd89c47c2379a69e8e0d46ff634c533449e8e7e532e84def4e2b2e168bc786e7
          |ValueVariant(Some(pkgId:Mod:EitherBis),Left,ValueBool(false))
          | 41edeaec86ac919e3c184057b021753781bd2ac1d60b8d4329375f60df953097
          |""".stripMargin

      val sep = System.getProperty("line.separator")
      val actualOutput = stabilityTestCases
        .map { value =>
          val hash = Hash
            .builder(Hash.Purpose.Testing, Hash.aCid2Bytes, upgradable = false)
            .addTypedValue(value)
            .build
            .toHexString
          s"${value.toString}$sep $hash"
        }
        .mkString("", sep, sep)
      actualOutput shouldBe expectedOut

    }
  }

  "addTypeValue upgradable values" should {

    def hash(x: Value) = Hash
      .builder(Hash.Purpose.Testing, Hash.aCid2Bytes, upgradable = true)
      .addTypedValue(x)
      .build

    "stable " in {

      val expectedOutAsNonWrapped =
        """ValueUnit
          | 9f44ac6acb8a37b00b615dc89259d306035be82dee7db2b472e4c48326195440
          |ValueBool(true)
          | 75c8fd04ad916aec3e3d5cb76a452b116b3d4d0912a0a485e9fb8e3d240e210c
          |ValueBool(false)
          | 9f44ac6acb8a37b00b615dc89259d306035be82dee7db2b472e4c48326195440
          |ValueInt64(-1)
          | f26fc3783d5b35ffb2fe7945ede3d29623535b8af80552ade5d34a5ce9876427
          |ValueInt64(0)
          | 627d256f1d5abaa3a870e08d021ec18b8112b3e6ff0ef4a15a948eac307b20dd
          |ValueInt64(1)
          | eb52bdaca91ec03c5f17216695b93acbc2e18ee88da59167cf3fbc4e4a1de445
          |ValueNumeric(-10000.0000000000)
          | 9e461a8a49688dd809fb7fe9f93840bc09cb9a73157efbeb9d5e74a1449ddbae
          |ValueNumeric(0E-10)
          | 92279f2e8f32b4c8d8122e22f6d185424b36d678cf5a4dd041e99482977dac3c
          |ValueNumeric(10000.0000000000)
          | fb13320481e43b753f3d97c245b2d78cc5aec4ab29d8ca2b18fe0564a5587a67
          |ValueNumeric(-10000)
          | 1a1cc1480738da9f759c3016edd29e9559918f52e42e92ac2414196db37ce229
          |ValueNumeric(0)
          | 92279f2e8f32b4c8d8122e22f6d185424b36d678cf5a4dd041e99482977dac3c
          |ValueNumeric(10000)
          | db66926bbbf01e053a2b6490ad77e08e6440cd649bb3228f434ba7c066e2141c
          |ValueDate(1970-01-01)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueDate(1969-07-21)
          | 5d5eba22a3723cd732677964e8a0789544999655072a028e3f07106c7188bc41
          |ValueDate(2019-12-16)
          | 94ff9110c3f5c9dc0f6d8a67b17a81e8e17e7f83915d65943508c04f01ac93d0
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | 627d256f1d5abaa3a870e08d021ec18b8112b3e6ff0ef4a15a948eac307b20dd
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | 0938a77f2c6ceef596ef35c8710a2b924126eba7406fb0ad55591113418deadb
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | e6867b934ce478fa4aa47129ba0e1ef44bef80516fa8283c05819470510eba4a
          |ValueText()
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueText(someText)
          | aa0c9bac7049aa6590344e7087fa4fe96c87256701f1333b3141cdc3da986df9
          |ValueText(a¶‱😂)
          | 82ac6804ce9420de48c14353da1b4a6a617d24531240275b44e719078a9d291d
          |ValueParty(alice)
          | b2623630e6d08b83fa8c247fa735ecfe22dcd04e8b55b2cc7971d77df8548053
          |ValueParty(bob)
          | 9186ad42806e6d9d3aa5fd2bf1d60dfecc69c63c1292cc024f9860f38f12af03
          |ValueContractId(ContractId(0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | 98810b1ec3d136e358b3b73a27c6291201d5c87eb331ba60dd1fe337a1e4ff40
          |ValueContractId(ContractId(0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 5ed345419c782aa68b990bb677fd5ae54fb71b049f55095b228eb6c7ba49dbc4
          |ValueOptional(None)
          | 9f44ac6acb8a37b00b615dc89259d306035be82dee7db2b472e4c48326195440
          |ValueOptional(Some(ValueBool(false)))
          | f896c3a5f9841b6e1f0a22bd35a6a1bc5efb28aaa23b66301ec8098ce57cf99a
          |ValueOptional(Some(ValueBool(true)))
          | 27ecd0a598e76f8a2fd264d427df0a119903e8eae384e478902541756f089dd1
          |ValueOptional(Some(ValueOptional(None)))
          | f896c3a5f9841b6e1f0a22bd35a6a1bc5efb28aaa23b66301ec8098ce57cf99a
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | 499ae0600c64765b309ab3cfebe22a70f8e51e7955dc791d01d9149a20d6bbd5
          |ValueList(FrontStack())
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueList(FrontStack(ValueBool(false)))
          | 92279f2e8f32b4c8d8122e22f6d185424b36d678cf5a4dd041e99482977dac3c
          |ValueList(FrontStack(ValueBool(true)))
          | 637513d8155a9059a2796e42c84d31d2540516c5e759befcebfee5691ae7eee7
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | 6343c2264acc770f4a2618f16fbcddc1e4d8a79ba78e8775bd4bdc048f468d35
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | f003659e67e8d9863930f454903c16004fc0b2422c763ece8ee9a576994a5a1d
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | 2038e3437ac028968344be737195eab1e195ca65854716a252b4a5776bbe09d5
          |ValueTextMap(SortedLookupList())
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | ce68b01101cc0225830c030171e3aeb59b5d9749c4ba729276b7e9bd0b069459
          |ValueTextMap(SortedLookupList((a,ValueBool(true))))
          | ae44b0b5a0e2a6a8c601b1768408effa07df26b877421d5ea3b2ee79d6787672
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | a5d6afb1fc59b905ad151db2279bf339c3a47fc343f137b8cc53cd6a1976b560
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | c22f164b80188197527b0cfbe7ad76b109b85f6996704f29777a100ad70f2518
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | cf4cb65205f80c38bf99ea0cd3e70131deb4162dd27006401800778939723dc4
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | acb314ced6ec890e313d66c78530e3600b40e01b7ea7401cf46c30abfe63f8da
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | 1140f3cb287564bdc164c930cc53cd91e14c3cd699df05281e67498b4aab0850
          |ValueGenMap()
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueGenMap((ValueText(a),ValueBool(false)))
          | ce68b01101cc0225830c030171e3aeb59b5d9749c4ba729276b7e9bd0b069459
          |ValueGenMap((ValueText(a),ValueBool(true)))
          | ae44b0b5a0e2a6a8c601b1768408effa07df26b877421d5ea3b2ee79d6787672
          |ValueGenMap((ValueText(b),ValueBool(false)))
          | a5d6afb1fc59b905ad151db2279bf339c3a47fc343f137b8cc53cd6a1976b560
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(false)))
          | c22f164b80188197527b0cfbe7ad76b109b85f6996704f29777a100ad70f2518
          |ValueGenMap((ValueText(a),ValueBool(true)),(ValueText(b),ValueBool(false)))
          | cf4cb65205f80c38bf99ea0cd3e70131deb4162dd27006401800778939723dc4
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(true)))
          | acb314ced6ec890e313d66c78530e3600b40e01b7ea7401cf46c30abfe63f8da
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(c),ValueBool(false)))
          | 1140f3cb287564bdc164c930cc53cd91e14c3cd699df05281e67498b4aab0850
          |ValueEnum(Some(pkgId:Mod:Color),Red)
          | ebc83f880e580deb67fce980b8266788546c4d98a4d28fce2ec110a7584ddb13
          |ValueEnum(Some(pkgId:Mod:Color),Green)
          | 0bd65c99036abfc195ae62c7b057a16bc954c2edd5fb111e28621f6f5cfdbfe3
          |ValueEnum(Some(pkgId:Mod:ColorBis),Green)
          | 0bd65c99036abfc195ae62c7b057a16bc954c2edd5fb111e28621f6f5cfdbfe3
          |ValueRecord(Some(pkgId:Mod:Unit),ImmArray())
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 1729a9355c2ab95ce452a6bcaf3814e056aa71c74a4fb6c01fad4a1683df0c5a
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 1253dad19a6e01ec99096830043f397cc261bf2f9da4bb774fc49d7041934b63
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(false))
          | ad154dbeef12528a512e6ccf5f13589e2623bf407b73e1683f823bb674341636
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(true))
          | 57689c3ea8617da95453e161536788122711133109fa3d7039779b60ef92a691
          |ValueVariant(Some(pkgId:Mod:Either),Right,ValueBool(false))
          | f3f05b039ece5a8008f0cc9268663ab955026d70a63d7862fd2cb28c338b5fcc
          |ValueVariant(Some(pkgId:Mod:EitherBis),Left,ValueBool(false))
          | ad154dbeef12528a512e6ccf5f13589e2623bf407b73e1683f823bb674341636
          |""".stripMargin

      val expectedOutAsWrapped =
        """ValueUnit
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueBool(true)
          | 1729a9355c2ab95ce452a6bcaf3814e056aa71c74a4fb6c01fad4a1683df0c5a
          |ValueBool(false)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueInt64(-1)
          | f30dc19ea78e0c645f96c2789cdeabc3efdf1bde5b07c8aa4d8ac0adbf8f0b64
          |ValueInt64(0)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueInt64(1)
          | 34fb0189471cb3e7704a1bdae8113271761580cff3dcfcd173cc27cde0a4e09e
          |ValueNumeric(-10000.0000000000)
          | b85d4482503afaacd61bd8136eb4dbb41a1789443430e94f283f8e437d58a637
          |ValueNumeric(0E-10)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueNumeric(10000.0000000000)
          | 7cecca9cb8075c469ba19c866e2ddd8d66c4a021d0877a93c05ab2fa340e77f2
          |ValueNumeric(-10000)
          | 15eb7d5f1884234013a3932920b32084c070ff48beffa57682d6c11d9c9b93ac
          |ValueNumeric(0)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueNumeric(10000)
          | 4d3b53f64defce30a1bef5df0cbc8c2d8bfe18e8db545708139226c5a5ad4332
          |ValueDate(1970-01-01)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueDate(1969-07-21)
          | 4c56bd5106fec2ec809375bd79308f901ab1fd6d2cc58a88c774be4dd423695f
          |ValueDate(2019-12-16)
          | 1a0b83dc4f97f08fbde89aa08c5a69dde4fb8e7306f2e3121c9b83f1a8bd5dfc
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | 2fed708bbeebf6ec556f806e865756bd1b0c1312bd2a7523a891495801f7da5f
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | e298de176eb6c5f0a6214be15361011aa33d0bb48e6bbae991266c5bbb607c29
          |ValueText()
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueText(someText)
          | bb4ad1c05b62329c2964b3ad03bc88429d60323612f5204454d2fb050a3b28a8
          |ValueText(a¶‱😂)
          | a6b29469cf9110a49e421209cd711a3be4ce6bb664a18dbc323141e351371c98
          |ValueParty(alice)
          | 343876f3e0d22b22a7c6a63599c2d2cf015abbdd661f0487fc204e60fa4b1e70
          |ValueParty(bob)
          | 9b8089a85c8ddf21bd6cf18d2319bc9cabab338834f3ac75eb531a6f310d03c6
          |ValueContractId(ContractId(0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | bdd68a4ffa145551f7c35533ab8a9049da46203146777974caa160212a424624
          |ValueContractId(ContractId(0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 20ff6ca6cf6d42180c9316b8b59a966f195c6534ab84c2fa6c904b8d15e6212e
          |ValueOptional(None)
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueOptional(Some(ValueBool(false)))
          | 1c843ec06c1b9d81a3a98be5b9812d1b4d33cae0e6273227407b163279ea8b05
          |ValueOptional(Some(ValueBool(true)))
          | 1729a9355c2ab95ce452a6bcaf3814e056aa71c74a4fb6c01fad4a1683df0c5a
          |ValueOptional(Some(ValueOptional(None)))
          | 1c843ec06c1b9d81a3a98be5b9812d1b4d33cae0e6273227407b163279ea8b05
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | d4613fc6871ec104a89254e2f74bfaaf0efa68d17f2183fedbf54fece80b4a04
          |ValueList(FrontStack())
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueList(FrontStack(ValueBool(false)))
          | 36ce2c3f9246eb4127f879fddcdd12ae5734d119e9fbd13c8f3210524eb222d2
          |ValueList(FrontStack(ValueBool(true)))
          | 1402cfc444942c588559984a192e715f168ac27c703dfa1a4446df45bf4f4b6a
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | d1edeaab490632b7ffb5ecfe54ad610152a0ab4b8a88b33ace48f0b72d13ec99
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | bf9fc7fe58c0862eb6023181e10eed77cef51d8f6effec7498191bd081ec38c7
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | 49c4df92250dfc1b37ad3b1882dd6c6643ff90e006099c2d890d943668b5d065
          |ValueTextMap(SortedLookupList())
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | 20e489daff665d24eb5dcc10712d37206bc6356305b4803485725e1e790bb48c
          |ValueTextMap(SortedLookupList((a,ValueBool(true))))
          | 2b3d943cc8e07d2446f4d1ffba6a4075b7e348b2da9e7cf9a92116f303f113b8
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | 846fbb96d8c6b3e97c05ae22dfe4b8907501551728e15928022ccb876fb89ede
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | a6530c4cca594216bb1444b3882f78c930bfd407545d24411a086930e7669c64
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | b2bef380b8def51587bca1aa59ab08b35a6e9c037d7b09789b128c7ac2753039
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | 90540e7c1f1fea016eecbd0bacd50cb5f6f9a624a1e9ef14ea65354d01131185
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | 9813fb2bfc0b10fd7faaaed2673dc7f6f2ba3223a89411c2114ce4e6dda17c35
          |ValueGenMap()
          | 1a71797cab8ed23c72233b7706b166a33049e4e87dfbc55b9e252f9c1843eca6
          |ValueGenMap((ValueText(a),ValueBool(false)))
          | 20e489daff665d24eb5dcc10712d37206bc6356305b4803485725e1e790bb48c
          |ValueGenMap((ValueText(a),ValueBool(true)))
          | 2b3d943cc8e07d2446f4d1ffba6a4075b7e348b2da9e7cf9a92116f303f113b8
          |ValueGenMap((ValueText(b),ValueBool(false)))
          | 846fbb96d8c6b3e97c05ae22dfe4b8907501551728e15928022ccb876fb89ede
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(false)))
          | a6530c4cca594216bb1444b3882f78c930bfd407545d24411a086930e7669c64
          |ValueGenMap((ValueText(a),ValueBool(true)),(ValueText(b),ValueBool(false)))
          | b2bef380b8def51587bca1aa59ab08b35a6e9c037d7b09789b128c7ac2753039
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(true)))
          | 90540e7c1f1fea016eecbd0bacd50cb5f6f9a624a1e9ef14ea65354d01131185
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(c),ValueBool(false)))
          | 9813fb2bfc0b10fd7faaaed2673dc7f6f2ba3223a89411c2114ce4e6dda17c35
          |ValueEnum(Some(pkgId:Mod:Color),Red)
          | 08fe9e55ac1132695f2d9103dcdaf9ebbb78f853e104f4b3acb4e603b49b3e2e
          |ValueEnum(Some(pkgId:Mod:Color),Green)
          | 06de6b8a728491fc2cb88c976f02ab602e95ed8ccb51f6d185a99b84c2749e51
          |ValueEnum(Some(pkgId:Mod:ColorBis),Green)
          | 06de6b8a728491fc2cb88c976f02ab602e95ed8ccb51f6d185a99b84c2749e51
          |ValueRecord(Some(pkgId:Mod:Unit),ImmArray())
          | ec5684692323c81c53bf15e0fe59047f88d3a9adee6eedb9a9a3b7e04f66c8e5
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | ec5684692323c81c53bf15e0fe59047f88d3a9adee6eedb9a9a3b7e04f66c8e5
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | ec5684692323c81c53bf15e0fe59047f88d3a9adee6eedb9a9a3b7e04f66c8e5
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | cd0576a339ec0a8ba86d97a1d15543a436c631fed0cb7437a9db08e98a97cca4
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 034c6c48c8cc35018c3233227eae327eb7220183044513bc46b1bf6e9dc5913e
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | ec5684692323c81c53bf15e0fe59047f88d3a9adee6eedb9a9a3b7e04f66c8e5
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(false))
          | 12d539011cac32ef2b9590e5d2119544711633136166596b5e63d842c8d5ba8f
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(true))
          | 699162132335e6031182cd0bfe7aae28ea8089a374ae1a38126e6ed371720db2
          |ValueVariant(Some(pkgId:Mod:Either),Right,ValueBool(false))
          | 6b768e15128598f088aed642dd7552d869350711ce2ae1b6b2457b2e2a96a975
          |ValueVariant(Some(pkgId:Mod:EitherBis),Left,ValueBool(false))
          | 12d539011cac32ef2b9590e5d2119544711633136166596b5e63d842c8d5ba8f
          |""".stripMargin

      val sep = System.getProperty("line.separator")
      val actualOutputAsNonWrapped = stabilityTestCases
        .map(value => s"${value.toString}$sep ${hash(value).toHexString}")
        .mkString("", sep, sep)
      actualOutputAsNonWrapped shouldBe expectedOutAsNonWrapped
      val actualOutputAsWrapped = stabilityTestCases
        .map(value =>
          s"${value.toString}$sep ${hash(ValueRecord(None, ImmArray(None -> value))).toHexString}"
        )
        .mkString("", sep, sep)
      actualOutputAsWrapped shouldBe expectedOutAsWrapped

    }

    "ignore default values" in {

      val hash0 = hash(ValueRecord(None, ImmArray.empty))
      val hash1 = hash(ValueRecord(None, fields = ImmArray(None -> ValueInt64(42))))
      val hash2 = hash(
        ValueRecord(
          None,
          fields = ImmArray(
            None -> ValueInt64(42),
            None -> ValueRecord(None, ImmArray(None -> ValueText("42"))),
          ),
        )
      )
      assert(Set(hash0, hash1, hash2).size == 3)

      val testCases = Table[Value](
        "value",
        ValueUnit,
        ValueInt64(0),
        ValueNumeric(data.Numeric.assertFromString("0.")),
        Value.ValueTimestamp(data.Time.Timestamp.Epoch),
        Value.ValueDate(data.Time.Date.Epoch),
        Value.ValueText(""),
        Value.ValueOptional(None),
        ValueList(FrontStack.empty),
        ValueTextMap(data.SortedLookupList.Empty),
        ValueGenMap(ImmArray.empty),
      )

      forAll(testCases) { value =>
        hash(ValueRecord(None, fields = ImmArray(None -> value))) shouldBe hash0
        hash(
          ValueRecord(None, fields = ImmArray(None -> ValueInt64(42), None -> value))
        ) shouldBe hash1
        hash(
          ValueRecord(
            None,
            fields = ImmArray(
              None -> ValueInt64(42),
              None -> ValueRecord(None, ImmArray(None -> ValueText("42"), None -> value)),
            ),
          )
        )
      }
    }

  }

  "Hash.fromString" should {
    "convert properly string" in {
      val s = "01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086"
      Hash.assertFromString(s).toHexString shouldBe s
    }
  }

  "Hash.derive" should {

    val k1 =
      Hash.assertFromString("01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086")
    val k2 =
      Hash.assertFromString("5a97286594af94c406d9354d35bf515a12e9d46b61f6dd6d4679e85395fde5f6")
    val p1 = Ref.Party.assertFromString("alice")
    val p2 = Ref.Party.assertFromString("bob")

    "not produce collisions" in {
      val set = for {
        k <- Set(k1, k2)
        p <- Set(p1, p2)
      } yield Hash.deriveMaintainerContractKeyUUID(k, p)

      set.size shouldBe 4
    }

    "be stable" in {
      Hash.deriveMaintainerContractKeyUUID(k1, p1) shouldBe Hash.assertFromString(
        "6ac76f1cb2b75305a6c910641ae39463321e09104d49d9aa32638d1d3286430c"
      )
      Hash.deriveMaintainerContractKeyUUID(k2, p2) shouldBe Hash.assertFromString(
        "6874798ccf6ec1577955d61a6b6d96247f823515ef3afe8b1e086b3533a4fd56"
      )
    }
  }

  "Hash.hashContractKey" should {

    val templateId = defRef(name = "upgradable")

    "produce a different hash for different package Names" in {
      val h1 = Hash.assertHashContractKey(
        templateId,
        Ref.PackageName.assertFromString("package-name-1"),
        ValueTrue,
      )
      val h2 = Hash.assertHashContractKey(
        templateId,
        Ref.PackageName.assertFromString("package-name-2"),
        ValueTrue,
      )
      h1 shouldNot be(h2)
    }

    "produce an identical hash to the same template in a different package" in {
      val h1 = Hash.assertHashContractKey(
        templateId.copy(packageId = Ref.PackageId.assertFromString("package-1")),
        packageName0,
        ValueTrue,
      )
      val h2 = Hash.assertHashContractKey(
        templateId.copy(packageId = Ref.PackageId.assertFromString("package-2")),
        packageName0,
        ValueTrue,
      )
      h1 shouldBe h2
    }

  }

  private def defRef(module: String = "Module", name: String): Ref.Identifier =
    Ref.Identifier(
      packageId0,
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(module),
        Ref.DottedName.assertFromString(name),
      ),
    )

}
