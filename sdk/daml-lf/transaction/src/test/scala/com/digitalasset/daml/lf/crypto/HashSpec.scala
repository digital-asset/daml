// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      val hash = "02e0bc59349374de68d5ea3be65d17cd728aabfc95a65e35ac21a354c38144ce"
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

    "not produce collision in list of texts" in {
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

    "not produce collision in Map of records" in {
      def ref8(x8: Long) = {
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
            None -> ValueUnit,
            None -> ValueInt64(x8),
          ),
        )
      }

      def genmap(elements: (String, Value)*) =
        ValueGenMap(ImmArray.from(elements.map { case (k, v) => ValueText(k) -> v }))

      // Not that the hahes of value1 and value2 collide if the <record-end-termination> is empty
      val value1 = genmap(
        "0" -> // '00000001' + '30'
          ref8(0x3030303030303030L), // '00000008' + '3030303030303030'
        "00000000" -> // '00000008' + '3030303030303030'
          ref8(0), // <record-end-termination>
      ) // <record-end-termination>
      val value2 = genmap(
        "0" -> // '00000001' + '30'
          ref8(0), // <record-end-termination>
        "00000000" -> // '00000008' + '30303030'
          ref8(0x3030303030303030L), // '00000008' + '30303030'
      ) // <record-end-termination>

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

    implicit def toTypeConId(s: String): Ref.TypeConId =
      Ref.TypeConId(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

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
            .builder(Hash.Purpose.Testing, Hash.aCid2Bytes, upgradeFriendly = false)
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
      .builder(Hash.Purpose.Testing, Hash.aCid2Bytes, upgradeFriendly = true)
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
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 509e422770f878df46204b70c128fc4ab4246c9eb1a45166e791cf58b78050f5
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | f2f312e8c015c1ffcdf340f6949e336b893b6481879bad246c679559339c6a71
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
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
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueBool(true)
          | 509e422770f878df46204b70c128fc4ab4246c9eb1a45166e791cf58b78050f5
          |ValueBool(false)
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueInt64(-1)
          | 680dd5f1d9efc754c1d0be6b3cebb71625c143cdd69d176b33a5ee52907f64b4
          |ValueInt64(0)
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueInt64(1)
          | 97f634413c56080177efb6f79a6bfe937d450d513cf9798b8622419ddb352a23
          |ValueNumeric(-10000.0000000000)
          | 0ee8e6f5ad359097537d7ee486efd3c0680057494fdc7a876b7f15dba3acd1af
          |ValueNumeric(0E-10)
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueNumeric(10000.0000000000)
          | 1b8eca057dbf26d3dfeb54f61bf31c18664f40c656f4746ca011db287a49548a
          |ValueNumeric(-10000)
          | e3bc6346190d078f43975136ccdd6e895b6ad8575792a1572a30f68c08939394
          |ValueNumeric(0)
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueNumeric(10000)
          | 42791560da3c56ebc15ce2f369ed677ed2d543db6710eceba5d09daaa4cacc86
          |ValueDate(1970-01-01)
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueDate(1969-07-21)
          | 14be9da961156e9b784122eb085040a94294c933204141a303841ef147db1a3d
          |ValueDate(2019-12-16)
          | 7d615a32e360b4987ed7ef31b2fe0437c91b232eee8ce1955b9c6080375e314c
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | 462fff30dea753c1310d615f30699b6df5a4b45664411d5ea7a6610871c8b74e
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | 41899ae946dd507bc63c9bef415c7d32f83fc1fe381857965b4af6e4def7511f
          |ValueText()
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueText(someText)
          | 59f72a644cc900c2a61e9d99c19c54e6ffa7170f5fc2b3da46d5be80b5e6527f
          |ValueText(a¶‱😂)
          | 0f48d723bab49fb3d0ee3d26863dc1b46bc0bf791a4886068c1b92359f56b2c0
          |ValueParty(alice)
          | e25c8a7108b51669637d070cc11ad8b07e5ef9efdf5e9c1f97378327209eb599
          |ValueParty(bob)
          | f283f7245157477f2a368446b5985b2b4b9e738685b46db9d781da01c49494aa
          |ValueContractId(ContractId(0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | c2aebcd0d95d08b9fa7165a373cc0198ec445400f3852af4f5c8b5e7eb326dab
          |ValueContractId(ContractId(0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | f67919370b8e43abb9221a2b968bac456b88cfd28f10289add9711d8529bb631
          |ValueOptional(None)
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueOptional(Some(ValueBool(false)))
          | 22ae2b8e7a5da85559baebfdca136d02f9721b91a36534e947aca4cd2035e480
          |ValueOptional(Some(ValueBool(true)))
          | 509e422770f878df46204b70c128fc4ab4246c9eb1a45166e791cf58b78050f5
          |ValueOptional(Some(ValueOptional(None)))
          | 22ae2b8e7a5da85559baebfdca136d02f9721b91a36534e947aca4cd2035e480
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | 531b78b4eca6252ac436e2f99be7c652954ba08a37be46fb8fbfc94e285727cd
          |ValueList(FrontStack())
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueList(FrontStack(ValueBool(false)))
          | 1b3824d2bbf63ef6ed6c64775053e7ac262bd107d9a1b9a1ad272e758beb2e97
          |ValueList(FrontStack(ValueBool(true)))
          | d04b562f85df3b0c3024e38aaa5fb2d142869697e98190514966d5e6b9733af5
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | a82c98e5beea06668c787695bbdd2e8ad126c3c7b2ace83dd3402f39b3ba1784
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | a6f4538c8ee31a7170ee4eff2a41b30183cdfa270ff7eac8c345463f7194beb5
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | dd2430e3a369a0321f8d0a3a535524e645b5c16f84b3cd8a041ff1e9f5a926ff
          |ValueTextMap(SortedLookupList())
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | 512f3ce939d729e21d85e3269e072fd9b541d354cf8a3c96504868ca45ddc79e
          |ValueTextMap(SortedLookupList((a,ValueBool(true))))
          | 9ed5b8a10c68d0716b62d0b2f96d9a4817b2052268f9eb1be1fa589f457213c4
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | a70a026baf77248379c7760b5aabcc5dcc8de7437f58f06213bcde9cf2cf5745
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | 2a59499c5e9f9ed34bbb6571d08ca8f5ab50e64872070356c53cbd4480d0536d
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | dd68780bb7213d69d5ac7f2f2cf300ff777164feeaab67e13642f2a08cd2df3c
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | 8969a59cd6fcd9afcd711ed8a58fb4388a47abeada0563bf27fc4bb234b51e07
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | 7f55bec51828eb4d039223313ca9b14c51d49849f05baca4035d19375526f541
          |ValueGenMap()
          | ff9b5729d5ed88f97042a190185d50112cecdbd62ff67f57dcde221e63bc95a7
          |ValueGenMap((ValueText(a),ValueBool(false)))
          | 512f3ce939d729e21d85e3269e072fd9b541d354cf8a3c96504868ca45ddc79e
          |ValueGenMap((ValueText(a),ValueBool(true)))
          | 9ed5b8a10c68d0716b62d0b2f96d9a4817b2052268f9eb1be1fa589f457213c4
          |ValueGenMap((ValueText(b),ValueBool(false)))
          | a70a026baf77248379c7760b5aabcc5dcc8de7437f58f06213bcde9cf2cf5745
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(false)))
          | 2a59499c5e9f9ed34bbb6571d08ca8f5ab50e64872070356c53cbd4480d0536d
          |ValueGenMap((ValueText(a),ValueBool(true)),(ValueText(b),ValueBool(false)))
          | dd68780bb7213d69d5ac7f2f2cf300ff777164feeaab67e13642f2a08cd2df3c
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(true)))
          | 8969a59cd6fcd9afcd711ed8a58fb4388a47abeada0563bf27fc4bb234b51e07
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(c),ValueBool(false)))
          | 7f55bec51828eb4d039223313ca9b14c51d49849f05baca4035d19375526f541
          |ValueEnum(Some(pkgId:Mod:Color),Red)
          | fe61975c99886ae2beb4bd80ee42c852df6b3397d9b1e8300fdb5a035680fbce
          |ValueEnum(Some(pkgId:Mod:Color),Green)
          | a18b9e16b80d6cc8c18ce5f6828c21a65935c211e8e21d498dd542a4c4caa256
          |ValueEnum(Some(pkgId:Mod:ColorBis),Green)
          | a18b9e16b80d6cc8c18ce5f6828c21a65935c211e8e21d498dd542a4c4caa256
          |ValueRecord(Some(pkgId:Mod:Unit),ImmArray())
          | ef07c685a9e7098418470f82267513bcd6ba636723edd58c2024c22b5c102026
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | ef07c685a9e7098418470f82267513bcd6ba636723edd58c2024c22b5c102026
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | ef07c685a9e7098418470f82267513bcd6ba636723edd58c2024c22b5c102026
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | aa9f3049056994e06ba8c56d45fe18f2be1b0bb70acde2048aec0e261375a2f5
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 25b28030aba88c650f998ed2996f6a5cf25a8581b6401fc650a9556396d77499
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | ef07c685a9e7098418470f82267513bcd6ba636723edd58c2024c22b5c102026
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(false))
          | 719c43d44282e13f054cdf6163cd980b2d6e7d40f172e22c6ea8897d0324247f
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(true))
          | 8fed22c309f7cdcfe5e1d2ca26ea417e0ff5954115c0f46df1aa7b301b03bada
          |ValueVariant(Some(pkgId:Mod:Either),Right,ValueBool(false))
          | 299133ad5e1463493cfc236432b5f259b7fd200e6f24def1d12ae849dd9e895f
          |ValueVariant(Some(pkgId:Mod:EitherBis),Left,ValueBool(false))
          | 719c43d44282e13f054cdf6163cd980b2d6e7d40f172e22c6ea8897d0324247f
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
        "486880a422a8740654d4cc114789f789d8b1d65d644773a6c9574b1c5db39b49"
      )
      Hash.deriveMaintainerContractKeyUUID(k2, p2) shouldBe Hash.assertFromString(
        "ad67f68d68df9045dc348a1c2ddcc2be10834665b6828e92384f43bbc644322e"
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
        templateId.copy(pkg = Ref.PackageId.assertFromString("package-1")),
        packageName0,
        ValueTrue,
      )
      val h2 = Hash.assertHashContractKey(
        templateId.copy(pkg = Ref.PackageId.assertFromString("package-2")),
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
