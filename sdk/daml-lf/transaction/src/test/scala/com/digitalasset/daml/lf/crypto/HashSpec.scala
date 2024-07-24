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
import shapeless.{Coproduct => HSum, HNil}

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
      val hash = "a8a73e9b78a46d4179bdd2f15cc002cfd30f864a451eae5b142b6fb61a67ccd7"
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

    "not produce collision in list of text" in {
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
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | cd29d6c22cec7641df8990db49f9ade8a0bbd8871289b901ad0ab3c10dfb6b65
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 5b5ca90960b8594498cc778421a40ff2aed14d788d06ede5d4a41207933d3e13
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(false))
          | 41edeaec86ac919e3c184057b021753781bd2ac1d60b8d4329375f60df953097
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(true))
          | 31d69356947365e8a3dd9706774182e86774af1aa6550055efc56a22bb594745
          |ValueVariant(Some(pkgId:Mod:Either),Right,ValueBool(false))
          | bd89c47c2379a69e8e0d46ff634c533449e8e7e532e84def4e2b2e168bc786e7
          |ValueVariant(Some(pkgId:Mod:EitherBis),Left,ValueBool(false))
          | 41edeaec86ac919e3c184057b021753781bd2ac1d60b8d4329375f60df953097
          |""".stripMargin

      val expectedOutAsWrapped =
        """ValueUnit
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueBool(true)
          | cd29d6c22cec7641df8990db49f9ade8a0bbd8871289b901ad0ab3c10dfb6b65
          |ValueBool(false)
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueInt64(-1)
          | 4d6fcec1f99851d47f4346909337fa6225f3a8f17c16e7d3153ab74afc132385
          |ValueInt64(0)
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueInt64(1)
          | 6c6debb65d6612a4506498a8e06caf69bfac7ff2e2b57bc552bb7875ecccc2e2
          |ValueNumeric(-10000.0000000000)
          | ea314e6196717d20300779bd72332dcd4dda6ca90e7f5ea8fad406dfb98b8f08
          |ValueNumeric(0E-10)
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueNumeric(10000.0000000000)
          | 270759e288db28ccc1dae8698101fde77f816b62850300ee3d4dee1c2235d5ec
          |ValueNumeric(-10000)
          | b3c4c998cc1bcc9e977bb620d3e45d2f1960ab376a5098c4d50a0fe8bda76270
          |ValueNumeric(0)
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueNumeric(10000)
          | f750b5e31726d95a816319586c4db3eb528468515facf2c62e22e46d05a0eb21
          |ValueDate(1970-01-01)
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueDate(1969-07-21)
          | b9f4e9c1d5abc28566efc63b891c44a0b7de52e6ac5419e41a89f155287bdb64
          |ValueDate(2019-12-16)
          | 356a8dac4379766c4f7e43804cbd6738497f4fb544386f4516e4012b43e01273
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | 27c3e785e401e9a888c0cd31b0014557214bd1dd6f34e279572434b2ff42300a
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | f8524e858702d78a9db8a31ee3f7fe7ef13a8bfc590417b51d77e08f9af130c0
          |ValueText()
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueText(someText)
          | e40a721ed6d6c8d7d6c41b776e58367a52c0adc93c52def30665f5f91b583666
          |ValueText(a¶‱😂)
          | 65918d97eec5068f62dc263a230662e5de38a9531f728096ac653d909cd2b330
          |ValueParty(alice)
          | 5c519d29edf6aebc7688b9dedc2a878d9d8c7eafd45ae23245f3a110fa717694
          |ValueParty(bob)
          | eb2ae47dd9c81965191d2cdb01d0bb69bfac805f400f6d6f4495c36bbd86f3c1
          |ValueContractId(ContractId(0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | ad2c11a7e545bde57ed758923cb15fb9517a1ef6fa1542cc1a51e21e51d8ceb8
          |ValueContractId(ContractId(0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 87a3c309519fb745e4ee87f0d92c980fc7ad59e0a7635f159cf53c11843c54d2
          |ValueOptional(None)
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueOptional(Some(ValueBool(false)))
          | 926bd18700e14d0a029dbd3fdd2c5b8d94b62a757027152869a1170225cbbaa5
          |ValueOptional(Some(ValueBool(true)))
          | cd29d6c22cec7641df8990db49f9ade8a0bbd8871289b901ad0ab3c10dfb6b65
          |ValueOptional(Some(ValueOptional(None)))
          | 13c6a7b85fcb0443c1d31dafe22561aac714fbaa99d3b9a56474d8dda0c9aee0
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | 83a1d1b8cfa23c2405c1af57599886963df318dbcee2d3512fc80744e244624a
          |ValueList(FrontStack())
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueList(FrontStack(ValueBool(false)))
          | 83a1d1b8cfa23c2405c1af57599886963df318dbcee2d3512fc80744e244624a
          |ValueList(FrontStack(ValueBool(true)))
          | 73b8c021cfafadd055353578ec89b4473f1bb0b2228076fc36e1d717bdda6403
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | 0a6201545791949e4bc09999a60cd453cee9fb20fff07ae114ee20917416a97e
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | 80ad109cde9931d7bd892d13b6c9fe0a70667e71eb1eb3525a5bd4e81171c5d7
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | 0e418808c77c52eaf9a56729997013ccb2584ee73e481d575624a0509a908292
          |ValueTextMap(SortedLookupList())
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | 21f6cb72b5c1414b07d9fa9903ce2e83c01c8a381163eb8f55dee7a1b691e1ab
          |ValueTextMap(SortedLookupList((a,ValueBool(true))))
          | 58c7b2ebe1851a5e64fe59be4a3c27dc2d87046b2a75b981f0f2d1a7f4525826
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | a49c187a9c67afb30f206e0a6aa16af5f0c2e490a54b0030c5335051de38af1f
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | 35bb07e06d59d622055e2f6c48cc3aa9b3813885c1db7d3663cc78081c1c6a15
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | fd0707854e5ad18a3ed155bd329fc58809f53d7e2b7ae1c76811e4be54994773
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | 996219badba34f8b2d83c8394b41bfcf56b2a4d89535488a3e22405b416e497c
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | dde8d89d798a993d37a994e116fa71152d3fb1d0dcb85b245047ac20e5a297ac
          |ValueGenMap()
          | b413f47d13ee2fe6c845b2ee141af81de858df4ec549a58b7970bb96645bc8d2
          |ValueGenMap((ValueText(a),ValueBool(false)))
          | 21f6cb72b5c1414b07d9fa9903ce2e83c01c8a381163eb8f55dee7a1b691e1ab
          |ValueGenMap((ValueText(a),ValueBool(true)))
          | 58c7b2ebe1851a5e64fe59be4a3c27dc2d87046b2a75b981f0f2d1a7f4525826
          |ValueGenMap((ValueText(b),ValueBool(false)))
          | a49c187a9c67afb30f206e0a6aa16af5f0c2e490a54b0030c5335051de38af1f
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(false)))
          | 35bb07e06d59d622055e2f6c48cc3aa9b3813885c1db7d3663cc78081c1c6a15
          |ValueGenMap((ValueText(a),ValueBool(true)),(ValueText(b),ValueBool(false)))
          | fd0707854e5ad18a3ed155bd329fc58809f53d7e2b7ae1c76811e4be54994773
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(b),ValueBool(true)))
          | 996219badba34f8b2d83c8394b41bfcf56b2a4d89535488a3e22405b416e497c
          |ValueGenMap((ValueText(a),ValueBool(false)),(ValueText(c),ValueBool(false)))
          | dde8d89d798a993d37a994e116fa71152d3fb1d0dcb85b245047ac20e5a297ac
          |ValueEnum(Some(pkgId:Mod:Color),Red)
          | 41132598210c71974f18e2dceed6be79a2070f27d94c3bf9b12f5492a485c22f
          |ValueEnum(Some(pkgId:Mod:Color),Green)
          | a0e6a213e8f20f6713f20160032ffab5ba705fd0d06259215b38c168b2738a7c
          |ValueEnum(Some(pkgId:Mod:ColorBis),Green)
          | a0e6a213e8f20f6713f20160032ffab5ba705fd0d06259215b38c168b2738a7c
          |ValueRecord(Some(pkgId:Mod:Unit),ImmArray())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 817ef4e6515b36a321ce0cf0669df2d8a434c60ab40c2849c8035072366fc85a
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 73b8c021cfafadd055353578ec89b4473f1bb0b2228076fc36e1d717bdda6403
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(false))
          | ceb9199b55658e8bdc2f628e92a9fffbedae46ac6fdf605ba34b3ff0912f1f9b
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(true))
          | 89fc83022d6b8a76b31317e09192c509483e74ede2346b84c0eab7024d482e12
          |ValueVariant(Some(pkgId:Mod:Either),Right,ValueBool(false))
          | c0deebea802536cb0b1b6ec4a837e0d58ed280c62b4e3b457c3a9568648447ce
          |ValueVariant(Some(pkgId:Mod:EitherBis),Left,ValueBool(false))
          | ceb9199b55658e8bdc2f628e92a9fffbedae46ac6fdf605ba34b3ff0912f1f9b
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
