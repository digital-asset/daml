// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.digitalasset.daml.lf.data.{Decimal, Numeric, Ref, SortedLookupList, Time}
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.TypedValueGenerators.{RNil, ValueAddend => VA}
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.{Value, ValueVersion}
import org.scalatest.{Matchers, WordSpec}
import shapeless.record.{Record => HRecord}
import shapeless.syntax.singleton._
import shapeless.{Coproduct => HSum}

import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class HashSpec extends WordSpec with Matchers {

  private val packageId0 = Ref.PackageId.assertFromString("package")

  private val complexRecordT =
    VA.record(
        defRef(name = "ComplexRecord"),
        'fInt0 ->> VA.int64
          :: 'fInt1 ->> VA.int64
          :: 'fInt2 ->> VA.int64
          :: 'fNumeric0 ->> VA.numeric(Decimal.scale)
          :: 'fNumeric1 ->> VA.numeric(Decimal.scale)
          :: 'fBool0 ->> VA.bool
          :: 'fBool1 ->> VA.bool
          :: 'fDate0 ->> VA.date
          :: 'fDate1 ->> VA.date
          :: 'fTime0 ->> VA.timestamp
          :: 'fTime1 ->> VA.timestamp
          :: 'fText0 ->> VA.text
          :: 'fTest1 ->> VA.text
          :: 'fPArty ->> VA.party
          :: 'fUnit ->> VA.unit
          :: 'fOpt0 ->> VA.optional(VA.text)
          :: 'fOpt1 ->> VA.optional(VA.text)
          :: 'fList ->> VA.list(VA.text)
          :: 'fVariant ->>
            VA.variant(
                defRef(name = "Variant"),
                'Variant ->> VA.int64 :: RNil,
              )
              ._2
          :: 'fRecord ->>
            VA.record(
                defRef(name = "Record"),
                'field1 ->> VA.text :: 'field2 ->> VA.text :: RNil,
              )
              ._2
          :: 'fTextMap ->> VA.map(VA.text)
          :: RNil,
      )
      ._2

  private val complexRecordV: complexRecordT.Inj[Nothing] =
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
      fVariant = HSum('Variant ->> 0L),
      fRecord = HRecord(field1 = "field1", field2 = "field2"),
      fTextMap = SortedLookupList(Map("keyA" -> "valueA", "keyB" -> "valueB")),
    )

  "KeyHasher" should {

    "be stable" in {
      val hash = "ea24627f5b014af67dbedb13d950e60be7f96a1a5bd9fb1a3b9a85b7fa9db4bc"
      val value = complexRecordT.inj(complexRecordV)
      val name = defRef("module", "name")
      Hash.hashContractKey(GlobalKey(name, value)).toLedgerString shouldBe hash
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector
        .fill(1000)(GlobalKey(defRef("module", "name"), complexRecordT.inj(complexRecordV)))
        .par
        .map(Hash.hashContractKey)

      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = VA.text.inj("A")

      val hash1 = Hash.hashContractKey(GlobalKey(defRef("AA", "A"), value))
      val hash2 = Hash.hashContractKey(GlobalKey(defRef("A", "AA"), value))

      hash1 should !==(hash2)
    }

    "not produce collision in list of text" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      def list(elements: String*) = VA.list(VA.text).inj(elements.toVector)
      val value1 = list("AA", "A")
      val value2 = list("A", "AA")

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in list of decimals" in {
      // Testing whether decimals are delimited: [10, 10] vs [101, 0]
      def list(elements: String*) =
        VA.list(VA.numeric(Decimal.scale)).inj(elements.map(Numeric.assertFromString).toVector)
      val value1 = list("10.0000000000", "10.0000000000")
      val value2 = list("101.0000000000", "0.0000000000")

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in list of lists" in {
      // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
      def list(elements: Vector[Unit]*) = VA.list(VA.list(VA.unit)).inj(elements.toVector)
      val value1 = list(Vector(()), Vector((), ()))
      val value2 = list(Vector((), ()), Vector(()))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Variant constructor" in {
      val variantT =
        VA.variant(defRef(name = "Variant"), 'A ->> VA.unit :: 'B ->> VA.unit :: RNil)._2
      val value1 = variantT.inj(HSum[variantT.Inj[Nothing]]('A ->> (())))
      val value2 = variantT.inj(HSum[variantT.Inj[Nothing]]('B ->> (())))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Variant value" in {
      val variantT = VA.variant(defRef(name = "Variant"), 'A ->> VA.int64 :: RNil)._2
      val value1 = variantT.inj(HSum('A ->> 0L))
      val value2 = variantT.inj(HSum('A ->> 1L))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Map keys" in {
      def textMap(elements: (String, Long)*) =
        VA.map(VA.int64).inj(SortedLookupList(elements.toMap))
      val value1 = textMap("A" -> 0, "B" -> 0)
      val value2 = textMap("A" -> 0, "C" -> 0)

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Map values" in {
      def textMap(elements: (String, Long)*) =
        VA.map(VA.int64).inj(SortedLookupList(elements.toMap))
      val value1 = textMap("A" -> 0, "B" -> 0)
      val value2 = textMap("A" -> 0, "B" -> 1)

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Bool" in {
      val value1 = ValueTrue
      val value2 = ValueFalse

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Int64" in {
      val value1 = ValueInt64(0L)
      val value2 = ValueInt64(1L)

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Decimal" in {
      val value1 = ValueNumeric(Numeric.assertFromString("0."))
      val value2 = ValueNumeric(Numeric.assertFromString("1."))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Date" in {
      val value1 = ValueDate(Time.Date.assertFromDaysSinceEpoch(0))
      val value2 = ValueDate(Time.Date.assertFromDaysSinceEpoch(1))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Timestamp" in {
      val value1 = ValueTimestamp(Time.Timestamp.assertFromLong(0))
      val value2 = ValueTimestamp(Time.Timestamp.assertFromLong(1))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Optional" in {
      val value1 = ValueNone
      val value2 = ValueOptional(Some(ValueUnit))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Record" in {
      val recordT =
        VA.record(defRef(name = "Tuple2"), '_1 ->> VA.text :: '_2 ->> VA.text :: RNil)._2
      val value1 = recordT.inj(HRecord(_1 = "A", _2 = "B"))
      val value2 = recordT.inj(HRecord(_1 = "A", _2 = "C"))

      val tid = defRef("module", "name")

      val hash1 = Hash.hashContractKey(GlobalKey(tid, value1))
      val hash2 = Hash.hashContractKey(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }
  }

  "KeyHasher.putValue" should {

    "stable " in {

      type V = Value[AbsoluteContractId]

      val pkgId = Ref.PackageId.assertFromString("pkgId")

      implicit def toTypeConName(s: String): Ref.TypeConName =
        Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

      implicit def toName(s: String): Ref.Name =
        Ref.Name.assertFromString(s)

      val units = List(ValueUnit)
      val bools = List(true, false).map(VA.bool.inj)
      val ints = List(-1L, 0L, 1L).map(VA.int64.inj)
      val decimals = List("-10000.0000000000", "0.0000000000", "10000.0000000000")
        .map(Numeric.assertFromString)
        .map(VA.numeric(Decimal.scale).inj)
      val numeric0s = List("-10000.", "0.", "10000.")
        .map(Numeric.assertFromString)
        .map(VA.numeric(Numeric.Scale.MinValue).inj)
      val texts = List("", "someText", "a¶‱😂").map(VA.text.inj)
      val dates =
        List(
          Time.Date.assertFromDaysSinceEpoch(0),
          Time.Date.assertFromString("1969-07-21"),
          Time.Date.assertFromString("2019-12-16"),
        ).map(VA.date.inj)
      val timestamps =
        List(
          Time.Timestamp.assertFromLong(0),
          Time.Timestamp.assertFromString("1969-07-21T02:56:15.000000Z"),
          Time.Timestamp.assertFromString("2019-12-16T11:17:54.940779363Z"),
        ).map(VA.timestamp.inj)
      val parties =
        List(
          Ref.Party.assertFromString("alice"),
          Ref.Party.assertFromString("bob"),
        ).map(VA.party.inj)
      val contractIds =
        List(
          "07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5",
          "59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b",
        ).map(x => AbsoluteContractId(Ref.ContractIdString.assertFromString(x)))
          .map(VA.contractId.inj)

      val enumT1 = VA.enum("Color", List("Red", "Green"))._2
      val enumT2 = VA.enum("ColorBis", List("Red", "Green"))._2

      val enums = List(
        enumT1.inj(enumT1.get("Red").get),
        enumT1.inj(enumT1.get("Green").get),
        enumT2.inj(enumT2.get("Green").get),
      )

      val record0T1 = VA.record("Unit", RNil)._2
      val record0T2 = VA.record("UnitBis", RNil)._2

      val records0 =
        List(
          record0T1.inj(HRecord()),
          record0T2.inj(HRecord()),
        )

      val record2T1 = VA.record("Tuple", '_1 ->> VA.bool :: '_2 ->> VA.bool :: RNil)._2
      val record2T2 = VA.record("TupleBis", '_1 ->> VA.bool :: '_2 ->> VA.bool :: RNil)._2

      val records2 =
        List(
          record2T1.inj(HRecord(_1 = false, _2 = false)),
          record2T1.inj(HRecord(_1 = true, _2 = false)),
          record2T1.inj(HRecord(_1 = false, _2 = true)),
          record2T2.inj(HRecord(_1 = false, _2 = false)),
        )

      val variantT1 = VA.variant("Either", 'Left ->> VA.bool :: 'Right ->> VA.bool :: RNil)._2
      val variantT2 = VA.variant("EitherBis", 'Left ->> VA.bool :: 'Right ->> VA.bool :: RNil)._2

      val variants = List(
        variantT1.inj(HSum[variantT1.Inj[Nothing]]('Left ->> false)),
        variantT1.inj(HSum[variantT1.Inj[Nothing]]('Left ->> true)),
        variantT1.inj(HSum[variantT1.Inj[Nothing]]('Right ->> false)),
        variantT2.inj(HSum[variantT1.Inj[Nothing]]('Left ->> false)),
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

      val textMaps = List[V](
        textMap(),
        textMap("a" -> false),
        textMap("a" -> false),
        textMap("b" -> false),
        textMap("a" -> false, "b" -> false),
        textMap("a" -> true, "b" -> false),
        textMap("a" -> false, "b" -> true),
        textMap("a" -> false, "c" -> false),
      )

      val optionals =
        List(None, Some(false), Some(true)).map(VA.optional(VA.bool).inj) ++
          List(Some(None), Some(Some(false))).map(VA.optional(VA.optional(VA.bool)).inj)

      val testCases: List[V] =
        units ++ bools ++ ints ++ decimals ++ numeric0s ++ dates ++ timestamps ++ texts ++ parties ++ contractIds ++ optionals ++ lists ++ textMaps ++ enums ++ records0 ++ records2 ++ variants

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
          |ValueContractId(AbsoluteContractId(07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | 399c8d4fb942204c9384a8bda062676d75a3a52080c798f98560b2914af61ad8
          |ValueContractId(AbsoluteContractId(59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | f05d36c187b003a1c1ca669579bdcf0cfce85ce634029cc533d23d24fd8382a1
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
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | 4c4384399821a8ed7526d8b29dc6f76ad87014ade285386e7d05d71e61d86c7c
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
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Red)
          | 048b20422b487b8eeba059a219589ad477e5f11eb769c7fea658b63f1bb1d405
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Green)
          | ff89416f14a9369d7ef3f9a23057878320aa7b777c7233a79f2b0cab812a3e7a
          |ValueEnum(Some(Identifier(pkgId,Mod:ColorBis)),Green)
          | ff89416f14a9369d7ef3f9a23057878320aa7b777c7233a79f2b0cab812a3e7a
          |ValueRecord(Some(Identifier(pkgId,Mod:Unit)),ImmArray())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueRecord(Some(Identifier(pkgId,Mod:UnitBis)),ImmArray())
          | 01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 8f5dff2ff3f971b847284fb225522005587449fad2746879a0280bbd036f1abc
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 768c5b90ed7ae5b727381e331fac83d7defd397d040f46ba067c80ec2af3eb33
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 4f6de867c24682cee05db95d48e1ea47cf5f8b6e74fe07582d3cd8cecaea84b7
          |ValueRecord(Some(Identifier(pkgId,Mod:TupleBis)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 8f5dff2ff3f971b847284fb225522005587449fad2746879a0280bbd036f1abc
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(false))
          | 41edeaec86ac919e3c184057b021753781bd2ac1d60b8d4329375f60df953097
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(true))
          | 31d69356947365e8a3dd9706774182e86774af1aa6550055efc56a22bb594745
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Right,ValueBool(false))
          | bd89c47c2379a69e8e0d46ff634c533449e8e7e532e84def4e2b2e168bc786e7
          |ValueVariant(Some(Identifier(pkgId,Mod:EitherBis)),Left,ValueBool(false))
          | 41edeaec86ac919e3c184057b021753781bd2ac1d60b8d4329375f60df953097
          |""".stripMargin

      val sep = System.getProperty("line.separator")
      val actualOutput = testCases
        .map { value =>
          val hash = Hash
            .builder(Hash.Purpose.Testing)
            .addTypedValue(value)
            .build
            .toLedgerString
          s"${value.toString}$sep $hash"
        }
        .mkString("", sep, sep)
      actualOutput shouldBe expectedOut

    }
  }

  "Hash.fromString" should {
    "convert properly string" in {
      val s = "01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086"
      Hash.assertFromString(s).toLedgerString shouldBe s
    }
  }

  private def defRef(module: String = "Module", name: String) =
    Ref.Identifier(
      packageId0,
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(module),
        Ref.DottedName.assertFromString(name),
      ),
    )

  private implicit def addVersion[Cid](v: Value[Cid]): VersionedValue[Cid] =
    VersionedValue(ValueVersion("4"), v)

}
