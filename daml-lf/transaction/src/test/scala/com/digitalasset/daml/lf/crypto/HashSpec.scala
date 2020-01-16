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
              'Variant ->> VA.int64 :: RNil
            )
            ._2
          :: 'fRecord ->>
          VA.record(
              defRef(name = "Record"),
              'field1 ->> VA.text :: 'field2 ->> VA.text :: RNil
            )
            ._2
          :: 'fTextMap ->> VA.map(VA.text)
          :: RNil
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
      fTextMap = SortedLookupList(Map("keyA" -> "valueA", "keyB" -> "valueB"))
    )

  "KeyHasher" should {

    "be stable" in {
      val hash = "a0a47f146a6ba1aca8865d9cc3741b766595a7810c25132fd87d92e433fa37bc"
      val value = complexRecordT.inj(complexRecordV)
      val name = defRef("module", "name")
      Hash(GlobalKey(name, value)).toHexa shouldBe hash
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector
        .fill(1000)(GlobalKey(defRef("module", "name"), complexRecordT.inj(complexRecordV)))
        .par
        .map(Hash(_))

      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = VA.text.inj("A")

      val hash1 = Hash(GlobalKey(defRef("AA", "A"), value))
      val hash2 = Hash(GlobalKey(defRef("A", "AA"), value))

      hash1 should !==(hash2)
    }

    "not produce collision in list of text" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      def list(elements: String*) = VA.list(VA.text).inj(elements.toVector)
      val value1 = list("AA", "A")
      val value2 = list("A", "AA")

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in list of decimals" in {
      // Testing whether decimals are delimited: [10, 10] vs [101, 0]
      def list(elements: String*) =
        VA.list(VA.numeric(Decimal.scale)).inj(elements.map(Numeric.assertFromString).toVector)
      val value1 = list("10.0000000000", "10.0000000000")
      val value2 = list("101.0000000000", "0.0000000000")

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in list of lists" in {
      // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
      def list(elements: Vector[Unit]*) = VA.list(VA.list(VA.unit)).inj(elements.toVector)
      val value1 = list(Vector(()), Vector((), ()))
      val value2 = list(Vector((), ()), Vector(()))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Variant constructor" in {
      val variantT =
        VA.variant(defRef(name = "Variant"), 'A ->> VA.unit :: 'B ->> VA.unit :: RNil)._2
      val value1 = variantT.inj(HSum[variantT.Inj[Nothing]]('A ->> (())))
      val value2 = variantT.inj(HSum[variantT.Inj[Nothing]]('B ->> (())))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Variant value" in {
      val variantT = VA.variant(defRef(name = "Variant"), 'A ->> VA.int64 :: RNil)._2
      val value1 = variantT.inj(HSum('A ->> 0L))
      val value2 = variantT.inj(HSum('A ->> 1L))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Map keys" in {
      def textMap(elements: (String, Long)*) =
        VA.map(VA.int64).inj(SortedLookupList(elements.toMap))
      val value1 = textMap("A" -> 0, "B" -> 0)
      val value2 = textMap("A" -> 0, "C" -> 0)

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Map values" in {
      def textMap(elements: (String, Long)*) =
        VA.map(VA.int64).inj(SortedLookupList(elements.toMap))
      val value1 = textMap("A" -> 0, "B" -> 0)
      val value2 = textMap("A" -> 0, "B" -> 1)

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Bool" in {
      val value1 = ValueTrue
      val value2 = ValueFalse

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Int64" in {
      val value1 = ValueInt64(0L)
      val value2 = ValueInt64(1L)

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Decimal" in {
      val value1 = ValueNumeric(Numeric.assertFromString("0."))
      val value2 = ValueNumeric(Numeric.assertFromString("1."))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Date" in {
      val value1 = ValueDate(Time.Date.assertFromDaysSinceEpoch(0))
      val value2 = ValueDate(Time.Date.assertFromDaysSinceEpoch(1))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Timestamp" in {
      val value1 = ValueTimestamp(Time.Timestamp.assertFromLong(0))
      val value2 = ValueTimestamp(Time.Timestamp.assertFromLong(1))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Optional" in {
      val value1 = ValueNone
      val value2 = ValueOptional(Some(ValueUnit))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1 should !==(hash2)
    }

    "not produce collision in Record" in {
      val recordT =
        VA.record(defRef(name = "Tuple2"), '_1 ->> VA.text :: '_2 ->> VA.text :: RNil)._2
      val value1 = recordT.inj(HRecord(_1 = "A", _2 = "B"))
      val value2 = recordT.inj(HRecord(_1 = "A", _2 = "C"))

      val tid = defRef("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

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
          "59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
        ).map(x => AbsoluteContractId(Ref.ContractIdString.assertFromString(x)))
          .map(VA.contractId.inj)

      val enumT1 = VA.enum("Color", List("Red", "Green"))._2
      val enumT2 = VA.enum("ColorBis", List("Red", "Green"))._2

      val enums = List(
        enumT1.inj(enumT1.get("Red").get),
        enumT1.inj(enumT1.get("Green").get),
        enumT2.inj(enumT2.get("Green").get)
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
        list(true, false)
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
          | 47dc540c94ceb704a23875c11273e16bb0b8a87aed84de911f2133568115f254
          |ValueBool(true)
          | 9dcf97a184f32623d11a73124ceb99a5709b083721e878a16d78f596718ba7b2
          |ValueBool(false)
          | 47dc540c94ceb704a23875c11273e16bb0b8a87aed84de911f2133568115f254
          |ValueInt64(-1)
          | 1272f520cf7ca5cf117a0b5a3116518371bf20fb7fac043ac1be568b8c55b96c
          |ValueInt64(0)
          | a536aa3cede6ea3c1f3e0357c3c60e0f216a8c89b853df13b29daa8f85065dfb
          |ValueInt64(1)
          | f52f3a746c2545658e1c6add32e5410365553ebaaa0433f5f8bd90c6f85fd6e2
          |ValueNumeric(-10000.0000000000)
          | 06688bdccaa8613e22b96e399b601884a4c7476b97647a1adcb173ab2c94ed4c
          |ValueNumeric(0E-10)
          | a90d4563c6a0ae0417ab3110f1ba68592833465954774201b0a69e8c457dc6ad
          |ValueNumeric(10000.0000000000)
          | f704cf5962e1f9e6568d0c9bc08f9caa2b7524ac901f311911c8cb70d8a01608
          |ValueNumeric(-10000)
          | af020447b2978bcc3114cf0d8c832f028c75ab7713a3985d44715e37c668807e
          |ValueNumeric(0)
          | a90d4563c6a0ae0417ab3110f1ba68592833465954774201b0a69e8c457dc6ad
          |ValueNumeric(10000)
          | 28b2998d102b9dbc2f39cbb1781f930e80f29fa585a3920c920028f92d1e6a6c
          |ValueDate(1970-01-01)
          | 957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027
          |ValueDate(1969-07-21)
          | aeb4c265245cd7aa582d4e9953e108b182516f279a60919795130842fab5912d
          |ValueDate(2019-12-16)
          | 247de9bec9f7a30173b946038ba22b7ea4f1d9c03aa2eb278c9f1c8de110e084
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | a536aa3cede6ea3c1f3e0357c3c60e0f216a8c89b853df13b29daa8f85065dfb
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | fcaf4b00d1f94c7126299dd9fe753ec03ca5823669a79e5cef0b706bedc98d74
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | 79e7e25c280740fbe3ba3a9c083f5cecc5dae5ad6f4dd77858113867d504b78e
          |ValueText()
          | 957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027
          |ValueText(someText)
          | 71209c3a5e05a8bb796881eaec6a498d96995ea4d212b9bf6dec8f4ab069f0ed
          |ValueText(a¶‱😂)
          | a9eb3431bd82a173e23fc0897642b30ffffb35b2d9bb0d47119a124031137d02
          |ValueParty(alice)
          | 151a185ae945526e7ba8dd89864b4f157eef8cec96ca7f00524186723e54d348
          |ValueParty(bob)
          | 1bc9e1ce9982ee796fac0e2542aff3f6863ecf5e64b459054b2e8928bb432c67
          |ValueContractId(AbsoluteContractId(07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | afa1aa91da7ea6dbd74a719c98d169addf4106ce0916430656f8a2bba73ea280
          |ValueContractId(AbsoluteContractId(59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | d0a26432252903db999a354a5ed6b2fbd5010e53637afcf804cdb4e4c37fc315
          |ValueOptional(None)
          | 957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027
          |ValueOptional(Some(ValueBool(false)))
          | a90d4563c6a0ae0417ab3110f1ba68592833465954774201b0a69e8c457dc6ad
          |ValueOptional(Some(ValueBool(true)))
          | 71ca9703af0fda42b802aa93ef5ff20cc9d02353e1b2d514acae2ec02f2c7278
          |ValueOptional(Some(ValueOptional(None)))
          | c1a55026080627649a9e5f2226e3ce91f2c1b7959d429a312a0c96339108b6c9
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | 649cc24b99273730cbfc3ad5726781e62593837e78a3d6b0b615f320c488abd6
          |ValueList(FrontStack())
          | 957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027
          |ValueList(FrontStack(ValueBool(false)))
          | a90d4563c6a0ae0417ab3110f1ba68592833465954774201b0a69e8c457dc6ad
          |ValueList(FrontStack(ValueBool(true)))
          | 71ca9703af0fda42b802aa93ef5ff20cc9d02353e1b2d514acae2ec02f2c7278
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | 1d3e4d40ae7f934c7b67fb747ae99f9af5bfc867f52966e7c3cf6b7975dd237a
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | f9ce3cd0946bd8a778fa90b81a439f80a999e263fac5877fe7316170298ae139
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | b238496934cc67758abae39e09934168f2da4f4db828cf25dfce22e276e58b9b
          |ValueTextMap(SortedLookupList())
          | 957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | e8dc56307e050c735828adff3914e46cc2ca3e6508e15f0e3a9f7f56998aa3e8
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | e8dc56307e050c735828adff3914e46cc2ca3e6508e15f0e3a9f7f56998aa3e8
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | d1385328e6b074f7bef63c877357b09ab11de0035747c344fe60ed0f733caf8f
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | 11f7643a5d2c1abdd28cb912a32f94cc9b145e9ea0932a423b0afd5d6da36dbb
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | b3cfd3d5a67480d1a0819d0ef66a9826c7a91b8d0125cbc82e0dca1b4b68a42e
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | b91b20a3daf58619e432c77b6dbca0c24d105092dfc4e420c72ede6e1cef5c23
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | 8df4200e13434cab9917c19c2d0b764e2259fdeaaf29e9817989af83eb22e583
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Red)
          | e913123a57c91b08dde781877cba20bcedd7e67dbb4bfb9e1b2fad2ac9201f89
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Green)
          | 6db1b39e291c0f9af7290149adc84cb0d04fff38c40056bb8bf1caa08664315d
          |ValueEnum(Some(Identifier(pkgId,Mod:ColorBis)),Green)
          | 6db1b39e291c0f9af7290149adc84cb0d04fff38c40056bb8bf1caa08664315d
          |ValueRecord(Some(Identifier(pkgId,Mod:Unit)),ImmArray())
          | 957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027
          |ValueRecord(Some(Identifier(pkgId,Mod:UnitBis)),ImmArray())
          | 957b88b12730e646e0f33d3618b77dfa579e8231e3c59c7104be7165611c8027
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 1d3e4d40ae7f934c7b67fb747ae99f9af5bfc867f52966e7c3cf6b7975dd237a
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | b238496934cc67758abae39e09934168f2da4f4db828cf25dfce22e276e58b9b
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | f9ce3cd0946bd8a778fa90b81a439f80a999e263fac5877fe7316170298ae139
          |ValueRecord(Some(Identifier(pkgId,Mod:TupleBis)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 1d3e4d40ae7f934c7b67fb747ae99f9af5bfc867f52966e7c3cf6b7975dd237a
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(false))
          | f94f4b19d80fd21a583399cc5d788421e624e4e433f813c236851417489d8c5a
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(true))
          | a80f8894f18978ee0d7e7c20b9b6adac521fb2274304fb2dfba9b9c8e2927ac7
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Right,ValueBool(false))
          | fbda4dff63596daae1be44be3eec19d25547a883a62ca4c1cca713208966983f
          |ValueVariant(Some(Identifier(pkgId,Mod:EitherBis)),Left,ValueBool(false))
          | f94f4b19d80fd21a583399cc5d788421e624e4e433f813c236851417489d8c5a
          |""".stripMargin

      val sep = System.getProperty("line.separator")
      val actualOutput = testCases
        .map { value =>
          val hash = Hash
            .builder(Hash.Purpose.Testing)
            .addTypedValue(value)
            .build
            .toByteArray
            .map("%02x" format _)
            .mkString
          s"${value.toString}$sep $hash"
        }
        .mkString("", sep, sep)
      actualOutput shouldBe expectedOut

    }
  }

  private def defRef(module: String = "Module", name: String) =
    Ref.Identifier(
      packageId0,
      Ref.QualifiedName(
        Ref.DottedName.assertFromString(module),
        Ref.DottedName.assertFromString(name))
    )

  private implicit def addVersion[Cid](v: Value[Cid]): VersionedValue[Cid] =
    VersionedValue(ValueVersion("4"), v)

}
