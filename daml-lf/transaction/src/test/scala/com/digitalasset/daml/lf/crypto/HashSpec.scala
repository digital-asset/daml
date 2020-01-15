// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package crypto

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.TypedValueGenerators.{ValueAddend => VA}
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.{Value, ValueVersion}
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

class HashSpec extends WordSpec with Matchers {

  private[this] def templateId(module: String, name: String) = Ref.Identifier(
    Ref.PackageId.assertFromString("package"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString(module),
      Ref.DottedName.assertFromString(name)
    )
  )

  private[this] def complexValue = {
    val fields = ImmArray(
      None -> ValueInt64(0),
      None -> ValueInt64(123456),
      None -> ValueInt64(-1),
      None -> ValueNumeric(0),
      None -> ValueNumeric(BigDecimal("0.3333333333")),
      None -> ValueTrue,
      None -> ValueFalse,
      None -> ValueDate(Time.Date.assertFromDaysSinceEpoch(0)),
      None -> ValueDate(Time.Date.assertFromDaysSinceEpoch(123456)),
      None -> ValueTimestamp(Time.Timestamp.assertFromLong(0)),
      None -> ValueTimestamp(Time.Timestamp.assertFromLong(123456)),
      None -> ValueText(""),
      None -> ValueText("abcd-äöü€"),
      None -> ValueParty(Ref.Party.assertFromString("Alice")),
      None -> ValueUnit,
      None -> ValueNone,
      None -> ValueOptional(Some(ValueText("Some"))),
      None -> VA.list(VA.text).inj(Vector("A", "B", "C")),
      None -> ValueVariant(None, Ref.Name.assertFromString("Variant"), ValueInt64(0)),
      None ->
        ValueRecord(
          None,
          ImmArray(
            None -> ValueText("field1"),
            None -> ValueText("field2")
          )),
      None -> VA.map(VA.text).inj(SortedLookupList(Map("keyA" -> "valueA", "keyB" -> "valueB")))
    )

    ValueRecord(None, fields)
  }

  "KeyHasher" should {

    "be stable" in {
      // Hashing function must not change
      val value = VersionedValue(ValueVersion("4"), complexValue)
      val hash = "bc25286de6c5f7745e65354a8ddd18d12a339d069248fea48d14a338fadb4f22"

      Hash(GlobalKey(templateId("module", "name"), value)).toHexa shouldBe hash
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector
        .range(0, 1000)
        .map(_ =>
          GlobalKey(templateId("module", "name"), VersionedValue(ValueVersion("4"), complexValue)))
        .par
        .map(key => Hash(key))

      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = VersionedValue(ValueVersion("4"), ValueText("A"))

      val hash1 = Hash(GlobalKey(templateId("AA", "A"), value))
      val hash2 = Hash(GlobalKey(templateId("A", "AA"), value))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of text" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      val value1 =
        VersionedValue(ValueVersion("4"), VA.list(VA.text).inj(Vector("AA", "A")))
      val value2 =
        VersionedValue(ValueVersion("4"), VA.list(VA.text).inj(Vector("A", "AA")))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of decimals" in {
      // Testing whether decimals are delimited: [10, 10] vs [101, 0]
      val value1 =
        VersionedValue(
          ValueVersion("4"),
          VA.list(VA.numeric(Decimal.scale)).inj(Vector[Numeric](10, 10)))
      val value2 =
        VersionedValue(
          ValueVersion("4"),
          VA.list(VA.numeric(Decimal.scale)).inj(Vector[Numeric](101, 0)))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of lists" in {
      // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
      val value1 = VersionedValue(
        ValueVersion("4"),
        VA.list(VA.list(VA.unit)).inj(Vector(Vector(()), Vector((), ()))))
      val value2 = VersionedValue(
        ValueVersion("4"),
        VA.list(VA.list(VA.unit)).inj(Vector(Vector((), ()), Vector(()))))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Variant constructor" in {
      val value1 =
        VersionedValue(
          ValueVersion("4"),
          ValueVariant(None, Ref.Name.assertFromString("A"), ValueUnit))
      val value2 =
        VersionedValue(
          ValueVersion("4"),
          ValueVariant(None, Ref.Name.assertFromString("B"), ValueUnit))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Variant value" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        ValueVariant(None, Ref.Name.assertFromString("A"), ValueInt64(0L)))
      val value2 = VersionedValue(
        ValueVersion("4"),
        ValueVariant(None, Ref.Name.assertFromString("A"), ValueInt64(1L)))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map keys" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        VA.map(VA.int64).inj(SortedLookupList(Map("A" -> 0, "B" -> 0))))
      val value2 = VersionedValue(
        ValueVersion("4"),
        VA.map(VA.int64).inj(SortedLookupList(Map("A" -> 0, "C" -> 0))))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map values" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        VA.map(VA.int64).inj(SortedLookupList(Map("A" -> 0, "B" -> 0))))
      val value2 = VersionedValue(
        ValueVersion("4"),
        VA.map(VA.int64).inj(SortedLookupList(Map("A" -> 0, "B" -> 1))))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Bool" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueTrue)
      val value2 = VersionedValue(ValueVersion("4"), ValueFalse)

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Int64" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueInt64(0L))
      val value2 = VersionedValue(ValueVersion("4"), ValueInt64(1L))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Decimal" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueNumeric(0))
      val value2 = VersionedValue(ValueVersion("4"), ValueNumeric(1))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Date" in {
      val value1 =
        VersionedValue(ValueVersion("4"), ValueDate(Time.Date.assertFromDaysSinceEpoch(0)))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueDate(Time.Date.assertFromDaysSinceEpoch(1)))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Timestamp" in {
      val value1 =
        VersionedValue(ValueVersion("4"), ValueTimestamp(Time.Timestamp.assertFromLong(0)))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueTimestamp(Time.Timestamp.assertFromLong(1)))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Optional" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueNone)
      val value2 = VersionedValue(ValueVersion("4"), ValueOptional(Some(ValueUnit)))

      val tid = templateId("module", "name")

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

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

      val hash1 = Hash(GlobalKey(tid, value1))
      val hash2 = Hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
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

      implicit def toSortedLookupList[V](a: ImmArray[(String, V)]): SortedLookupList[V] =
        SortedLookupList.fromSortedImmArray(a).right.get

      val EnumTypeCon: Ref.TypeConName = "Color"
      val EnumTypeConBis: Ref.TypeConName = "ColorBis"

      val EnumCon1: Ref.Name = "Red"
      val EnumCon2: Ref.Name = "Green"

      val Record0TypeCon: Ref.TypeConName = "Unit"
      val Record2TypeCon: Ref.TypeConName = "Tuple"
      val Record0TypeConBis: Ref.TypeConName = "UnitBis"
      val Record2TypeConBis: Ref.TypeConName = "TupleBis"
      val fstField = Ref.Name.assertFromString("_1")
      val sndField = Ref.Name.assertFromString("_2")

      val VariantTypeCon: Ref.TypeConName = "Either"
      val VariantTypeConBis: Ref.TypeConName = "EitherBis"
      val VariantCon1: Ref.Name = "Left"
      val VariantCon2: Ref.Name = "Right"

      val units =
        List[V](
          ValueUnit
        )
      val bools =
        List[V](ValueTrue, ValueFalse)
      val ints =
        List[V](ValueInt64(-1L), ValueInt64(0L), ValueInt64(1L))
      val decimals =
        List[V](
          ValueNumeric(Numeric.assertFromString("-10000.0000000000")),
          ValueNumeric(Numeric.assertFromString("0.0000000000")),
          ValueNumeric(Numeric.assertFromString("10000.0000000000")),
        )
      val numeric0s =
        List[V](
          ValueNumeric(Numeric.assertFromString("-10000.")),
          ValueNumeric(Numeric.assertFromString("0.")),
          ValueNumeric(Numeric.assertFromString("10000.")),
        )

      val texts =
        List[V](
          ValueText(""),
          ValueText("someText"),
          ValueText("a¶‱😂"),
        )
      val dates =
        List[V](
          ValueDate(Time.Date.assertFromDaysSinceEpoch(0)),
          ValueDate(Time.Date.assertFromString("1969-07-21")),
          ValueDate(Time.Date.assertFromString("2019-12-16")),
        )
      val timestamps =
        List[V](
          ValueTimestamp(Time.Timestamp.assertFromLong(0)),
          ValueTimestamp(Time.Timestamp.assertFromString("1969-07-21T02:56:15.000000Z")),
          ValueTimestamp(Time.Timestamp.assertFromString("2019-12-16T11:17:54.940779363Z")),
        )
      val parties =
        List[V](
          ValueParty(Ref.Party.assertFromString("alice")),
          ValueParty(Ref.Party.assertFromString("bob")),
        )
      val contractIds =
        List[V](
          ValueContractId(
            AbsoluteContractId(Ref.ContractIdString.assertFromString(
              "07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"))),
          ValueContractId(
            AbsoluteContractId(Ref.ContractIdString.assertFromString(
              "59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b")))
        )

      val enums =
        List[V](
          ValueEnum(Some(EnumTypeCon), EnumCon1),
          ValueEnum(Some(EnumTypeCon), EnumCon2),
          ValueEnum(Some(EnumTypeConBis), EnumCon2),
        )

      val records0 =
        List[V](
          ValueRecord(Some(Record0TypeCon), ImmArray.empty),
          ValueRecord(Some(Record0TypeConBis), ImmArray.empty),
        )

      val records2 =
        List[V](
          ValueRecord(
            Some(Record2TypeCon),
            ImmArray(Some(fstField) -> ValueFalse, Some(sndField) -> ValueFalse)),
          ValueRecord(
            Some(Record2TypeCon),
            ImmArray(Some(fstField) -> ValueTrue, Some(sndField) -> ValueFalse)),
          ValueRecord(
            Some(Record2TypeCon),
            ImmArray(Some(fstField) -> ValueFalse, Some(sndField) -> ValueTrue)),
          ValueRecord(
            Some(Record2TypeConBis),
            ImmArray(Some(fstField) -> ValueFalse, Some(sndField) -> ValueFalse)),
        )

      val variants = List[V](
        ValueVariant(Some(VariantTypeCon), VariantCon1, ValueFalse),
        ValueVariant(Some(VariantTypeCon), VariantCon1, ValueTrue),
        ValueVariant(Some(VariantTypeCon), VariantCon2, ValueFalse),
        ValueVariant(Some(VariantTypeConBis), VariantCon1, ValueFalse),
      )

      val lists = List[V](
        ValueList(FrontStack.empty),
        ValueList(FrontStack(ValueFalse)),
        ValueList(FrontStack(ValueTrue)),
        ValueList(FrontStack(ValueFalse, ValueFalse)),
        ValueList(FrontStack(ValueFalse, ValueTrue)),
        ValueList(FrontStack(ValueTrue, ValueFalse)),
      )

      val textMaps = List[V](
        ValueTextMap(SortedLookupList.empty),
        ValueTextMap(ImmArray("a" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueFalse)),
        ValueTextMap(ImmArray("b" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueFalse, "b" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueTrue, "b" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueFalse, "b" -> ValueTrue)),
        ValueTextMap(ImmArray("a" -> ValueFalse, "c" -> ValueFalse)),
      )

      val optionals = List[V](
        ValueOptional(None),
        ValueOptional(Some(ValueFalse)),
        ValueOptional(Some(ValueTrue)),
        ValueOptional(Some(ValueOptional(None))),
        ValueOptional(Some(ValueOptional(Some(ValueFalse)))),
      )

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
          | cb29539512e7105f9b02fa5d2dd2e30d014adecdf642eaf41e3eb15504e4a416
          |ValueNumeric(0E-10)
          | b53a15f0a7751f1a0231aded6f6727dbf740bb18fef23991043b8c8541bde644
          |ValueNumeric(10000.0000000000)
          | 43d41d5da60e52c692202d36221058c1910d803fd18c5e52b305a774f051fec1
          |ValueNumeric(-10000)
          | 809c49663762af809a784b2585179a6868dcd6e93a1810957cdd0be87d553575
          |ValueNumeric(0)
          | 36c159d76b52e03e496363607c5940227f62f4e048df344b050159bc97bba3d7
          |ValueNumeric(10000)
          | 712289c5328ad24f612ad6351bc47269a5fcbf8d363ba0a1c0fdf687bb6ad774
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

  private implicit def int2decimal(x: Int): Numeric =
    BigDecimal(x)

  private implicit def BigDecimal2decimal(x: BigDecimal): Numeric =
    Numeric.assertFromBigDecimal(Decimal.scale, x)

}
