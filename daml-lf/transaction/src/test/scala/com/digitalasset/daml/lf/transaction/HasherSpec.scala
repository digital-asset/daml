// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.{Value, ValueVersion}
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

class HasherSpec extends WordSpec with Matchers {

  private[this] def templateId(module: String, name: String) = Ref.Identifier(
    Ref.PackageId.assertFromString("package"),
    Ref.QualifiedName(
      Ref.ModuleName.assertFromString(module),
      Ref.DottedName.assertFromString(name)
    )
  )

  private[this] def complexValue = {
    val builder = ImmArray.newBuilder[(Option[Ref.Name], Value[AbsoluteContractId])]
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
    builder += None -> ValueParty(Ref.Party.assertFromString("Alice"))
    builder += None -> ValueUnit
    builder += None -> ValueNone
    builder += None -> ValueOptional(Some(ValueText("Some")))
    builder += None -> ValueList(FrontStack(ValueText("A"), ValueText("B"), ValueText("C")))
    builder += None -> ValueVariant(None, Ref.Name.assertFromString("Variant"), ValueInt64(0))
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

  "KeyHasher" should {

    "be stable" in {
      // Hashing function must not change
      val value = VersionedValue(ValueVersion("4"), complexValue)
      val hash = "db77982742e7612b0a62e858002cb9a9a895a718d2741defad3e369da271fb74"

      Hasher.hash(GlobalKey(templateId("module", "name"), value)).toHexa shouldBe hash
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector
        .range(0, 1000)
        .map(_ =>
          GlobalKey(templateId("module", "name"), VersionedValue(ValueVersion("4"), complexValue)))
        .par
        .map(key => Hasher.hash(key))

      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = VersionedValue(ValueVersion("4"), ValueText("A"))

      val hash1 = Hasher.hash(GlobalKey(templateId("AA", "A"), value))
      val hash2 = Hasher.hash(GlobalKey(templateId("A", "AA"), value))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of text" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      val value1 =
        VersionedValue(ValueVersion("4"), ValueList(FrontStack(ValueText("AA"), ValueText("A"))))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueList(FrontStack(ValueText("A"), ValueText("AA"))))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of decimals" in {
      // Testing whether decimals are delimited: [10, 10] vs [101, 0]
      val value1 =
        VersionedValue(
          ValueVersion("4"),
          ValueList(FrontStack(ValueNumeric(decimal(10)), ValueNumeric(decimal(10)))))
      val value2 =
        VersionedValue(
          ValueVersion("4"),
          ValueList(FrontStack(ValueNumeric(decimal(101)), ValueNumeric(decimal(0)))))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

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

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

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

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

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

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map keys" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(0)
            ))))
      val value2 = VersionedValue(
        ValueVersion("4"),
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "C" -> ValueInt64(0)
            ))))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map values" in {
      val value1 = VersionedValue(
        ValueVersion("4"),
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(0)
            ))))
      val value2 = VersionedValue(
        ValueVersion("4"),
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(1)
            ))))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Bool" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueTrue)
      val value2 = VersionedValue(ValueVersion("4"), ValueFalse)

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Int64" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueInt64(0L))
      val value2 = VersionedValue(ValueVersion("4"), ValueInt64(1L))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Decimal" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueNumeric(decimal(0)))
      val value2 = VersionedValue(ValueVersion("4"), ValueNumeric(decimal(1)))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Date" in {
      val value1 =
        VersionedValue(ValueVersion("4"), ValueDate(Time.Date.assertFromDaysSinceEpoch(0)))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueDate(Time.Date.assertFromDaysSinceEpoch(1)))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Timestamp" in {
      val value1 =
        VersionedValue(ValueVersion("4"), ValueTimestamp(Time.Timestamp.assertFromLong(0)))
      val value2 =
        VersionedValue(ValueVersion("4"), ValueTimestamp(Time.Timestamp.assertFromLong(1)))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Optional" in {
      val value1 = VersionedValue(ValueVersion("4"), ValueNone)
      val value2 = VersionedValue(ValueVersion("4"), ValueOptional(Some(ValueUnit)))

      val tid = templateId("module", "name")

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

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

      val hash1 = Hasher.hash(GlobalKey(tid, value1))
      val hash2 = Hasher.hash(GlobalKey(tid, value2))

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
          | 15f2f1a4339f5f2a313b95015cad8124d054a171ac2f31cf529dda7cfb6a38b4
          |ValueBool(true)
          | 89eefc18fa4b815bd1aded2f24eb28885993aa00b6d0171bf5005f9d39aaea10
          |ValueBool(false)
          | 016a682d1df4f869b32c48b0a9b442a1493949fb85d951d121c1143bd3d5c1af
          |ValueInt64(-1)
          | c6c7c4bc40424a55aa5e3bd95c327747c9af51c2e8cb82d4bd0992e2197e6b0f
          |ValueInt64(0)
          | bcd951ff7bd950aef43c9ef95db7418f4773a42ab05a78b0916d2d255e72ee8e
          |ValueInt64(1)
          | eb07f73c38490f4cb04a1a6def3ab2dcbfb28272f89a81dcc590fe3dc6f9c273
          |ValueNumeric(-10000.0000000000)
          | c4f7690ce2158103fadaac9213c165ec39c0e078f1ce6a1b1bb87968b9002e0f
          |ValueNumeric(0E-10)
          | e5f25e3d3d8f71e96903f66006d46fd0823e0ff3fea0325e9db191b09b6245d0
          |ValueNumeric(10000.0000000000)
          | a8f14244b403fd3cf9db68033930cb257b6807618c94284ffba520d903459d9f
          |ValueNumeric(-10000)
          | 33594221d9ad5035b57b781fc9d8a63bdcd17a76515f61346f3573268cf4e9c4
          |ValueNumeric(0)
          | 0c6582c1ff7eb3e7b85fd306a55245badff8c41810b4fdc666c3423657865e0f
          |ValueNumeric(10000)
          | 21959671a9043323d76128695817fe446139886d1e1749105ecf2de7a486c04b
          |ValueDate(1970-01-01)
          | 9503e533b097397f5118a748890cd09caacfd4c95c4d13f1fd106a5b5e39e78a
          |ValueDate(1969-07-21)
          | be2184bbbf374335bbbe5cf898a987d017b1bca16b3ce989add0e87b5f3c0a92
          |ValueDate(2019-12-16)
          | ea6c2fccb5581d105bd3d9c2fb74e70157656ba918718b172a99bacd696078cc
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | 8c8b25d714d32f493507b27547e65c10712e8179832b71b5fc442c99f369a637
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | ed5bbec42069ea10870ce5829d58ec5ca76cccf5d75e9a0812eee07140302c18
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | c15b65da18a78c5e83a31cb81a0810d8a0eff3858ff0955122c5197b1f2b6930
          |ValueText()
          | 440025f7b4bce5faa563ac26871ded6790667f1f4955d9b5b7c676d30931dcb6
          |ValueText(someText)
          | 53d06d97bbbb95b677bc56e5f998cb3228f1513cee7e3a8f442af325241d02a9
          |ValueText(a¶‱😂)
          | 3929c32b3e48200e4a5e30d91a0754e0a0750672d2cdf2a1a375d0f058813ead
          |ValueParty(alice)
          | 5e3e4778c3fdf7adc1a1aa6d443475a6fd2e6a4a842b415f0b8b8be4c9720869
          |ValueParty(bob)
          | d145e694eea4408cba91f7c1136e70841e695d57e19178d131d996e37fea33c7
          |ValueContractId(AbsoluteContractId(07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | 27c3a4065fa0b0cb036069a3c691ae14fc695a4cfccbfe50df6153949508bd36
          |ValueContractId(AbsoluteContractId(59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 7baf071daae9fd38312390cd5885dc090eef3644c58d76c5362a9bd670f0cdce
          |ValueOptional(None)
          | aa6d7aa607cf60ddeb2c5f584d9f206224590ffcc28f6d0640749d32a0fc42a4
          |ValueOptional(Some(ValueBool(false)))
          | b8a910ccd6c1c90d01678566500732cf71b7ec9354820b30e8407e4beb05cb2f
          |ValueOptional(Some(ValueBool(true)))
          | f052f3f83fd029715b04bb7cc4b7092a47a5aacdd9ac2b8b24549cc25833c67b
          |ValueOptional(Some(ValueOptional(None)))
          | c817fde8b9263a53e4e2d486b415799c58b7a37a7d91e2dd5aef70b8bb42792d
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | e440c6091805f4ebea2b52f6cf3dadd52b99213db218dcbe0a6b7140f81867ad
          |ValueList(FrontStack())
          | 0d8ffb061b89a6e3059a2d4471a3975299a4200a782a016b31fe1c280353a4db
          |ValueList(FrontStack(ValueBool(false)))
          | d6164011e601839e9410a5a91ddb380b0c5e79f9ca033e1c690b4f30ad6fe249
          |ValueList(FrontStack(ValueBool(true)))
          | 7e08f8579689515cf861204126ddd7e4c1ad359595f2a4657e0c4071c301a80f
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | cece4925bef52d965f339c55aa7646211cdc2f52b2e8b4944a2669cd232cc592
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | 4591fc459c81d2fb04c414a070b114959a00fcb745e3794f90e75ea371686abc
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | 336591f8c3110e33a1419b94de5115c19355e59bd622b5e223f26bf4f79987d9
          |ValueTextMap(SortedLookupList())
          | 9e8dd7513fae9788834f682bd4fab17fb4747fa2e8c5e50eafc6c7dd00f6df28
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | 65e1ef074f5aff120f96166e69dd3d18eadc8beb3558e8959930c86b23bac3b1
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | 65e1ef074f5aff120f96166e69dd3d18eadc8beb3558e8959930c86b23bac3b1
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | b9fde31caf5bf186fbc34247827a96d4989564ac3abcab98bf0afcaf7cea26a8
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | 22e1410c70c183292775054a25a4aaddf0d618f73bedc7a772ef146a5d3c805a
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | fd549d867fa394a7095e8c58cfb9557d4f2d48a0f2a369c86bb18ee1dd4ad91b
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | da2e5c18774df1a5ff965a78dc4685c7793af98626994bd5744d0d951a795270
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | 76868f3593a2cd85ca9dcffd95af146c6fdf62249be554fa8a5a946a8a078d69
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Red)
          | 4f2d5c0395f10513675c007e25a67d8a000a4bcd064bcb69ad513f07cbf6385d
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Green)
          | 283f5ef79f3bf60aba46362fa51c762679cd762bc1817d7a56fe9e7e546cc4a8
          |ValueEnum(Some(Identifier(pkgId,Mod:ColorBis)),Green)
          | 283f5ef79f3bf60aba46362fa51c762679cd762bc1817d7a56fe9e7e546cc4a8
          |ValueRecord(Some(Identifier(pkgId,Mod:Unit)),ImmArray())
          | be0f4c815b2fdaa3b57c5ac5f4ad3c8b19e0888cf13ae9b2791dbd7350926f7e
          |ValueRecord(Some(Identifier(pkgId,Mod:UnitBis)),ImmArray())
          | be0f4c815b2fdaa3b57c5ac5f4ad3c8b19e0888cf13ae9b2791dbd7350926f7e
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 4d0188dd59d31763de4a8054c38e4406e9cd2998d774893bf049ddc9408b6b7e
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 5ce6ef0139ff1a98d8ffd820665ff6215eae91430e94c4f738fa02fbcf4a4002
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 6084112443fb9127f8e0b9894e9211fb1842c633874bc75498ddea897342e6d8
          |ValueRecord(Some(Identifier(pkgId,Mod:TupleBis)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | 4d0188dd59d31763de4a8054c38e4406e9cd2998d774893bf049ddc9408b6b7e
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(false))
          | 3821c0c9e0d580d49529abdf3ae0bb8c4659009813275e861fb316d4cfa3dbce
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(true))
          | 4fc1205a25e86608bacac58b77b464822b2d4e732dc6702e5c079cb259af4762
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Right,ValueBool(false))
          | 29c47371aaf5e867a74b5d987bb70aeb534dcb09387fd9ffa349832b119ce18e
          |ValueVariant(Some(Identifier(pkgId,Mod:EitherBis)),Left,ValueBool(false))
          | 3821c0c9e0d580d49529abdf3ae0bb8c4659009813275e861fb316d4cfa3dbce
          |""".stripMargin

      val sep = System.getProperty("line.separator")
      val actualOutput = testCases
        .map { value =>
          val hash = Hasher
            .HashBuilderOps(crypto.Hash.builder(crypto.HashPurpose.Testing))
            .addValue(value)
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

  private implicit def decimal(x: BigDecimal): Numeric =
    Numeric.assertFromBigDecimal(Decimal.scale, x)

}
