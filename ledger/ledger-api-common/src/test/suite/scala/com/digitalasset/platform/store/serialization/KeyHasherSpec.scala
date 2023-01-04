// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.security.MessageDigest

import com.daml.lf
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.parallel.CollectionConverters._
import scala.language.implicitConversions

class KeyHasherSpec extends AnyWordSpec with Matchers {
  private[this] def templateId(module: String, name: String) = Identifier(
    PackageId.assertFromString("package"),
    QualifiedName(
      ModuleName.assertFromString(module),
      DottedName.assertFromString(name),
    ),
  )

  private[this] def complexValue = {
    val builder = ImmArray.newBuilder[(Option[Name], Value)]
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
        None -> ValueText("field2"),
      ),
    )
    builder += None -> ValueTextMap(
      SortedLookupList(
        Map(
          "keyA" -> ValueText("valueA"),
          "keyB" -> ValueText("valueB"),
        )
      )
    )
    val fields = builder.result()

    ValueRecord(None, fields)
  }

  "KeyHasher" should {

    "be stable" in {
      // Hashing function must not change
      val value = complexValue
      val hash = "2b1019f99147ca726baa3a12509399327746f1f9c4636a6ec5f5d7af1e7c2942"

      KeyHasher.hashKeyString(GlobalKey(templateId("module", "name"), value)) shouldBe hash
    }

    "be deterministic and thread safe" in {
      // Compute many hashes in parallel, check that they are all equal
      // Note: intentionally does not reuse value instances
      val hashes = Vector
        .range(0, 1000)
        .map(_ => GlobalKey(templateId("module", "name"), complexValue))
        .par
        .map(key => KeyHasher.hashKeyString(key))

      hashes.toSet.size shouldBe 1
    }

    "not produce collision in template id" in {
      // Same value but different template ID should produce a different hash
      val value = ValueText("A")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(templateId("AA", "A"), value))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(templateId("A", "AA"), value))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of text" in {
      // Testing whether strings are delimited: ["AA", "A"] vs ["A", "AA"]
      val value1 = ValueList(FrontStack(ValueText("AA"), ValueText("A")))
      val value2 = ValueList(FrontStack(ValueText("A"), ValueText("AA")))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of decimals" in {
      // Testing whether decimals are delimited: [10, 10] vs [101, 0]
      val value1 =
        ValueList(FrontStack(ValueNumeric(decimal(10)), ValueNumeric(decimal(10))))
      val value2 =
        ValueList(FrontStack(ValueNumeric(decimal(101)), ValueNumeric(decimal(0))))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in list of lists" in {
      // Testing whether lists are delimited: [[()], [(), ()]] vs [[(), ()], [()]]
      val value1 =
        ValueList(
          FrontStack(
            ValueList(FrontStack(ValueUnit)),
            ValueList(FrontStack(ValueUnit, ValueUnit)),
          )
        )
      val value2 =
        ValueList(
          FrontStack(
            ValueList(FrontStack(ValueUnit, ValueUnit)),
            ValueList(FrontStack(ValueUnit)),
          )
        )

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Variant constructor" in {
      val value1 =
        ValueVariant(None, Name.assertFromString("A"), ValueUnit)
      val value2 =
        ValueVariant(None, Name.assertFromString("B"), ValueUnit)

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Variant value" in {
      val value1 =
        ValueVariant(None, Name.assertFromString("A"), ValueInt64(0L))
      val value2 =
        ValueVariant(None, Name.assertFromString("A"), ValueInt64(1L))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map keys" in {
      val value1 =
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(0),
            )
          )
        )
      val value2 =
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "C" -> ValueInt64(0),
            )
          )
        )

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Map values" in {
      val value1 =
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(0),
            )
          )
        )
      val value2 =
        ValueTextMap(
          SortedLookupList(
            Map(
              "A" -> ValueInt64(0),
              "B" -> ValueInt64(1),
            )
          )
        )

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Bool" in {
      val value1 = ValueTrue
      val value2 = ValueFalse

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Int64" in {
      val value1 = ValueInt64(0L)
      val value2 = ValueInt64(1L)

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Decimal" in {
      val value1 = ValueNumeric(decimal(0))
      val value2 = ValueNumeric(decimal(1))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Date" in {
      val value1 =
        ValueDate(Time.Date.assertFromDaysSinceEpoch(0))
      val value2 =
        ValueDate(Time.Date.assertFromDaysSinceEpoch(1))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Timestamp" in {
      val value1 =
        ValueTimestamp(Time.Timestamp.assertFromLong(0))
      val value2 =
        ValueTimestamp(Time.Timestamp.assertFromLong(1))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Optional" in {
      val value1 = ValueNone
      val value2 = ValueOptional(Some(ValueUnit))

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }

    "not produce collision in Record" in {
      val value1 =
        ValueRecord(
          None,
          ImmArray(
            None -> ValueText("A"),
            None -> ValueText("B"),
          ),
        )
      val value2 =
        ValueRecord(
          None,
          ImmArray(
            None -> ValueText("A"),
            None -> ValueText("C"),
          ),
        )

      val tid = templateId("module", "name")

      val hash1 = KeyHasher.hashKeyString(GlobalKey(tid, value1))
      val hash2 = KeyHasher.hashKeyString(GlobalKey(tid, value2))

      hash1.equals(hash2) shouldBe false
    }
  }

  "KeyHasher.putValue" should {

    "stable " in {

      type Value = lf.value.Value

      val pkgId = Ref.PackageId.assertFromString("pkgId")

      implicit def toTypeConName(s: String): Ref.TypeConName =
        Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

      implicit def toName(s: String): Ref.Name =
        Ref.Name.assertFromString(s)

      implicit def toSortedLookupList[V](a: ImmArray[(String, V)]): SortedLookupList[V] =
        SortedLookupList.fromOrderedImmArray(a).toOption.get

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
        List[Value](
          ValueUnit
        )
      val bools =
        List[Value](ValueTrue, ValueFalse)
      val ints =
        List[Value](ValueInt64(-1L), ValueInt64(0L), ValueInt64(1L))
      val decimals =
        List[Value](
          ValueNumeric(Numeric.assertFromString("-10000.0000000000")),
          ValueNumeric(Numeric.assertFromString("0.0000000000")),
          ValueNumeric(Numeric.assertFromString("10000.0000000000")),
        )
      val numeric0s =
        List[Value](
          ValueNumeric(Numeric.assertFromString("-10000.")),
          ValueNumeric(Numeric.assertFromString("0.")),
          ValueNumeric(Numeric.assertFromString("10000.")),
        )

      val texts =
        List[Value](
          ValueText(""),
          ValueText("someText"),
          ValueText("a¶‱😂"),
        )
      val dates =
        List[Value](
          ValueDate(Time.Date.assertFromDaysSinceEpoch(0)),
          ValueDate(Time.Date.assertFromString("1969-07-21")),
          ValueDate(Time.Date.assertFromString("2019-12-16")),
        )
      val timestamps =
        List[Value](
          ValueTimestamp(Time.Timestamp.assertFromLong(0)),
          ValueTimestamp(Time.Timestamp.assertFromString("1969-07-21T02:56:15.000000Z")),
          ValueTimestamp(Time.Timestamp.assertFromString("2019-12-16T11:17:54.940779363Z")),
        )
      val parties =
        List[Value](
          ValueParty(Ref.Party.assertFromString("alice")),
          ValueParty(Ref.Party.assertFromString("bob")),
        )
      val contractIds =
        List[Value](
          ValueContractId(
            ContractId.assertFromString(
              "0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"
            )
          ),
          ValueContractId(
            ContractId.assertFromString(
              "0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b"
            )
          ),
        )

      val enums =
        List[Value](
          ValueEnum(Some(EnumTypeCon), EnumCon1),
          ValueEnum(Some(EnumTypeCon), EnumCon2),
          ValueEnum(Some(EnumTypeConBis), EnumCon2),
        )

      val records0 =
        List[Value](
          ValueRecord(Some(Record0TypeCon), ImmArray.Empty),
          ValueRecord(Some(Record0TypeConBis), ImmArray.Empty),
        )

      val records2 =
        List[Value](
          ValueRecord(
            Some(Record2TypeCon),
            ImmArray(Some(fstField) -> ValueFalse, Some(sndField) -> ValueFalse),
          ),
          ValueRecord(
            Some(Record2TypeCon),
            ImmArray(Some(fstField) -> ValueTrue, Some(sndField) -> ValueFalse),
          ),
          ValueRecord(
            Some(Record2TypeCon),
            ImmArray(Some(fstField) -> ValueFalse, Some(sndField) -> ValueTrue),
          ),
          ValueRecord(
            Some(Record2TypeConBis),
            ImmArray(Some(fstField) -> ValueFalse, Some(sndField) -> ValueFalse),
          ),
        )

      val variants = List[Value](
        ValueVariant(Some(VariantTypeCon), VariantCon1, ValueFalse),
        ValueVariant(Some(VariantTypeCon), VariantCon1, ValueTrue),
        ValueVariant(Some(VariantTypeCon), VariantCon2, ValueFalse),
        ValueVariant(Some(VariantTypeConBis), VariantCon1, ValueFalse),
      )

      val lists = List[Value](
        ValueList(FrontStack.empty),
        ValueList(FrontStack(ValueFalse)),
        ValueList(FrontStack(ValueTrue)),
        ValueList(FrontStack(ValueFalse, ValueFalse)),
        ValueList(FrontStack(ValueFalse, ValueTrue)),
        ValueList(FrontStack(ValueTrue, ValueFalse)),
      )

      val textMaps = List[Value](
        ValueTextMap(SortedLookupList.Empty),
        ValueTextMap(ImmArray("a" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueFalse)),
        ValueTextMap(ImmArray("b" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueFalse, "b" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueTrue, "b" -> ValueFalse)),
        ValueTextMap(ImmArray("a" -> ValueFalse, "b" -> ValueTrue)),
        ValueTextMap(ImmArray("a" -> ValueFalse, "c" -> ValueFalse)),
      )

      val optionals = List[Value](
        ValueOptional(None),
        ValueOptional(Some(ValueFalse)),
        ValueOptional(Some(ValueTrue)),
        ValueOptional(Some(ValueOptional(None))),
        ValueOptional(Some(ValueOptional(Some(ValueFalse)))),
      )

      val testCases: List[Value] =
        units ++ bools ++ ints ++ decimals ++ numeric0s ++ dates ++ timestamps ++ texts ++ parties ++ contractIds ++ optionals ++ lists ++ textMaps ++ enums ++ records0 ++ records2 ++ variants

      val expectedOut =
        """ValueUnit
          | 6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d
          |ValueBool(true)
          | 4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a
          |ValueBool(false)
          | 6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d
          |ValueInt64(-1)
          | 12a3ae445661ce5dee78d0650d33362dec29c4f82af05e7e57fb595bbbacf0ca
          |ValueInt64(0)
          | af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc
          |ValueInt64(1)
          | cd2662154e6d76b2b2b92e70c0cac3ccf534f9b74eb5b89819ec509083d00a50
          |ValueNumeric(-10000.0000000000)
          | 92b92de61f21059f70dec535eb848a8275a402b520c6c8c65c29368e7181920a
          |ValueNumeric(0E-10)
          | 7b083527a0b46146dad7ef53700e2f975266e2d212fe683b0307c3ea00e892bc
          |ValueNumeric(10000.0000000000)
          | 44a82bcdb9f16bc015fa19d3db2a8f474c502a0c2f0d3fcf926bf1c934b8de59
          |ValueNumeric(-10000)
          | 92b92de61f21059f70dec535eb848a8275a402b520c6c8c65c29368e7181920a
          |ValueNumeric(0)
          | 7b083527a0b46146dad7ef53700e2f975266e2d212fe683b0307c3ea00e892bc
          |ValueNumeric(10000)
          | 44a82bcdb9f16bc015fa19d3db2a8f474c502a0c2f0d3fcf926bf1c934b8de59
          |ValueDate(1970-01-01)
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueDate(1969-07-21)
          | 434fa8eb0883d896f4bcf774541c2b0a9cadc0d0e5c6e747a40ccc8c2b1007b3
          |ValueDate(2019-12-16)
          | 198ffe57505806fdb66781e9ebd1e3bddef98140ece736496dd4e8f141bd8c8b
          |ValueTimestamp(1970-01-01T00:00:00Z)
          | af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc
          |ValueTimestamp(1969-07-21T02:56:15Z)
          | 29915511efcac543356407a5aeec43025bb79ccce44c8248646830dc3dcf8434
          |ValueTimestamp(2019-12-16T11:17:54.940779Z)
          | d138f5a129f8a66d7c0eccc99116de7dc563a9425543ba33d5401109ec4be877
          |ValueText()
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueText(someText)
          | 5d556be4585eafaf71039732a9be3821c5b960d88099a2749783762d16dcd344
          |ValueText(a¶‱😂)
          | 4f896bd4ef0468d1bef9e88eee1d7a6dc7a0e58045205df0f17345562ba78814
          |ValueParty(alice)
          | e3e40cc57896dcdac6731f60cb1748bd34b45ac0a6e42aa517d41dfea2ff8a88
          |ValueParty(bob)
          | 492f3783b824fb976eac36c0623337a7fd7440b95095581eb81687c71e802943
          |ValueContractId(ContractId(0007e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | a03a7ce4c418622a2187d068208c5ad32460eb62a56797975f39988e959ca377
          |ValueContractId(ContractId(0059b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 01540890e0cd14209bcc408d019266b711ec85a16dac2e8eb3567f6e041cb86b
          |ValueOptional(None)
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueOptional(Some(ValueBool(false)))
          | 060dc63e5595dffbd161c9ec98bc06fcf67cb22e2e75ecdf0003821388aeee4d
          |ValueOptional(Some(ValueBool(true)))
          | a1f9a549ddc784959537f084c79ac5564de8080503dfc842a54616337b87d795
          |ValueOptional(Some(ValueOptional(None)))
          | cbbc48750debb8535093b3deaf88ac7f4cff87425576a58de2bac754acdb4616
          |ValueOptional(Some(ValueOptional(Some(ValueBool(false)))))
          | 2209dc1ac9031cb0089fbd019a1fa065d54ddcef9487f6469d64f9106dbb0c6a
          |ValueList(FrontStack())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueList(FrontStack(ValueBool(false)))
          | 060dc63e5595dffbd161c9ec98bc06fcf67cb22e2e75ecdf0003821388aeee4d
          |ValueList(FrontStack(ValueBool(true)))
          | a1f9a549ddc784959537f084c79ac5564de8080503dfc842a54616337b87d795
          |ValueList(FrontStack(ValueBool(false),ValueBool(false)))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueList(FrontStack(ValueBool(false),ValueBool(true)))
          | 4f1366f56ad5b2ebd9738248a63d9d90bbb3b4b2eac3b74713c6bfd852477802
          |ValueList(FrontStack(ValueBool(true),ValueBool(false)))
          | 06cb0843c56b268bd5fc5373f450e9ee50c49705f3d8d8e33356af5d54ab0315
          |ValueTextMap(SortedLookupList())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | a5da6937a2f4b9d4f58bf5091a2be8560b91d66792de3a69d8a6ca335304e640
          |ValueTextMap(SortedLookupList((a,ValueBool(false))))
          | a5da6937a2f4b9d4f58bf5091a2be8560b91d66792de3a69d8a6ca335304e640
          |ValueTextMap(SortedLookupList((b,ValueBool(false))))
          | aa1e21601e11f7ae67f57ed00fdf8a30b7b787ca454693b9ec17495192ebad8e
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(false))))
          | fc40403816a5de30f0d79bd5c7bf186a6528c58157bc4070abdcad9dcb7fa9d8
          |ValueTextMap(SortedLookupList((a,ValueBool(true)),(b,ValueBool(false))))
          | 336817ad8e596a47b85b0db035efa84fc72acab9f2f4bb079b93621e79a2be4c
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(b,ValueBool(true))))
          | 91e247b396cea58ab670b0767940d360cf1fd541b52444d5b1dcb4d74132d0f9
          |ValueTextMap(SortedLookupList((a,ValueBool(false)),(c,ValueBool(false))))
          | 10e757f68e9e602f8780440193064fec42a7e2f85bec983d416d171079b7240e
          |ValueEnum(Some(pkgId:Mod:Color),Red)
          | 3bf7245f74973e912a49c95a28e77d59594f73c78ede8683663d4bf9eca5c37c
          |ValueEnum(Some(pkgId:Mod:Color),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueEnum(Some(pkgId:Mod:ColorBis),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueRecord(Some(pkgId:Mod:Unit),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(pkgId:Mod:UnitBis),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 06cb0843c56b268bd5fc5373f450e9ee50c49705f3d8d8e33356af5d54ab0315
          |ValueRecord(Some(pkgId:Mod:Tuple),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 4f1366f56ad5b2ebd9738248a63d9d90bbb3b4b2eac3b74713c6bfd852477802
          |ValueRecord(Some(pkgId:Mod:TupleBis),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |ValueVariant(Some(pkgId:Mod:Either),Left,ValueBool(true))
          | bd43854d7f0bfe9fc246492fe783c5e1600a764195152cc240dc1750f7c5ce16
          |ValueVariant(Some(pkgId:Mod:Either),Right,ValueBool(false))
          | 635185b1cff7ebfdbde5045291955d39af1d3c392b30c53d36c06615e5479b24
          |ValueVariant(Some(pkgId:Mod:EitherBis),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |""".stripMargin

      val sep = System.getProperty("line.separator")
      testCases
        .map { value =>
          val digest = MessageDigest.getInstance("SHA-256")
          val hash = KeyHasher.putValue(digest, value).digest.map("%02x" format _).mkString
          s"${value.toString}$sep $hash"
        }
        .mkString("", sep, sep) shouldBe expectedOut

    }
  }

  private implicit def decimal(x: BigDecimal): Numeric =
    Numeric.assertFromBigDecimal(Decimal.scale, x)

}
