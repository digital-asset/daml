// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.ValueHasher
import org.scalatest.{Matchers, WordSpec}

import scala.language.implicitConversions

class HasherSpec extends WordSpec with Matchers {

  private type Value = lf.value.Value[AbsoluteContractId]

  private val pkgId = Ref.PackageId.assertFromString("pkgId")

  private implicit def toTypeConName(s: String): Ref.TypeConName =
    Ref.TypeConName(pkgId, Ref.QualifiedName.assertFromString(s"Mod:$s"))

  private implicit def toName(s: String): Ref.Name =
    Ref.Name.assertFromString(s)

  private implicit def toSortedLookupList[V](a: ImmArray[(String, V)]) =
    SortedLookupList.fromSortedImmArray(a).right.get

  private val EnumTypeCon: Ref.TypeConName = "Color"
  private val EnumTypeConBis: Ref.TypeConName = "ColorBis"

  private val EnumCon1: Ref.Name = "Red"
  private val EnumCon2: Ref.Name = "Green"

  private val Record0TypeCon: Ref.TypeConName = "Unit"
  private val Record2TypeCon: Ref.TypeConName = "Tuple"
  private val Record0TypeConBis: Ref.TypeConName = "UnitBis"
  private val Record2TypeConBis: Ref.TypeConName = "TupleBis"
  private val fstField = Ref.Name.assertFromString("_1")
  private val sndField = Ref.Name.assertFromString("_2")

  private val VariantTypeCon: Ref.TypeConName = "Either"
  private val VariantTypeConBis: Ref.TypeConName = "EitherBis"
  private val VariantCon1: Ref.Name = "Left"
  private val VariantCon2: Ref.Name = "Right"

  private val units =
    List[Value](
      ValueUnit
    )
  private val bools =
    List[Value](ValueTrue, ValueFalse)
  private val ints =
    List[Value](ValueInt64(-1L), ValueInt64(0L), ValueInt64(1L))
  private val decimals =
    List[Value](
      ValueNumeric(Numeric.assertFromString("-10000.0000000000")),
      ValueNumeric(Numeric.assertFromString("0.0000000000")),
      ValueNumeric(Numeric.assertFromString("10000.0000000000")),
    )
  private val numeric0s =
    List[Value](
      ValueNumeric(Numeric.assertFromString("-10000.")),
      ValueNumeric(Numeric.assertFromString("0.")),
      ValueNumeric(Numeric.assertFromString("10000.")),
    )

  private val texts =
    List[Value](
      ValueText(""),
      ValueText("someText"),
      ValueText("a¶‱😂"),
    )
  private val dates =
    List[Value](
      ValueDate(Time.Date.assertFromDaysSinceEpoch(0)),
      ValueDate(Time.Date.assertFromString("1969-07-21")),
      ValueDate(Time.Date.assertFromString("2019-12-16")),
    )
  private val timestamps =
    List[Value](
      ValueTimestamp(Time.Timestamp.assertFromLong(0)),
      ValueTimestamp(Time.Timestamp.assertFromString("1969-07-21T02:56:15.000000Z")),
      ValueTimestamp(Time.Timestamp.assertFromString("2019-12-16T11:17:54.940779363Z")),
    )
  private val parties =
    List[Value](
      ValueParty(Ref.Party.assertFromString("alice")),
      ValueParty(Ref.Party.assertFromString("bob")),
    )
  private val contractIds =
    List[Value](
      ValueContractId(
        AbsoluteContractId(Ref.ContractIdString.assertFromString(
          "07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5"))),
      ValueContractId(
        AbsoluteContractId(Ref.ContractIdString.assertFromString(
          "59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b")))
    )

  private val enums =
    List[Value](
      ValueEnum(Some(EnumTypeCon), EnumCon1),
      ValueEnum(Some(EnumTypeCon), EnumCon2),
      ValueEnum(Some(EnumTypeConBis), EnumCon2),
    )

  private val records0 =
    List[Value](
      ValueRecord(Some(Record0TypeCon), ImmArray.empty),
      ValueRecord(Some(Record0TypeCon), ImmArray.empty),
      ValueRecord(Some(Record0TypeConBis), ImmArray.empty),
    )

  private val records2 =
    List[Value](
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

  private val variants = List[Value](
    ValueVariant(Some(VariantTypeCon), VariantCon1, ValueFalse),
    ValueVariant(Some(VariantTypeCon), VariantCon1, ValueTrue),
    ValueVariant(Some(VariantTypeCon), VariantCon2, ValueFalse),
    ValueVariant(Some(VariantTypeConBis), VariantCon1, ValueFalse),
  )

  private val lists = List[Value](
    ValueList(FrontStack.empty),
    ValueList(FrontStack(ValueFalse)),
    ValueList(FrontStack(ValueTrue)),
    ValueList(FrontStack(ValueFalse, ValueFalse)),
    ValueList(FrontStack(ValueFalse, ValueTrue)),
    ValueList(FrontStack(ValueTrue, ValueFalse)),
  )

  private val textMaps = List[Value](
    ValueTextMap(SortedLookupList.empty),
    ValueTextMap(ImmArray("a" -> ValueFalse)),
    ValueTextMap(ImmArray("a" -> ValueFalse)),
    ValueTextMap(ImmArray("b" -> ValueFalse)),
    ValueTextMap(ImmArray("a" -> ValueFalse, "b" -> ValueFalse)),
    ValueTextMap(ImmArray("a" -> ValueTrue, "b" -> ValueFalse)),
    ValueTextMap(ImmArray("a" -> ValueFalse, "b" -> ValueTrue)),
    ValueTextMap(ImmArray("a" -> ValueFalse, "c" -> ValueFalse)),
  )

  private val optionals = List[Value](
    ValueOptional(None),
    ValueOptional(Some(ValueFalse)),
    ValueOptional(Some(ValueTrue)),
    ValueOptional(Some(ValueOptional(None))),
    ValueOptional(Some(ValueOptional(Some(ValueFalse)))),
  )

  "Hashing" should {

    "be stable" in {

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
          |ValueContractId(AbsoluteContractId(07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | fa24d4f2cd646f7e6d7f4e43813e93106d52df42b4272b007d36ba7c9bf21f6b
          |ValueContractId(AbsoluteContractId(59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 65b079e97a8b4804622173ef0c7c86e6bc3b4dbedef9ab7508391b8283279df7
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
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Red)
          | 3bf7245f74973e912a49c95a28e77d59594f73c78ede8683663d4bf9eca5c37c
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueEnum(Some(Identifier(pkgId,Mod:ColorBis)),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueRecord(Some(Identifier(pkgId,Mod:Unit)),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(Identifier(pkgId,Mod:Unit)),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(Identifier(pkgId,Mod:UnitBis)),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 06cb0843c56b268bd5fc5373f450e9ee50c49705f3d8d8e33356af5d54ab0315
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 4f1366f56ad5b2ebd9738248a63d9d90bbb3b4b2eac3b74713c6bfd852477802
          |ValueRecord(Some(Identifier(pkgId,Mod:TupleBis)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(true))
          | bd43854d7f0bfe9fc246492fe783c5e1600a764195152cc240dc1750f7c5ce16
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Right,ValueBool(false))
          | 635185b1cff7ebfdbde5045291955d39af1d3c392b30c53d36c06615e5479b24
          |ValueVariant(Some(Identifier(pkgId,Mod:EitherBis)),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |ValueUnit
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
          |ValueContractId(AbsoluteContractId(07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | fa24d4f2cd646f7e6d7f4e43813e93106d52df42b4272b007d36ba7c9bf21f6b
          |ValueContractId(AbsoluteContractId(59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 65b079e97a8b4804622173ef0c7c86e6bc3b4dbedef9ab7508391b8283279df7
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
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Red)
          | 3bf7245f74973e912a49c95a28e77d59594f73c78ede8683663d4bf9eca5c37c
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueEnum(Some(Identifier(pkgId,Mod:ColorBis)),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueRecord(Some(Identifier(pkgId,Mod:Unit)),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(Identifier(pkgId,Mod:UnitBis)),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 06cb0843c56b268bd5fc5373f450e9ee50c49705f3d8d8e33356af5d54ab0315
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 4f1366f56ad5b2ebd9738248a63d9d90bbb3b4b2eac3b74713c6bfd852477802
          |ValueRecord(Some(Identifier(pkgId,Mod:TupleBis)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(true))
          | bd43854d7f0bfe9fc246492fe783c5e1600a764195152cc240dc1750f7c5ce16
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Right,ValueBool(false))
          | 635185b1cff7ebfdbde5045291955d39af1d3c392b30c53d36c06615e5479b24
          |ValueVariant(Some(Identifier(pkgId,Mod:EitherBis)),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |ValueUnit
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
          |ValueContractId(AbsoluteContractId(07e7b5534931dfca8e1b485c105bae4e10808bd13ddc8e897f258015f9d921c5))
          | fa24d4f2cd646f7e6d7f4e43813e93106d52df42b4272b007d36ba7c9bf21f6b
          |ValueContractId(AbsoluteContractId(59b59ad7a6b6066e77b91ced54b8282f0e24e7089944685cb8f22f32fcbc4e1b))
          | 65b079e97a8b4804622173ef0c7c86e6bc3b4dbedef9ab7508391b8283279df7
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
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Red)
          | 3bf7245f74973e912a49c95a28e77d59594f73c78ede8683663d4bf9eca5c37c
          |ValueEnum(Some(Identifier(pkgId,Mod:Color)),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueEnum(Some(Identifier(pkgId,Mod:ColorBis)),Green)
          | 181bfc4e71007c1dc5406594346ae45a52c2a0bb377800b04e26ce09d8b66004
          |ValueRecord(Some(Identifier(pkgId,Mod:Unit)),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(Identifier(pkgId,Mod:UnitBis)),ImmArray())
          | df3f619804a92fdb4057192dc43dd748ea778adc52bc498ce80524c014b81119
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(true)),(Some(_2),ValueBool(false))))
          | 06cb0843c56b268bd5fc5373f450e9ee50c49705f3d8d8e33356af5d54ab0315
          |ValueRecord(Some(Identifier(pkgId,Mod:Tuple)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(true))))
          | 4f1366f56ad5b2ebd9738248a63d9d90bbb3b4b2eac3b74713c6bfd852477802
          |ValueRecord(Some(Identifier(pkgId,Mod:TupleBis)),ImmArray((Some(_1),ValueBool(false)),(Some(_2),ValueBool(false))))
          | e44728408fa247053c017f791d5d2fe87752119c5010006ffc4e098efbaea679
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Left,ValueBool(true))
          | bd43854d7f0bfe9fc246492fe783c5e1600a764195152cc240dc1750f7c5ce16
          |ValueVariant(Some(Identifier(pkgId,Mod:Either)),Right,ValueBool(false))
          | 635185b1cff7ebfdbde5045291955d39af1d3c392b30c53d36c06615e5479b24
          |ValueVariant(Some(Identifier(pkgId,Mod:EitherBis)),Left,ValueBool(false))
          | 7ac33585fca214756dfe4b2c4de9283d7682f5a47ae8a78acf7abe266d5f41bc
          |""".stripMargin

      remy.log(
        testCases
          .map(value =>
            value.toString + "\n " + ValueHasher.hashValue(value).map("%02x" format _).mkString)
          .mkString("\n"))
      () shouldBe expectedOut
    }
  }

}
