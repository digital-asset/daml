// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package language

import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion._

private[daml] sealed class StablePackage(
    moduleNameStr: String,
    packageIdStr: String,
    nameStr: String,
    val languageVersion: LanguageVersion,
) {
  val moduleName: Ref.ModuleName = Ref.ModuleName.assertFromString(moduleNameStr)
  val packageId: Ref.PackageId = Ref.PackageId.assertFromString(packageIdStr)
  val name: Ref.PackageName = Ref.PackageName.assertFromString(nameStr)

  def identifier(idName: Ref.DottedName): Ref.Identifier =
    Ref.Identifier(packageId, Ref.QualifiedName(moduleName, idName))

  @throws[IllegalArgumentException]
  def assertIdentifier(idName: String): Ref.Identifier =
    identifier(Ref.DottedName.assertFromString(idName))
}

private[daml] trait StablePackages {
  val allPackages: List[StablePackage]

  val DA_Types: StablePackage

  val ArithmeticError: Ref.TypeConName
  val AnyChoice: Ref.TypeConName
  val AnyContractKey: Ref.TypeConName
  val AnyTemplate: Ref.TypeConName
  val TemplateTypeRep: Ref.TypeConName
  val AnyView: Ref.TypeConName
  val NonEmpty: Ref.TypeConName
  val Tuple2: Ref.TypeConName
  val Tuple3: Ref.TypeConName
  val Either: Ref.TypeConName
}

object StablePackages {
  def apply(languageMajorVersion: LanguageMajorVersion): StablePackages =
    languageMajorVersion match {
      case LanguageMajorVersion.V1 => StablePackagesV1
      case LanguageMajorVersion.V2 => StablePackagesV2
    }

  // TODO(#17366): remove once ids uses StablePackages(allowedLanguageVersion.majorVersion).allPackages
  private[this] val allStablePackagesAcrossMajorVersions: List[StablePackage] =
    LanguageMajorVersion.All.flatMap(major => StablePackages(major).allPackages)

  def ids(allowedLanguageVersions: VersionRange[LanguageVersion]): Set[Ref.PackageId] = {
    import scala.Ordering.Implicits.infixOrderingOps
    // TODO(#17366): use StablePackages(allowedLanguageVersion.majorVersion).allPackages instead to
    //    restrict the packages to those matching the major version of allowedLanguageVersions once
    //    Canton stops feeding LF v1 packages (AdminWorkflowsWithVacuuming and AdminWorkflows) to
    //    the engine no matter what.
    allStablePackagesAcrossMajorVersions.view
      .filter(_.languageVersion <= allowedLanguageVersions.max)
      .map(_.packageId)
      .toSet
  }
}

private[daml] object StablePackagesV1 extends StablePackages {

  val DA_Date_Types: StablePackage = new StablePackage(
    "DA.Date.Types",
    "bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f",
    "daml-stdlib",
    v1_6,
  )
  val DA_Exception_ArithmeticError: StablePackage = new StablePackage(
    "DA.Exception.ArithmeticError",
    "cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a",
    "daml-prim",
    v1_14,
  )
  val DA_Exception_AssertionFailed: StablePackage = new StablePackage(
    "DA.Exception.AssertionFailed",
    "3f4deaf145a15cdcfa762c058005e2edb9baa75bb7f95a4f8f6f937378e86415",
    "daml-prim",
    v1_14,
  )
  val DA_Exception_GeneralError: StablePackage = new StablePackage(
    "DA.Exception.GeneralError",
    "86828b9843465f419db1ef8a8ee741d1eef645df02375ebf509cdc8c3ddd16cb",
    "daml-prim",
    v1_14,
  )
  val DA_Exception_PreconditionFailed: StablePackage = new StablePackage(
    "DA.Exception.PreconditionFailed",
    "f20de1e4e37b92280264c08bf15eca0be0bc5babd7a7b5e574997f154c00cb78",
    "daml-prim",
    v1_14,
  )
  val DA_Internal_Any: StablePackage = new StablePackage(
    "DA.Internal.Any",
    "cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7",
    "daml-stdlib",
    v1_7,
  )
  val DA_Internal_Down: StablePackage = new StablePackage(
    "DA.Internal.Down",
    "057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba",
    "daml-stdlib",
    v1_6,
  )
  val DA_Internal_Erased: StablePackage = new StablePackage(
    "DA.Internal.Erased",
    "76bf0fd12bd945762a01f8fc5bbcdfa4d0ff20f8762af490f8f41d6237c6524f",
    "daml-prim",
    v1_6,
  )
  val DA_Internal_Interface_AnyView_Types: StablePackage = new StablePackage(
    "DA.Internal.Interface.AnyView.Types",
    "6df2d1fd8ea994ed048a79587b2722e3a887ac7592abf31ecf46fe09ac02d689",
    "daml-stdlib",
    v1_15,
  )
  val DA_Internal_NatSyn: StablePackage = new StablePackage(
    "DA.Internal.NatSyn",
    "38e6274601b21d7202bb995bc5ec147decda5a01b68d57dda422425038772af7",
    "daml-prim",
    v1_14,
  )
  val DA_Internal_PromotedText: StablePackage = new StablePackage(
    "DA.Internal.PromotedText",
    "d58cf9939847921b2aab78eaa7b427dc4c649d25e6bee3c749ace4c3f52f5c97",
    "daml-prim",
    v1_6,
  )
  val DA_Internal_Template: StablePackage = new StablePackage(
    "DA.Internal.Template",
    "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662",
    "daml-stdlib",
    v1_6,
  )
  val DA_Logic_Types: StablePackage = new StablePackage(
    "DA.Logic.Types",
    "c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd",
    "daml-stdlib",
    v1_6,
  )
  val DA_Monoid_Types: StablePackage = new StablePackage(
    "DA.Monoid.Types",
    "6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06",
    "daml-stdlib",
    v1_6,
  )
  val DA_NonEmpty_Types: StablePackage = new StablePackage(
    "DA.NonEmpty.Types",
    "e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c",
    "daml-stdlib",
    v1_6,
  )
  val DA_Semigroup_Types: StablePackage = new StablePackage(
    "DA.Semigroup.Types",
    "8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb",
    "daml-stdlib",
    v1_6,
  )
  val DA_Set_Types: StablePackage = new StablePackage(
    "DA.Set.Types",
    "97b883cd8a2b7f49f90d5d39c981cf6e110cf1f1c64427a28a6d58ec88c43657",
    "daml-stdlib",
    v1_11,
  )
  val DA_Time_Types: StablePackage = new StablePackage(
    "DA.Time.Types",
    "733e38d36a2759688a4b2c4cec69d48e7b55ecc8dedc8067b815926c917a182a",
    "daml-stdlib",
    v1_6,
  )
  val DA_Validation_Types: StablePackage = new StablePackage(
    "DA.Validation.Types",
    "99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235",
    "daml-stdlib",
    v1_6,
  )
  val GHC_Prim: StablePackage = new StablePackage(
    "GHC.Prim",
    "e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b",
    "daml-prim",
    v1_6,
  )
  val GHC_Tuple: StablePackage = new StablePackage(
    "GHC.Tuple",
    "6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d",
    "daml-prim",
    v1_6,
  )
  val GHC_Types: StablePackage = new StablePackage(
    "GHC.Types",
    "518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c",
    "daml-prim",
    v1_6,
  )
  val DA_Action_State_Type: StablePackage = new StablePackage(
    "DA.Action.State.Type",
    "10e0333b52bba1ff147fc408a6b7d68465b157635ee230493bd6029b750dcb05",
    "daml-stdlib",
    v1_14,
  )
  val DA_Random_Types: StablePackage = new StablePackage(
    "DA.Random.Types",
    "e4cc67c3264eba4a19c080cac5ab32d87551578e0f5f58b6a9460f91c7abc254",
    "daml-stdlib",
    v1_14,
  )
  val DA_Stack_Types: StablePackage = new StablePackage(
    "DA.Stack.Types",
    "5921708ce82f4255deb1b26d2c05358b548720938a5a325718dc69f381ba47ff",
    "daml-stdlib",
    v1_14,
  )

  override val DA_Types: StablePackage = new StablePackage(
    "DA.Types",
    "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7",
    "daml-prim",
    v1_6,
  )

  override val allPackages: List[StablePackage] =
    List(
      DA_Date_Types,
      DA_Exception_ArithmeticError,
      DA_Exception_AssertionFailed,
      DA_Exception_GeneralError,
      DA_Exception_PreconditionFailed,
      DA_Internal_Any,
      DA_Internal_Down,
      DA_Internal_Erased,
      DA_Internal_Interface_AnyView_Types,
      DA_Internal_NatSyn,
      DA_Internal_PromotedText,
      DA_Internal_Template,
      DA_Logic_Types,
      DA_Monoid_Types,
      DA_NonEmpty_Types,
      DA_Semigroup_Types,
      DA_Set_Types,
      DA_Time_Types,
      DA_Types,
      DA_Validation_Types,
      GHC_Prim,
      GHC_Tuple,
      GHC_Types,
      DA_Action_State_Type,
      DA_Random_Types,
      DA_Stack_Types,
    )

  override val ArithmeticError: Ref.TypeConName =
    DA_Exception_ArithmeticError.assertIdentifier("ArithmeticError")
  override val AnyChoice: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyChoice")
  override val AnyContractKey: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyContractKey")
  override val AnyTemplate: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyTemplate")
  override val TemplateTypeRep: Ref.TypeConName =
    DA_Internal_Any.assertIdentifier("TemplateTypeRep")
  override val AnyView: Ref.TypeConName =
    DA_Internal_Interface_AnyView_Types.assertIdentifier("AnyView")
  override val NonEmpty: Ref.TypeConName = DA_NonEmpty_Types.assertIdentifier("NonEmpty")
  override val Tuple2: Ref.TypeConName = DA_Types.assertIdentifier("Tuple2")
  override val Tuple3: Ref.TypeConName = DA_Types.assertIdentifier("Tuple3")
  override val Either: Ref.TypeConName = GHC_Tuple.assertIdentifier("Either")
}

private[daml] object StablePackagesV2 extends StablePackages {
  val DA_Internal_Down: StablePackage = new StablePackage(
    "DA.Internal.Down",
    "901c94892ca0a8b7db859ff8123670390569900c72df0e6f713bd343287369b3",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Logic_Types: StablePackage = new StablePackage(
    "DA.Logic.Types",
    "03a9c4f3f8350fe657f8b50d94446e75e52d2109525d5fb53e3e13d808651c56",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Stack_Types: StablePackage = new StablePackage(
    "DA.Stack.Types",
    "ebc0898bbdcca2067b50aac5dec20f466a1109e0dd981fed2620d7630b9e30df",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Monoid_Types: StablePackage = new StablePackage(
    "DA.Monoid.Types",
    "f4873a4200dbc9c6580c32f0e833f7caa045b172c6823efae425a5ddebd9bdf7",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Set_Types: StablePackage = new StablePackage(
    "DA.Set.Types",
    "c3a0bc4a29dabe85ac4fc5620111e8c6fdb5c5b39ead8c796a3250877f7f7eeb",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Validation_Types: StablePackage = new StablePackage(
    "DA.Validation.Types",
    "05a7e7c3ba9e21ed445a755da14cc8d0d4054351cb9f8aa0a6c2bbdaa843cefe",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Date_Types: StablePackage = new StablePackage(
    "DA.Date.Types",
    "c433203574171536938edb649580d6e386c04cf4370849ac9cdcedf15083514b",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Semigroup_Types: StablePackage = new StablePackage(
    "DA.Semigroup.Types",
    "4526a9c3ed3541c184915e23ecebdd37da992bc54c1d8acf7452c233baaa03dc",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Time_Types: StablePackage = new StablePackage(
    "DA.Time.Types",
    "2dc4fa91858cf886fd1bc04657ee3306a1f7afbe7cee71bdade02e34a71086e4",
    "daml-stdlib",
    v2_dev,
  )
  val DA_NonEmpty_Types: StablePackage = new StablePackage(
    "DA.NonEmpty.Types",
    "01fad85c1387f07cdb501393126f28c26b996e0f915eeca7f5f948633386351b",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Random_Types: StablePackage = new StablePackage(
    "DA.Random.Types",
    "9834dfea5448af3bbdff5ff018b339934e0e0b7c832e7b6d7edd29e2573d3f87",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Internal_Interface_AnyView_Types: StablePackage = new StablePackage(
    "DA.Internal.Interface.AnyView.Types",
    "a1e881e552a5cdbcd2527689b83d9dd5abadfa6e8f301591dc1eb8eb5d743b64",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Internal_Any: StablePackage = new StablePackage(
    "DA.Internal.Any",
    "dced80f58028bf24a09a7eeec64835235113268f2cc12f7c2f508d996c127c39",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Internal_Template: StablePackage = new StablePackage(
    "DA.Internal.Template",
    "47b219490b21b933f77b9fd2fa50a6e099cf6dc61e130f4ee4f2fd483486910f",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Action_State_Type: StablePackage = new StablePackage(
    "DA.Action.State.Type",
    "8264648065732f3911cc7d312dbdce483be209034436c0d94368911d564d766e",
    "daml-stdlib",
    v2_dev,
  )
  val DA_Internal_Erased: StablePackage = new StablePackage(
    "DA.Internal.Erased",
    "dbaaeb5bca3128ab47a6002269eaec4746eaaa1e3f4261daa2efda85ec39d0dd",
    "daml-prim",
    v2_dev,
  )
  val DA_Internal_NatSyn: StablePackage = new StablePackage(
    "DA.Internal.NatSyn",
    "1a5600a3bec70ddc384f9ba816478df607aab9dd14f8b51ce8aa4430fcc62bad",
    "daml-prim",
    v2_dev,
  )
  val DA_Internal_PromotedText: StablePackage = new StablePackage(
    "DA.Internal.PromotedText",
    "621e7201be325597066b72a87ce059e776a7f55684b47f2cedda00eddd3a99bb",
    "daml-prim",
    v2_dev,
  )
  val GHC_Prim: StablePackage = new StablePackage(
    "GHC.Prim",
    "576e4df7bd43f1612662e771bf9e954b2092c951f171fd49951d574635c94d1b",
    "daml-prim",
    v2_dev,
  )
  val DA_Exception_AssertionFailed: StablePackage = new StablePackage(
    "DA.Exception.AssertionFailed",
    "650bde086984ec3b611a8245d55052a6583ce88b20608e24af3333f8e835e343",
    "daml-prim",
    v2_dev,
  )
  val GHC_Tuple: StablePackage = new StablePackage(
    "GHC.Tuple",
    "ae7354ff73b60d939080ec043a63c4984d78e259deecac984d46a62fc2435f2a",
    "daml-prim",
    v2_dev,
  )
  val DA_Exception_ArithmeticError: StablePackage = new StablePackage(
    "DA.Exception.ArithmeticError",
    "e22c0fb5c45cdcc51753d5e384f5bfbb2583d0cfd2a939404ffa4257ec0dae74",
    "daml-prim",
    v2_dev,
  )
  val GHC_Types: StablePackage = new StablePackage(
    "GHC.Types",
    "fc600344f90b24b60069a8173cbdae3867708b4354ae6d98e280243551aa6630",
    "daml-prim",
    v2_dev,
  )
  val DA_Exception_GeneralError: StablePackage = new StablePackage(
    "DA.Exception.GeneralError",
    "04b7860c1ff4cd488f249d7626bd1aeb4487e28f1e69649d6e2f10a6580a1da1",
    "daml-prim",
    v2_dev,
  )
  val DA_Exception_PreconditionFailed: StablePackage = new StablePackage(
    "DA.Exception.PreconditionFailed",
    "11b13322111b649f3ab3a71ef523ed3167d9a4edbb60f1c5aa48d7173582d841",
    "daml-prim",
    v2_dev,
  )

  override val DA_Types: StablePackage = new StablePackage(
    "DA.Types",
    "312d1994bf28ca0546e6dce6fea9fd64c2fb6de43e173ea22d4d4fd6d00962df",
    "daml-prim",
    v2_dev,
  )

  override val allPackages: List[StablePackage] =
    List(
      DA_Date_Types,
      DA_Exception_ArithmeticError,
      DA_Exception_AssertionFailed,
      DA_Exception_GeneralError,
      DA_Exception_PreconditionFailed,
      DA_Internal_Any,
      DA_Internal_Down,
      DA_Internal_Erased,
      DA_Internal_Interface_AnyView_Types,
      DA_Internal_NatSyn,
      DA_Internal_PromotedText,
      DA_Internal_Template,
      DA_Logic_Types,
      DA_Monoid_Types,
      DA_NonEmpty_Types,
      DA_Semigroup_Types,
      DA_Set_Types,
      DA_Time_Types,
      DA_Types,
      DA_Validation_Types,
      GHC_Prim,
      GHC_Tuple,
      GHC_Types,
      DA_Action_State_Type,
      DA_Random_Types,
      DA_Stack_Types,
    )

  override val ArithmeticError: Ref.TypeConName =
    DA_Exception_ArithmeticError.assertIdentifier("ArithmeticError")
  override val AnyChoice: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyChoice")
  override val AnyContractKey: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyContractKey")
  override val AnyTemplate: Ref.TypeConName = DA_Internal_Any.assertIdentifier("AnyTemplate")
  override val TemplateTypeRep: Ref.TypeConName =
    DA_Internal_Any.assertIdentifier("TemplateTypeRep")
  override val AnyView: Ref.TypeConName =
    DA_Internal_Interface_AnyView_Types.assertIdentifier("AnyView")
  override val NonEmpty: Ref.TypeConName = DA_NonEmpty_Types.assertIdentifier("NonEmpty")
  override val Tuple2: Ref.TypeConName = DA_Types.assertIdentifier("Tuple2")
  override val Tuple3: Ref.TypeConName = DA_Types.assertIdentifier("Tuple3")
  override val Either: Ref.TypeConName = GHC_Tuple.assertIdentifier("Either")
}
