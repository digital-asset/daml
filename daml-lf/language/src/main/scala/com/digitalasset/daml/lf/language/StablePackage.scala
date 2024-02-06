// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package language

import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion._
import com.daml.lf.language.LanguageVersionRangeOps._

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

  def ids(allowedLanguageVersions: VersionRange[LanguageVersion]): Set[Ref.PackageId] = {
    import scala.Ordering.Implicits.infixOrderingOps
    StablePackages(allowedLanguageVersions.majorVersion).allPackages.view
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
    "35df06619756af233f13051ba77718e172589b979d22dd3ec40c8787563bb435",
    "daml-stdlib",
    v2_1,
  )
  val DA_Logic_Types: StablePackage = new StablePackage(
    "DA.Logic.Types",
    "19cfdcbac283f2a26c05bfcea437177cfb7adb0b2eb8aa82944e91b13b671912",
    "daml-stdlib",
    v2_1,
  )
  val DA_Stack_Types: StablePackage = new StablePackage(
    "DA.Stack.Types",
    "747f749a860db32a01ae0c5c741e6648497b93ffcfef3948854c31cc8167eacf",
    "daml-stdlib",
    v2_1,
  )
  val DA_Monoid_Types: StablePackage = new StablePackage(
    "DA.Monoid.Types",
    "bb581ddc78c4c0e682727d9a2302dc1eba5941809c528aca149a5fdaf25c6cbd",
    "daml-stdlib",
    v2_1,
  )
  val DA_Set_Types: StablePackage = new StablePackage(
    "DA.Set.Types",
    "9d88bb9904dab8f44a47e4f27c8d8ee4fc57fece9c2e3d385ef7ed19fcc24049",
    "daml-stdlib",
    v2_1,
  )
  val DA_Validation_Types: StablePackage = new StablePackage(
    "DA.Validation.Types",
    "4687117abb53238857bccdb0d00be7fc005eb334e1f232de3d78152b90b3f202",
    "daml-stdlib",
    v2_1,
  )
  val DA_Date_Types: StablePackage = new StablePackage(
    "DA.Date.Types",
    "a4c44a89461229bb4a4fddbdeabe3f9dfaf6db35896c87d76f638cd45e1f0678",
    "daml-stdlib",
    v2_1,
  )
  val DA_Semigroup_Types: StablePackage = new StablePackage(
    "DA.Semigroup.Types",
    "ceb729ab26af3934f35fb803534d633c4dd37b466888afcced34a32155e9c2cd",
    "daml-stdlib",
    v2_1,
  )
  val DA_Time_Types: StablePackage = new StablePackage(
    "DA.Time.Types",
    "b47113ba94c31372c553e3869fffed9a45ef1c0f5ac1be3287857cd9450c0bae",
    "daml-stdlib",
    v2_1,
  )
  val DA_NonEmpty_Types: StablePackage = new StablePackage(
    "DA.NonEmpty.Types",
    "d3a94c9e99da7fbb5e35d52d05eec84db27233d4c1aed75548dba1057c84ad81",
    "daml-stdlib",
    v2_1,
  )
  val DA_Random_Types: StablePackage = new StablePackage(
    "DA.Random.Types",
    "58f4cb7b68a305d056a067c03083f80550ed7d98d6fe100a5ddfa282851ba49a",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Interface_AnyView_Types: StablePackage = new StablePackage(
    "DA.Internal.Interface.AnyView.Types",
    "db7b27684bd900f7ca47c854d526eeaffd9e84d761859c66cc6cf5957ad89ed4",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Any: StablePackage = new StablePackage(
    "DA.Internal.Any",
    "7de198711a7a5b3c897eff937b85811438d22f48452a118222590b0ec080bf54",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Template: StablePackage = new StablePackage(
    "DA.Internal.Template",
    "c2eed01333d3c95b12ca5ef0f196db5cd481c58902e01c8ac6b1e49a62875aa5",
    "daml-stdlib",
    v2_1,
  )
  val DA_Action_State_Type: StablePackage = new StablePackage(
    "DA.Action.State.Type",
    "2d7314e12cc21ce1482d7a59ab8b42f8deafbe3cdb62eae9fcd4f583ba0ad8d0",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Erased: StablePackage = new StablePackage(
    "DA.Internal.Erased",
    "71ca307ec24fc584d601fd6d5f49ec76d100730f56eef290e41248f32ccadfb1",
    "daml-prim",
    v2_1,
  )
  val DA_Internal_NatSyn: StablePackage = new StablePackage(
    "DA.Internal.NatSyn",
    "eb6926e50bb83fbc8f3e154c7c88b1219b31a3c0b812f26b276d46e212c2dd71",
    "daml-prim",
    v2_1,
  )
  val DA_Internal_PromotedText: StablePackage = new StablePackage(
    "DA.Internal.PromotedText",
    "c2bac57e7a921c98523ae40766bfefab9f5b7bf4bf34e1c09d9a9d1483417b6c",
    "daml-prim",
    v2_1,
  )
  val GHC_Prim: StablePackage = new StablePackage(
    "GHC.Prim",
    "37375ebfb7ebef8a38aa0d037db55833bcac40eb04074b9024ed81c249ea2387",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_AssertionFailed: StablePackage = new StablePackage(
    "DA.Exception.AssertionFailed",
    "ffc462638e7338aaf5d45b3eae8aba5d8e9259a2e44d1ec9db70ed4ee83601e0",
    "daml-prim",
    v2_1,
  )
  val GHC_Tuple: StablePackage = new StablePackage(
    "GHC.Tuple",
    "afbf83d4d9ef0fb1a4ee4d1d6bef98b5cea044a6d395c1c27834abd7e8eb57ae",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_ArithmeticError: StablePackage = new StablePackage(
    "DA.Exception.ArithmeticError",
    "0e35772044c88dda5159f70a9170eae0f07e2f1942af4ab401e7cd79166e8c94",
    "daml-prim",
    v2_1,
  )
  val GHC_Types: StablePackage = new StablePackage(
    "GHC.Types",
    "2f07eb3e4731beccfd88fcd19177268f120f9a2b06a52534ee808a4ba1e89720",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_GeneralError: StablePackage = new StablePackage(
    "DA.Exception.GeneralError",
    "48426ca53c6510a1d641cdc05dd5b3cea288bd4bcd54311ffaa284b5097d4b9d",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_PreconditionFailed: StablePackage = new StablePackage(
    "DA.Exception.PreconditionFailed",
    "4d035c16dee0b8d75814624a05de9fcb062e942ff3b3b60d913335b468a84789",
    "daml-prim",
    v2_1,
  )

  override val DA_Types: StablePackage = new StablePackage(
    "DA.Types",
    "87530dd1038863bad7bdf02c59ae851bc00f469edb2d7dbc8be3172daafa638c",
    "daml-prim",
    v2_1,
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
