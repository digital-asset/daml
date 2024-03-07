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

private[daml] object StablePackagesV2 extends StablePackages {
  val DA_Internal_Down: StablePackage = new StablePackage(
    "DA.Internal.Down",
    "86d888f34152dae8729900966b44abcb466b9c111699678de58032de601d2b04",
    "daml-stdlib",
    v2_1,
  )
  val DA_Logic_Types: StablePackage = new StablePackage(
    "DA.Logic.Types",
    "cae345b5500ef6f84645c816f88b9f7a85a9f3c71697984abdf6849f81e80324",
    "daml-stdlib",
    v2_1,
  )
  val DA_Stack_Types: StablePackage = new StablePackage(
    "DA.Stack.Types",
    "60c61c542207080e97e378ab447cc355ecc47534b3a3ebbff307c4fb8339bc4d",
    "daml-stdlib",
    v2_1,
  )
  val DA_Monoid_Types: StablePackage = new StablePackage(
    "DA.Monoid.Types",
    "52854220dc199884704958df38befd5492d78384a032fd7558c38f00e3d778a2",
    "daml-stdlib",
    v2_1,
  )
  val DA_Set_Types: StablePackage = new StablePackage(
    "DA.Set.Types",
    "c3bb0c5d04799b3f11bad7c3c102963e115cf53da3e4afcbcfd9f06ebd82b4ff",
    "daml-stdlib",
    v2_1,
  )
  val DA_Validation_Types: StablePackage = new StablePackage(
    "DA.Validation.Types",
    "3cde94fe9be5c700fc1d9a8ad2277e2c1214609f8c52a5b4db77e466875b8cb7",
    "daml-stdlib",
    v2_1,
  )
  val DA_Date_Types: StablePackage = new StablePackage(
    "DA.Date.Types",
    "fa79192fe1cce03d7d8db36471dde4cf6c96e6d0f07e1c391dd49e355af9b38c",
    "daml-stdlib",
    v2_1,
  )
  val DA_Semigroup_Types: StablePackage = new StablePackage(
    "DA.Semigroup.Types",
    "d095a2ccf6dd36b2415adc4fa676f9191ba63cd39828dc5207b36892ec350cbc",
    "daml-stdlib",
    v2_1,
  )
  val DA_Time_Types: StablePackage = new StablePackage(
    "DA.Time.Types",
    "b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946",
    "daml-stdlib",
    v2_1,
  )
  val DA_NonEmpty_Types: StablePackage = new StablePackage(
    "DA.NonEmpty.Types",
    "bde4bd30749e99603e5afa354706608601029e225d4983324d617825b634253a",
    "daml-stdlib",
    v2_1,
  )
  val DA_Random_Types: StablePackage = new StablePackage(
    "DA.Random.Types",
    "bfda48f9aa2c89c895cde538ec4b4946c7085959e031ad61bde616b9849155d7",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Interface_AnyView_Types: StablePackage = new StablePackage(
    "DA.Internal.Interface.AnyView.Types",
    "c280cc3ef501d237efa7b1120ca3ad2d196e089ad596b666bed59a85f3c9a074",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Any: StablePackage = new StablePackage(
    "DA.Internal.Any",
    "6f8e6085f5769861ae7a40dccd618d6f747297d59b37cab89b93e2fa80b0c024",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Template: StablePackage = new StablePackage(
    "DA.Internal.Template",
    "9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69",
    "daml-stdlib",
    v2_1,
  )
  val DA_Action_State_Type: StablePackage = new StablePackage(
    "DA.Action.State.Type",
    "a1fa18133ae48cbb616c4c148e78e661666778c3087d099067c7fe1868cbb3a1",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Erased: StablePackage = new StablePackage(
    "DA.Internal.Erased",
    "0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816",
    "daml-prim",
    v2_1,
  )
  val DA_Internal_NatSyn: StablePackage = new StablePackage(
    "DA.Internal.NatSyn",
    "e5411f3d75f072b944bd88e652112a14a3d409c491fd9a51f5f6eede6d3a3348",
    "daml-prim",
    v2_1,
  )
  val DA_Internal_PromotedText: StablePackage = new StablePackage(
    "DA.Internal.PromotedText",
    "ab068e2f920d0e06347975c2a342b71f8b8e3b4be0f02ead9442caac51aa8877",
    "daml-prim",
    v2_1,
  )
  val GHC_Prim: StablePackage = new StablePackage(
    "GHC.Prim",
    "fcee8dfc1b81c449b421410edd5041c16ab59c45bbea85bcb094d1b17c3e9df7",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_AssertionFailed: StablePackage = new StablePackage(
    "DA.Exception.AssertionFailed",
    "6da1f43a10a179524e840e7288b47bda213339b0552d92e87ae811e52f59fc0e",
    "daml-prim",
    v2_1,
  )
  val GHC_Tuple: StablePackage = new StablePackage(
    "GHC.Tuple",
    "19f0df5fdaf5a96e137b6ea885fdb378f37bd3166bd9a47ee11518e33fa09a20",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_ArithmeticError: StablePackage = new StablePackage(
    "DA.Exception.ArithmeticError",
    "ee33fb70918e7aaa3d3fc44d64a399fb2bf5bcefc54201b1690ecd448551ba88",
    "daml-prim",
    v2_1,
  )
  val GHC_Types: StablePackage = new StablePackage(
    "GHC.Types",
    "e7e0adfa881e7dbbb07da065ae54444da7c4bccebcb8872ab0cb5dcf9f3761ce",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_GeneralError: StablePackage = new StablePackage(
    "DA.Exception.GeneralError",
    "f181cd661f7af3a60bdaae4b0285a2a67beb55d6910fc8431dbae21a5825ec0f",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_PreconditionFailed: StablePackage = new StablePackage(
    "DA.Exception.PreconditionFailed",
    "91e167fa7a256f21f990c526a0a0df840e99aeef0e67dc1f5415b0309486de74",
    "daml-prim",
    v2_1,
  )

  override val DA_Types: StablePackage = new StablePackage(
    "DA.Types",
    "5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4",
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
