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
    "dcbdacc42764d3887610f44ffe5134f09a500ffcfea5b0696a0693e82b104cda",
    "daml-stdlib",
    v2_1,
  )
  val DA_Logic_Types: StablePackage = new StablePackage(
    "DA.Logic.Types",
    "3f28b0de0e1a9f71e5d8e1e6c2dc0625aefd32672fb29c9fdd8145a07bedd5f1",
    "daml-stdlib",
    v2_1,
  )
  val DA_Stack_Types: StablePackage = new StablePackage(
    "DA.Stack.Types",
    "baf3f887f97419ebd2b87e25b6b7ab8b66145c9813ef15851249a9eff75efb79",
    "daml-stdlib",
    v2_1,
  )
  val DA_Monoid_Types: StablePackage = new StablePackage(
    "DA.Monoid.Types",
    "db242b5ad988328f01188f72988552a89bfbe630b8157e1e11f60758499a2ce5",
    "daml-stdlib",
    v2_1,
  )
  val DA_Set_Types: StablePackage = new StablePackage(
    "DA.Set.Types",
    "eb6c6231bdfb99f78b09c2241af1774a96ee2d557f8269deba92c3ce6340e90e",
    "daml-stdlib",
    v2_1,
  )
  val DA_Validation_Types: StablePackage = new StablePackage(
    "DA.Validation.Types",
    "4ad518a6e60589d5bd7f0e20196f89cb017b5e193160841ab36e39e9a35b3118",
    "daml-stdlib",
    v2_1,
  )
  val DA_Date_Types: StablePackage = new StablePackage(
    "DA.Date.Types",
    "001109f95f991bea2ce8d641c2188d9f8c9d2909786549ba6d652024b3680e63",
    "daml-stdlib",
    v2_1,
  )
  val DA_Semigroup_Types: StablePackage = new StablePackage(
    "DA.Semigroup.Types",
    "1b912db5849106a731884f56cfdf8414a573a535ecc9422d95410e6b52aae93c",
    "daml-stdlib",
    v2_1,
  )
  val DA_Time_Types: StablePackage = new StablePackage(
    "DA.Time.Types",
    "f141230257fa9c6467b03e6ae3cc73a42ff1fdaf14ff172d91ec78cfeb181633",
    "daml-stdlib",
    v2_1,
  )
  val DA_NonEmpty_Types: StablePackage = new StablePackage(
    "DA.NonEmpty.Types",
    "a997b91f62098e8ed20659cc42265ece0c2edc3bde2fbc082bfb0a5bb6ead7e5",
    "daml-stdlib",
    v2_1,
  )
  val DA_Random_Types: StablePackage = new StablePackage(
    "DA.Random.Types",
    "840302f70e3557d06c98e73282c84542b705a81d5fa4ba669b5750acae42d15d",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Interface_AnyView_Types: StablePackage = new StablePackage(
    "DA.Internal.Interface.AnyView.Types",
    "5e93adc04d625458acbfc898bc92665d1d1947c10ff0a42661cd44b9f8b84d57",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Any: StablePackage = new StablePackage(
    "DA.Internal.Any",
    "34b23589825aee625d6e4fb70d56404cfd999083a2ec4abf612757d48f7c14df",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Template: StablePackage = new StablePackage(
    "DA.Internal.Template",
    "ace2eb6a9cd13bca35ce7f068b942ab5c47987eed34efea52470b3aa0458a2f5",
    "daml-stdlib",
    v2_1,
  )
  val DA_Action_State_Type: StablePackage = new StablePackage(
    "DA.Action.State.Type",
    "d6da817f1d703324619eaae87be5a80c16eb0a1da0cdae48d1dd5111aee5be30",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Erased: StablePackage = new StablePackage(
    "DA.Internal.Erased",
    "a486f9d83acf91ddcb27a9a8743c042f310beab20be676cfc37220961df03900",
    "daml-prim",
    v2_1,
  )
  val DA_Internal_NatSyn: StablePackage = new StablePackage(
    "DA.Internal.NatSyn",
    "ce33df2997d69e8ac89f00951c322753e60abccdfdd92d47d804518a2029748f",
    "daml-prim",
    v2_1,
  )
  val DA_Internal_PromotedText: StablePackage = new StablePackage(
    "DA.Internal.PromotedText",
    "ad8708bc34bce0096a8f43500940f0d62fbf947aed8484efa92dc6ae2f9126ac",
    "daml-prim",
    v2_1,
  )
  val GHC_Prim: StablePackage = new StablePackage(
    "GHC.Prim",
    "574f715baa8298bf09261ba87a77589f5aeef88e12b7b672cb80c4d2604035fa",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_AssertionFailed: StablePackage = new StablePackage(
    "DA.Exception.AssertionFailed",
    "f3ae1c80664957609d28f76e8f0bf8a5aaec25d93c786501b785ab5a49b2a8f7",
    "daml-prim",
    v2_1,
  )
  val GHC_Tuple: StablePackage = new StablePackage(
    "GHC.Tuple",
    "91c8a48444c590867fe71f4da1c3001516b45ecaf2fdad381ad60d179224cd9b",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_ArithmeticError: StablePackage = new StablePackage(
    "DA.Exception.ArithmeticError",
    "e1713757215a2462915fed49bbe47c3e459dfabb3fb9521a832cc7bb32499bae",
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
    "7e1ea9e0fbef673f4ee6c0059c7ede41dfd893a6736a23e263ad9b4395d89ff1",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_PreconditionFailed: StablePackage = new StablePackage(
    "DA.Exception.PreconditionFailed",
    "4c290f7b9fe25f59a2d0ad07ee506d5e0a3ecfdb413e5c32e60dafa1052fdd6a",
    "daml-prim",
    v2_1,
  )

  override val DA_Types: StablePackage = new StablePackage(
    "DA.Types",
    "202599a30d109125440918fdd6cd5f35c9e76175ef43fa5c9d6d9fd1eb7b66ff",
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
