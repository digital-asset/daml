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
    "54abeb11f0eed3da544d37cbad04d8f866d83acba977bb014b3e346f2eb9e551",
    "daml-stdlib",
    v2_1,
  )
  val DA_Logic_Types: StablePackage = new StablePackage(
    "DA.Logic.Types",
    "edb5aeef08a062be44018bcd548d8141951fcadc6705e483fa7e62d908d84dea",
    "daml-stdlib",
    v2_1,
  )
  val DA_Stack_Types: StablePackage = new StablePackage(
    "DA.Stack.Types",
    "5ba9b13b8f42b1d5d0cdbea93247c8816bfabd2101a9c5972b6852a3151f7100",
    "daml-stdlib",
    v2_1,
  )
  val DA_Monoid_Types: StablePackage = new StablePackage(
    "DA.Monoid.Types",
    "c6ac07a6623e57d226f3289e934c22cd251dda95eb1d82108374023a7e032254",
    "daml-stdlib",
    v2_1,
  )
  val DA_Set_Types: StablePackage = new StablePackage(
    "DA.Set.Types",
    "9511092860971d9c6ba81c73fed994a3670e5279d5bf193e4bbb02063281dab7",
    "daml-stdlib",
    v2_1,
  )
  val DA_Validation_Types: StablePackage = new StablePackage(
    "DA.Validation.Types",
    "7851ba55b61ff1efd2dc04e55093ba273843501d3cb792c5be6e983e94530dd2",
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
    "8bf075ed0f9b502294940d256cadace47e71b7adfa7cce854c1829c2bddf241f",
    "daml-stdlib",
    v2_1,
  )
  val DA_Time_Types: StablePackage = new StablePackage(
    "DA.Time.Types",
    "13f71afbf5d73853a854c2ad9269e47acf5a94c2f533141b5522542f66e86526",
    "daml-stdlib",
    v2_1,
  )
  val DA_NonEmpty_Types: StablePackage = new StablePackage(
    "DA.NonEmpty.Types",
    "d6ae362400b05ec4ed649cc313f5e5bb06a1fed92cce72589ec8ee45573962dc",
    "daml-stdlib",
    v2_1,
  )
  val DA_Random_Types: StablePackage = new StablePackage(
    "DA.Random.Types",
    "c8463c6500cba09d1f52d6851f94882ebfe8b0d9c782291e98f483f8c21e7ae2",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Interface_AnyView_Types: StablePackage = new StablePackage(
    "DA.Internal.Interface.AnyView.Types",
    "2513dbd49a110892bfbfdad4bd0b5aef82e34979d59529c1f7e74b425e561977",
    "daml-stdlib",
    v2_1,
  )
  val DA_Internal_Any: StablePackage = new StablePackage(
    "DA.Internal.Any",
    "053b10c09112715e460733385963e120a75768abf5a5539428a6437017792e65",
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
    "1bf85ad08ef3be26f2d8a864b4bf907f38f65051ddaa18bf1ec5872756010276",
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
    "5548421c4a31fac59b22505f2216177920df47059071a34da3f8d8c07dfeb7f6",
    "daml-prim",
    v2_1,
  )
  val GHC_Tuple: StablePackage = new StablePackage(
    "GHC.Tuple",
    "9c1f8a2f36dfdbf1f30087c75e654fa39cb5fc614503979485b263f70a2e5422",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_ArithmeticError: StablePackage = new StablePackage(
    "DA.Exception.ArithmeticError",
    "ded2974feb90808a03199cad3355a505bf930a717456a85cd7ac6b03ace303c9",
    "daml-prim",
    v2_1,
  )
  val GHC_Types: StablePackage = new StablePackage(
    "GHC.Types",
    "48b29a202dfd2b7c892f113aff1e70ff124059df9f756af4bcf1faf75fc41b19",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_GeneralError: StablePackage = new StablePackage(
    "DA.Exception.GeneralError",
    "449a5a5c62a70ef892325acbd396b77eab3fd5e1e8cb780df40c856bb22a23ea",
    "daml-prim",
    v2_1,
  )
  val DA_Exception_PreconditionFailed: StablePackage = new StablePackage(
    "DA.Exception.PreconditionFailed",
    "9c64df81897c6b98c86063b3a2a4503d756bb7994f06c290ea3d6ad719b76c72",
    "daml-prim",
    v2_1,
  )

  override val DA_Types: StablePackage = new StablePackage(
    "DA.Types",
    "26b14ad5a8a2ed45d75e3c774aeb1c41a918ef2f4a7d2bd40f9716f26c46bfdf",
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
