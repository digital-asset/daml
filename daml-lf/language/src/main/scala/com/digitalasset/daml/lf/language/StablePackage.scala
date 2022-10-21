// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package language

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.TypeConName

import scala.annotation.nowarn

private[daml] sealed class StablePackage(
    moduleNameStr: String,
    packageIdStr: String,
    nameStr: String,
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

private[daml] object StablePackage {

  object DA {
    object Date {
      object Types
          extends StablePackage(
            "DA.Date.Types",
            "bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f",
            "daml-stdlib",
          )
    }
    object Exception {
      object ArithmeticError
          extends StablePackage(
            "DA.Exception.ArithmeticError",
            "cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a",
            "daml-prim",
          ) {
        val ArithmeticError = assertIdentifier("ArithmeticError")
      }
      object AssertionFailed
          extends StablePackage(
            "DA.Exception.AssertionFailed",
            "3f4deaf145a15cdcfa762c058005e2edb9baa75bb7f95a4f8f6f937378e86415",
            "daml-prim",
          )
      object GeneralError
          extends StablePackage(
            "DA.Exception.GeneralError",
            "86828b9843465f419db1ef8a8ee741d1eef645df02375ebf509cdc8c3ddd16cb",
            "daml-prim",
          )
      object PreconditionFailed
          extends StablePackage(
            "DA.Exception.PreconditionFailed",
            "f20de1e4e37b92280264c08bf15eca0be0bc5babd7a7b5e574997f154c00cb78",
            "daml-prim",
          )
    }
    object Internal {
      object Any
          extends StablePackage(
            "DA.Internal.Any",
            "cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7",
            "daml-stdlib",
          ) {
        val AnyChoice: TypeConName = assertIdentifier("AnyChoice")
        val AnyTemplate: TypeConName = assertIdentifier("AnyTemplate")
        val TemplateTypeRep: TypeConName = assertIdentifier("TemplateTypeRep")
      }
      object Down
          extends StablePackage(
            "DA.Internal.Down",
            "057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba",
            "daml-stdlib",
          )
      object Erased
          extends StablePackage(
            "DA.Internal.Erased",
            "76bf0fd12bd945762a01f8fc5bbcdfa4d0ff20f8762af490f8f41d6237c6524f",
            "daml-prim",
          )
      object NatSyn
          extends StablePackage(
            "DA.Internal.NatSyn",
            "38e6274601b21d7202bb995bc5ec147decda5a01b68d57dda422425038772af7",
            "daml-prim",
          )
      object PromotedText
          extends StablePackage(
            "DA.Internal.PromotedText",
            "d58cf9939847921b2aab78eaa7b427dc4c649d25e6bee3c749ace4c3f52f5c97",
            "daml-prim",
          )
      object Template
          extends StablePackage(
            "DA.Internal.Template",
            "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662",
            "daml-stdlib",
          )
      object Interface {
        object AnyView
            extends StablePackage(
              "DA.Internal.Interface.AnyView",
              "a417c67577f9c321db0d43b5c63f82101d456f45cfe1a61c091a7b5c449f04f1",
              "daml-stdlib",
            )
      }
    }
    object Logic {
      object Types
          extends StablePackage(
            "DA.Logic.Types",
            "c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd",
            "daml-stdlib",
          )
    }
    object Monoid {
      object Types
          extends StablePackage(
            "DA.Monoid.Types",
            "6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06",
            "daml-stdlib",
          )
    }
    object NonEmpty {
      object Types
          extends StablePackage(
            "DA.NonEmpty.Types",
            "e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c",
            "daml-stdlib",
          )
    }
    object Semigroup {
      object Types
          extends StablePackage(
            "DA.Semigroup.Types",
            "8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb",
            "daml-stdlib",
          )
    }
    object Set {
      object Types
          extends StablePackage(
            "DA.Set.Types",
            "97b883cd8a2b7f49f90d5d39c981cf6e110cf1f1c64427a28a6d58ec88c43657",
            "daml-stdlib",
          )
    }
    object Time {
      object Types
          extends StablePackage(
            "DA.Time.Types",
            "733e38d36a2759688a4b2c4cec69d48e7b55ecc8dedc8067b815926c917a182a",
            "daml-stdlib",
          )
    }
    object Types
        extends StablePackage(
          "DA.Types",
          "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7",
          "daml-prim",
        ) {
      val Tuple2: Ref.TypeConName = assertIdentifier("Tuple2")
    }
    object Validation {
      object Types
          extends StablePackage(
            "DA.Validation.Types",
            "99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235",
            "daml-stdlib",
          )
    }
  }

  object GHC {
    object Prim
        extends StablePackage(
          "GHC.Prim",
          "e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b",
          "daml-prim",
        )
    object Tuple
        extends StablePackage(
          "GHC.Tuple",
          "6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d",
          "daml-prim",
        )
    object Types
        extends StablePackage(
          "GHC.Types",
          "518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c",
          "daml-prim",
        )
  }

  private lazy val stablePackages =
    List(
      DA.Date.Types,
      DA.Exception.ArithmeticError,
      DA.Exception.AssertionFailed,
      DA.Exception.GeneralError,
      DA.Exception.PreconditionFailed,
      DA.Internal.Any,
      DA.Internal.Down,
      DA.Internal.Erased,
      DA.Internal.NatSyn,
      DA.Internal.PromotedText,
      DA.Internal.Template,
      DA.Logic.Types,
      DA.Monoid.Types,
      DA.NonEmpty.Types,
      DA.Semigroup.Types,
      DA.Set.Types,
      DA.Time.Types,
      DA.Types,
      DA.Validation.Types,
      GHC.Prim,
      GHC.Tuple,
      GHC.Types,
    )

  private[this] lazy val allStablePackageIds: Set[Ref.PackageId] =
    stablePackages.view.map(_.packageId).toSet

  // TODO (MK) Filter the stable packages to the ones where stablePkgVersion < version
  @nowarn("msg=parameter value allowedLanguageVersions .* is never used")
  def ids(allowedLanguageVersions: VersionRange[LanguageVersion]): Set[Ref.PackageId] =
    allStablePackageIds
}
