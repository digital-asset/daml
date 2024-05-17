// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.transaction.test.TestNodeBuilder.{CreateKey, CreateTransactionVersion}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.transaction.test.TransactionBuilder.Implicits._
import com.daml.lf.transaction.test.{TestIdFactory, TestNodeBuilder, TreeTransactionBuilder}
import com.daml.lf.transaction.{Node, TransactionVersion}
import com.daml.lf.value.Value.{ValueParty, ValueUnit}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class MetaDataTestV1 extends MetaDataTest(LanguageMajorVersion.V1)
//class MetaDataTestV2 extends MetaDataTest(LanguageMajorVersion.V2)

class MetaDataTest(majorVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with TestIdFactory {

  val helpers = new MetaDataTestHelper(majorVersion)
  import helpers._
  import TreeTransactionBuilder._

  "Engine#desp" should {

    val create = langNodeBuilder.create(
      id = newCid,
      templateId = Ref.Identifier("pkgT", "M:T"),
      argument = ValueUnit,
      signatories = parties,
      observers = noOne,
      key = CreateKey.SignatoryMaintainerKey(ValueParty("alice")),
      version = CreateTransactionVersion.FromPackage,
      packageName = Some(Ref.PackageName.assertFromString("package-name")),
    )
    val nodeWithoutInterface = Table[TestNodeBuilder => Node](
      "transaction",
      _ => create,
      _.exercise(
        contract = create,
        choice = "ChT",
        consuming = false,
        actingParties = parties,
        argument = ValueUnit,
        byKey = false,
      ),
      _.exercise(
        contract = create,
        choice = "ChT",
        consuming = false,
        actingParties = parties,
        argument = ValueUnit,
        byKey = true,
      ),
      _.fetch(contract = create, byKey = false),
      _.fetch(contract = create, byKey = true),
      _.lookupByKey(contract = create),
    )

    val createWithInterface = langNodeBuilder.create(
      id = newCid,
      templateId = Ref.Identifier("pkgImpl", "M:Impl"),
      argument = ValueUnit,
      signatories = parties,
      observers = noOne,
      key = CreateKey.SignatoryMaintainerKey(ValueParty("alice")),
      version = CreateTransactionVersion.FromPackage,
      packageName = somePkgName,
    )
    val nodeWithInterface = Table[TestNodeBuilder => Node](
      "transaction",
      _ => createWithInterface,
      _.exercise(
        contract = createWithInterface,
        choice = "ChI",
        consuming = false,
        actingParties = parties,
        argument = ValueUnit,
        byKey = false,
      ),
      _.fetch(contract = createWithInterface, byKey = false),
    )

    "works as expected on root actions node by template" in {
      val expected = ResultDone(Set("pkgT", "pkgTLib"))
      forEvery(nodeWithoutInterface) { mkNode =>
        engine.deps(toVersionedTransaction(mkNode(langNodeBuilder))) shouldBe expected
      }
    }

    "works as expected on root action nodes by interface" in {
      val expected = ResultDone(Set("pkgInt", "pkgIntLib", "pkgImpl", "pkgImplLib"))
      forEvery(nodeWithInterface) { mkNode =>
        engine.deps(toVersionedTransaction(mkNode(langNodeBuilder))) shouldBe expected
      }
    }

    "works as expected on non-root action nodes" in {
      val expected = ResultDone(
        Set(
          "pkgBase",
          "pkgBaseLib",
          "pkgT",
          "pkgTLib",
          "pkgInt",
          "pkgIntLib",
          "pkgImpl",
          "pkgImplLib",
        )
      )
      val contract = langNodeBuilder.create(
        id = newCid,
        templateId = Ref.Identifier("pkgBase", "M:T"),
        argument = ValueUnit,
        signatories = parties,
        observers = noOne,
        version = CreateTransactionVersion.FromPackage,
        packageName = somePkgName,
      )
      forEvery(nodeWithoutInterface) { mkNodeWithout =>
        forEvery(nodeWithInterface) { mkNodeWith =>
          val exercise = langNodeBuilder.exercise(
            contract = contract,
            choice = "Ch0",
            consuming = true,
            actingParties = parties,
            argument = ValueUnit,
            byKey = false,
          )
          val tx = toVersionedTransaction(
            exercise.withChildren(
              mkNodeWithout(langNodeBuilder),
              mkNodeWith(langNodeBuilder),
            )
          )
          engine.deps(tx) shouldBe expected
        }
      }
    }
  }

}

class MetaDataTestHelper(majorLanguageVersion: LanguageMajorVersion) {

  val langVersion = majorLanguageVersion.maxStableVersion

  object langNodeBuilder extends TestNodeBuilder {
    override def packageTxVersion(packageId: PackageId): Option[TransactionVersion] =
      Some(TransactionVersion.assignNodeVersion(langVersion))
  }

  val somePkgName = Some(Ref.PackageName.assertFromString("package-name"))

  val engine = Engine.DevEngine(majorLanguageVersion)

  val emptyPkg = language.Ast.Package(Map.empty, Set.empty, langVersion, None)

  // For the sake of simplicity we load the engine with empty packages where only the directDeps is set.
  List(
    "pkgTLib" -> emptyPkg,
    "pkgT" -> emptyPkg.copy(directDeps = Set("pkgTLib")),
    "pkgIntLib" -> emptyPkg,
    "pkgInt" -> emptyPkg.copy(directDeps = Set("pkgIntLib")),
    "pkgBaseLib" -> emptyPkg,
    "pkgBase" -> emptyPkg.copy(directDeps = Set("pkgBaseLib", "pkgT", "pkgInt")),
    "pkgImplLib" -> emptyPkg,
    "pkgImpl" -> emptyPkg.copy(directDeps = Set("pkgImplLib", "pkgInt")),
  ).foreach { case (pkgId, pkg) =>
    require(engine.preloadPackage(pkgId, pkg).isInstanceOf[ResultDone[_]])
  }

  val parties = Set[Ref.Party]("alice")
  val noOne = Set.empty

}
