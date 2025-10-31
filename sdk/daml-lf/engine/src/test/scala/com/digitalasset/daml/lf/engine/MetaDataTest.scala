// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.{
  CreateKey,
  CreateSerializationVersion,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits._
import com.digitalasset.daml.lf.transaction.test.{
  TestIdFactory,
  TestNodeBuilder,
  TreeTransactionBuilder,
}
import com.digitalasset.daml.lf.transaction.{Node, SerializationVersion}
import com.digitalasset.daml.lf.value.Value.{ValueParty, ValueUnit}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class MetaDataTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with TestIdFactory {

  val helpers = new MetaDataTestHelper
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
      version = CreateSerializationVersion.FromPackage,
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
      version = CreateSerializationVersion.FromPackage,
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
        version = CreateSerializationVersion.FromPackage,
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

class MetaDataTestHelper {

  val langVersion = LanguageVersion.latestStable

  object langNodeBuilder extends TestNodeBuilder {
    override def serializationVersion(packageId: PackageId): Option[SerializationVersion] =
      Some(SerializationVersion.assign(langVersion))
  }

  val engine = Engine.DevEngine

  def emptyPkg(pkgName: String): language.Ast.Package =
    language.Ast.Package(
      Map.empty,
      Set.empty,
      langVersion,
      language.Ast.PackageMetadata(
        Ref.PackageName.assertFromString(pkgName),
        Ref.PackageVersion.assertFromString("0.0.0"),
        None,
      ),
      language.Ast.GeneratedImports(
        reason = "package made in com.digitalasset.daml.lf.engine.MetaDataTest",
        pkgIds = Set.empty,
      ),
    )

  // For the sake of simplicity we load the engine with empty packages where only the directDeps is set.
  List(
    "pkgTLib" -> emptyPkg("pkgTLibName"),
    "pkgT" -> emptyPkg("pkgTName").copy(directDeps = Set("pkgTLib")),
    "pkgIntLib" -> emptyPkg("pkgIntLibName"),
    "pkgInt" -> emptyPkg("pkgIntName").copy(directDeps = Set("pkgIntLib")),
    "pkgBaseLib" -> emptyPkg("pkgBaseLibName"),
    "pkgBase" -> emptyPkg("pkgBaseName").copy(directDeps = Set("pkgBaseLib", "pkgT", "pkgInt")),
    "pkgImplLib" -> emptyPkg("pkgImplLibName"),
    "pkgImpl" -> emptyPkg("pkgImplName").copy(directDeps = Set("pkgImplLib", "pkgInt")),
  ).foreach { case (pkgId, pkg) =>
    require(engine.preloadPackage(pkgId, pkg).isInstanceOf[ResultDone[_]])
  }

  val parties = Set[Ref.Party]("alice")
  val noOne = Set.empty

}
