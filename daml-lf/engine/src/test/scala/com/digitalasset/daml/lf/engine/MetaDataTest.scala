// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.test.TransactionBuilder.Implicits._
import com.daml.lf.value.Value.{ValueParty, ValueUnit}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class MetaDataTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import MetaDataTest._

  "Engine#desp" should {

    val create = newBuilder.create(
      id = TransactionBuilder.newCid,
      templateId = Ref.Identifier("pkgT", "M:T"),
      argument = ValueUnit,
      signatories = parties,
      observers = noOne,
      key = Some(ValueParty("alice")),
      maintainers = parties,
    )
    val nodeWithoutInterface = Table[TransactionBuilder => Node](
      "transaction",
      _ => create,
      _.exercise(
        contract = create,
        choice = "ChT",
        consuming = false,
        actingParties = parties,
        argument = ValueUnit,
      ),
      _.exerciseByKey(
        contract = create,
        choice = "ChT",
        consuming = false,
        actingParties = parties,
        argument = ValueUnit,
      ),
      _.fetch(contract = create),
      _.fetchByKey(contract = create),
      _.lookupByKey(contract = create),
    )

    val createWithInterface = newBuilder.create(
      id = TransactionBuilder.newCid,
      templateId = Ref.Identifier("pkgImpl", "M:Impl"),
      argument = ValueUnit,
      signatories = parties,
      observers = noOne,
      key = Some(ValueParty("alice")),
      maintainers = parties,
    )
    val nodeWithInterface = Table[TransactionBuilder => Node](
      "transaction",
      _ => createWithInterface,
      _.exercise(
        contract = createWithInterface,
        choice = "ChI",
        consuming = false,
        actingParties = parties,
        argument = ValueUnit,
      ),
      _.fetch(contract = createWithInterface),
    )

    "works as expected on root actions node by template" in {
      val expected = ResultDone(Set("pkgT", "pkgTLib"))
      forEvery(nodeWithoutInterface) { mkNode =>
        val builder = newBuilder
        builder.add(mkNode(builder))
        engine.deps(builder.build()) shouldBe expected
      }
    }

    "works as expected on root action nodes by interface" in {
      val expected = ResultDone(Set("pkgInt", "pkgIntLib", "pkgImpl", "pkgImplLib"))
      forEvery(nodeWithInterface) { mkNode =>
        val builder = newBuilder
        builder.add(mkNode(builder))
        engine.deps(builder.build()) shouldBe expected
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
      val contract = newBuilder.create(
        id = TransactionBuilder.newCid,
        templateId = Ref.Identifier("pkgBase", "M:T"),
        argument = ValueUnit,
        signatories = parties,
        observers = noOne,
      )
      forEvery(nodeWithoutInterface) { mkNodeWithout =>
        forEvery(nodeWithInterface) { mkNodeWith =>
          val builder = newBuilder
          val exeId = builder.add(builder.exercise(contract, "Ch0", true, parties, ValueUnit))
          builder.add(mkNodeWithout(builder), exeId)
          builder.add(mkNodeWith(builder), exeId)
          engine.deps(builder.build()) shouldBe expected
        }
      }
    }
  }

}

object MetaDataTest {

  private[this] val langVersion = language.LanguageVersion.v1_15

  private def newBuilder = new TransactionBuilder(_ =>
    transaction.TransactionVersion.assignNodeVersion(langVersion)
  )

  private val engine = Engine.DevEngine()

  private[this] val emptyPkg = language.Ast.Package(Map.empty, Set.empty, langVersion, None)

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

  private val parties = Set[Ref.Party]("alice")
  private val noOne = Set.empty

}
