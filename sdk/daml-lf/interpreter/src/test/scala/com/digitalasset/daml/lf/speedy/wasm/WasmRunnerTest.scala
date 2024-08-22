// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package wasm

import com.daml.bazeltools.BazelRunfiles
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.Ast.{
  BLInt64,
  EBuiltinLit,
  FeatureFlags,
  Module,
  Package,
  PackageMetadata,
  Template,
}
import com.digitalasset.daml.lf.language.{LanguageVersion, PackageInterface}
import com.digitalasset.daml.lf.speedy.NoopAuthorizationChecker
import com.digitalasset.daml.lf.speedy.Speedy.UpdateMachine
import com.digitalasset.daml.lf.speedy.wasm.WasmRunner.WasmExpr
import com.digitalasset.daml.lf.transaction.{Node, TransactionVersion}
import com.digitalasset.daml.lf.value.Value.{ValueInt64, ValueParty, ValueRecord}
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger

import java.nio.file.{Files, Paths}
import scala.collection.immutable.VectorMap

class WasmRunnerTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with BeforeAndAfterAll {

  private val languages: Map[String, String] = Map("rust" -> "rs")
  private val mockLogger = mock[Logger]

  override def beforeAll(): Unit = {
    val _ = when(mockLogger.isInfoEnabled()).thenReturn(true)
  }

  "hello-world" in {
    val wasmModule = ByteString.readFrom(
      Files.newInputStream(
        BazelRunfiles.rlocation(
          Paths.get(s"daml-lf/interpreter/src/test/resources/hello-world.${languages("rust")}.wasm")
        )
      )
    )
    val submissionTime = Time.Timestamp.now()
    val initialSeeding = InitialSeeding.NoSeed
    val wasm = new WasmRunner(
      submitters = Set.empty,
      readAs = Set.empty,
      seeding = initialSeeding,
      submissionTime = submissionTime,
      authorizationChecker = NoopAuthorizationChecker,
      logger = createContextualizedLogger(mockLogger),
    )(LoggingContext.ForTesting)
    val wexpr = WasmExpr(wasmModule, "main")
    val result = wasm.evaluateWasmExpression(
      wexpr,
      ledgerTime = submissionTime,
    )

    inside(result) {
      case Right(
            UpdateMachine.Result(
              tx,
              locationInfo,
              nodeSeeds,
              globalKeyMapping,
              disclosedCreateEvents,
            )
          ) =>
        tx.nodes shouldBe empty
        tx.roots shouldBe empty
        tx.version shouldBe TransactionVersion.V31
        locationInfo shouldBe empty
        nodeSeeds shouldBe empty
        globalKeyMapping shouldBe empty
        disclosedCreateEvents shouldBe empty
        verify(mockLogger).info("hello-world")
    }
  }

  "create-contract" in {
    val wasmModule = ByteString.readFrom(
      Files.newInputStream(
        BazelRunfiles.rlocation(
          Paths.get(
            s"daml-lf/interpreter/src/test/resources/create-contract.${languages("rust")}.wasm"
          )
        )
      )
    )
    val submissionTime = Time.Timestamp.now()
    val hashRoot0 = crypto.Hash.assertFromString("deadbeef" * 8)
    val initialSeeding = InitialSeeding.RootNodeSeeds(ImmArray(Some(hashRoot0)))
    val submitters = Set.empty[Ref.Party] // FIXME:
    val stakeholders = Set.empty[Ref.Party] // FIXME:
    val pkgName = Ref.PackageName.assertFromString("package-1")
    val modName = Ref.ModuleName.assertFromString("create_contract")
    // TODO: following should be built using the (disassembled output of?) wasm code
    val pkgInterface = PackageInterface(
      Map(
        // TODO: this should be the wasm module/zip hash
        Ref.PackageId.assertFromString(
          "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833"
        ) -> Package(
          modules = Map(
            modName -> Module(
              name = modName,
              definitions = Map.empty,
              templates = Map(
                Ref.DottedName.assertFromString("SimpleTemplate") -> Template(
                  param = Ref.Name.assertFromString(
                    "_0"
                  ), // FIXME: template table func ptr to SimpleTemplate.new
                  precond = EBuiltinLit(
                    BLInt64(1)
                  ), // FIXME: template table func ptr to SimpleTemplate.precond
                  signatories = EBuiltinLit(
                    BLInt64(2)
                  ), // FIXME: template table func ptr to SimpleTemplate.signatories
                  choices = Map.empty,
                  observers = EBuiltinLit(
                    BLInt64(3)
                  ), // FIXME: template table func ptr to SimpleTemplate.observers
                  key = None,
                  implements = VectorMap.empty,
                )
              ),
              exceptions = Map.empty,
              interfaces = Map.empty,
              featureFlags = FeatureFlags.default,
            )
          ),
          directDeps = Set.empty,
          languageVersion = LanguageVersion.Features.default,
          metadata = PackageMetadata(
            name = pkgName,
            // TODO: this should be pulled from the Cargo.toml file?
            version = PackageVersion.assertFromString("0.1.0"),
            upgradedPackageId = None,
          ),
        )
      )
    )
    val wasm = new WasmRunner(
      submitters = submitters,
      readAs = stakeholders,
      seeding = initialSeeding,
      submissionTime = submissionTime,
      authorizationChecker = NoopAuthorizationChecker,
      logger = createContextualizedLogger(mockLogger),
      pkgInterface = pkgInterface,
    )(LoggingContext.ForTesting)
    val wexpr = WasmExpr(wasmModule, "main")
    val result = wasm.evaluateWasmExpression(
      wexpr,
      ledgerTime = submissionTime,
    )

    inside(result) {
      case Right(
            UpdateMachine.Result(
              tx,
              locationInfo,
              nodeSeeds,
              globalKeyMapping,
              disclosedCreateEvents,
            )
          ) =>
        tx.transaction.isWellFormed shouldBe Set.empty
        tx.roots.length shouldBe 1
        val nodeId = tx.roots.head
        inside(tx.nodes(nodeId)) {
          case Node.Create(
                contractId,
                `pkgName`,
                _,
                templateId,
                ValueRecord(None, fields),
                "",
                `submitters`,
                `stakeholders`,
                None,
                TransactionVersion.V31,
              ) =>
            verify(mockLogger).info(s"created contract with ID ${contractId.coid}")
            templateId shouldBe Ref.TypeConName.assertFromString(
              "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833:create_contract:SimpleTemplate.new"
            )
            fields.length shouldBe 2
            fields(0) shouldBe (None, ValueParty(Ref.Party.assertFromString("bob")))
            fields(1) shouldBe (None, ValueInt64(42L))
        }
        tx.version shouldBe TransactionVersion.V31
        locationInfo shouldBe empty
        nodeSeeds.length shouldBe 1
        nodeSeeds.head shouldBe (nodeId, hashRoot0)
        globalKeyMapping shouldBe empty
        disclosedCreateEvents shouldBe empty
    }
  }

  private def createContextualizedLogger(logger: Logger): ContextualizedLogger = {
    val method = classOf[ContextualizedLogger.type].getDeclaredMethod("createFor", classOf[Logger])
    method.setAccessible(true)
    method.invoke(ContextualizedLogger, logger).asInstanceOf[ContextualizedLogger]
  }
}
