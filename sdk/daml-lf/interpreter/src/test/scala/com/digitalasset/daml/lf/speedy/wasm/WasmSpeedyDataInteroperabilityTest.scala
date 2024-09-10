// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package wasm

import com.daml.bazeltools.BazelRunfiles
import com.digitalasset.daml.lf.PureCompiledPackages
import com.digitalasset.daml.lf.language.Ast.{
  FeatureFlags,
  ModuleSignature,
  PackageMetadata,
  PackageSignature,
  TemplateSignature,
}
import com.digitalasset.daml.lf.language.LanguageVersion

import scala.collection.immutable.VectorMap
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.daml.lf.archive.{DarReader, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.speedy.Speedy.{ContractInfo, UpdateMachine}
import com.digitalasset.daml.lf.speedy.wasm.WasmRunner.WasmExpr
import com.digitalasset.daml.lf.transaction.{Node, NodeId, TransactionVersion}
import com.digitalasset.daml.lf.value.{Value => LfValue}
import com.digitalasset.daml.lf.value.Value.{ValueInt64, ValueParty, ValueRecord}
import com.google.protobuf.ByteString
import org.mockito.{Mockito, MockitoSugar}
import org.scalatest.{BeforeAndAfterEach, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger

import java.nio.file.{Files, Paths}
import scala.concurrent.ExecutionContext.Implicits.global

class WasmSpeedyDataInteroperabilityTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with BeforeAndAfterEach {

  private val languages: Map[String, String] = Map("rust" -> "rs")
  private val mockLogger = mock[Logger]
  private val darFile = Paths
    .get(
      BazelRunfiles.rlocation("daml-lf/interpreter/src/test/resources/data-interoperability.dar")
    )
    .toFile
  private val Right(dar) = DarReader.readArchiveFromFile(darFile)
  private val config = Compiler.Config.Dev(LanguageMajorVersion.V2)
  private val wasmModuleName = Ref.ModuleName.assertFromString("wasm_module")
  private val wasmPkgName = Ref.PackageName.assertFromString("wasm_package")
  private val wasmCompiledPackages = Map(
    Ref.PackageId.assertFromString(
      "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833"
    ) -> PackageSignature(
      modules = Map(
        wasmModuleName -> ModuleSignature(
          name = wasmModuleName,
          definitions = Map.empty,
          templates = Map(
            Ref.DottedName.assertFromString("SimpleTemplate") -> TemplateSignature(
              param = Ref.Name.assertFromString("new"),
              precond = (),
              signatories = (),
              choices = Map.empty,
              observers = (),
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
        name = wasmPkgName,
        // TODO: this should be pulled from the Cargo.toml file?
        version = PackageVersion.assertFromString("0.1.0"),
        upgradedPackageId = None,
      ),
    )
  )
  private val damlCompiledPackages = PureCompiledPackages.assertBuild(
    dar.all.map(Decode.assertDecodeArchivePayload(_)).toMap,
    config,
  )
  private val compiledPackages = PureCompiledPackages(
    packages = damlCompiledPackages.signatures ++ wasmCompiledPackages,
    definitions = damlCompiledPackages.definitions,
    compilerConfig = config,
  )
  override def beforeEach(): Unit = {
    val _ = when(mockLogger.isInfoEnabled()).thenReturn(true)
  }

  override def afterEach(): Unit = {
    Mockito.reset(mockLogger)
  }

  "WASM to Speedy data-interoperability" in {
    val wasmModule = ByteString.readFrom(
      Files.newInputStream(
        BazelRunfiles.rlocation(
          Paths.get(
            s"daml-lf/interpreter/src/test/resources/data-interoperability.${languages("rust")}.wasm"
          )
        )
      )
    )
    val submissionTime = Time.Timestamp.now()
    val node0 = NodeId(0)
    val node1 = NodeId(1)
    val node2 = NodeId(2)
    val node3 = NodeId(3)
    val node4 = NodeId(4)
    val hashNode0 = crypto.Hash.assertFromString("deadbeef" * 8)
    val hashNode1 = crypto.Hash.assertFromString(
      "9e3269759b30a85980bfae0e31d361783c42f01ff4e1912145586b27697d7ef6"
    )
    val hashNode2 = crypto.Hash.assertFromString(
      "e7669b49f7142433617fa9ce4790bbcf5e8211dc5756daf63a276f5fe6071d5f"
    )
    val hashNode3 = crypto.Hash.assertFromString(
      "e7669b49f7142433617fa9ce4790bbcf5e8211dc5756daf63a276f5fe6071d5f"
    )
    val hashNode4 = crypto.Hash.assertFromString(
      "cf6a1589dc9caec85374e13123c4cbcbf05bb98209184b095e709c6c210ba308"
    )
    val initialSeeding =
      InitialSeeding.RootNodeSeeds(ImmArray(Some(hashNode0), Some(hashNode1), Some(hashNode2)))
    val bob = Ref.Party.assertFromString("bob")
    val alice = Ref.Party.assertFromString("alice")
    val submitters = Set(bob)
    val readAs = Set.empty[Ref.Party]
    val stakeholders = submitters ++ readAs
    val damlPkgName = Ref.PackageName.assertFromString("data-interoperability")
    val wasmPkgName = Ref.PackageName.assertFromString("wasm_package")
    val damlTemplateId = Ref.TypeConName.assertFromString(
      "feadceaea7984045371ed7f47f4343319e6cd76332682789125a547979118412:SimpleTemplate:SimpleTemplate"
    )
    val wasmTemplateId = Ref.TypeConName.assertFromString(
      "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833:wasm_module:SimpleTemplate.new"
    )
    val txVersion31 = TransactionVersion.V31
    val txVersionDev = TransactionVersion.VDev
    val wexpr = WasmExpr(wasmModule, "main")
    val wasm = WasmRunner(
      submitters = submitters,
      readAs = stakeholders,
      seeding = initialSeeding,
      submissionTime = submissionTime,
      authorizationChecker = DefaultAuthorizationChecker,
      logger = createContextualizedLogger(mockLogger),
      compiledPackages = compiledPackages,
      wasmExpr = wexpr,
      activeContractStore = WasmRunnerTestLib.acs(),
    )(LoggingContext.ForTesting)

    val result = wasm.evaluateWasmExpression()

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
        tx.roots shouldBe ImmArray(node0, node1, node3)
        inside(tx.nodes(node0)) {
          case Node.Create(
                contractId0,
                `damlPkgName`,
                _,
                `damlTemplateId`,
                ValueRecord(None, fields),
                "",
                `submitters`,
                `stakeholders`,
                None,
                `txVersionDev`,
              ) =>
            verify(mockLogger).info(
              s"Daml contract creation with ID ${contractId0.coid}"
            )
            fields.length shouldBe 2
            fields(0) shouldBe (None, ValueParty(Ref.Party.assertFromString("bob")))
            fields(1) shouldBe (None, ValueInt64(42L))
        }

        inside(tx.nodes(node1)) {
          case Node.Exercise(
                contractId1,
                `damlPkgName`,
                `damlTemplateId`,
                None,
                choiceId,
                true,
                actingParties,
                LfValue.ValueUnit,
                stakeholders,
                signatories,
                choiceObservers,
                None,
                children,
                Some(exerciseResult),
                None,
                false,
                `txVersionDev`,
              ) =>
            actingParties shouldBe Set(bob)
            stakeholders shouldBe Set(bob)
            signatories shouldBe Set(bob)
            choiceObservers shouldBe Set.empty
            children shouldBe ImmArray(node2)
            choiceId shouldBe Ref.ChoiceName.assertFromString("SimpleTemplate_increment")
            tx.consumedContracts should contain(contractId1)
            tx.inactiveContracts should contain(contractId1)
            tx.inputContracts shouldBe Set.empty
            tx.consumedBy should contain(contractId1 -> node1)
            inside(exerciseResult) { case LfValue.ValueContractId(newContractId) =>
              inside(tx.nodes(node2)) {
                case Node.Create(
                      `newContractId`,
                      `damlPkgName`,
                      _,
                      `damlTemplateId`,
                      arg @ ValueRecord(None, fields),
                      "",
                      `submitters`,
                      `stakeholders`,
                      None,
                      `txVersionDev`,
                    ) =>
                  verify(mockLogger).info(
                    s"result of exercising Daml choice SimpleTemplate_increment on contract ID ${contractId1.coid} is ${newContractId.coid}"
                  )
                  fields.length shouldBe 2
                  fields(0) shouldBe (None, ValueParty(Ref.Party.assertFromString("bob")))
                  fields(1) shouldBe (None, ValueInt64(43L))
                  tx.localContracts.keySet should contain(newContractId)
                  inside(getLocalContractStore(wasm).get(newContractId)) {
                    case Some(contractInfo) =>
                      contractInfo.templateId shouldBe damlTemplateId
                      contractInfo.arg shouldBe arg
                  }
              }
            }
        }

        inside(tx.nodes(node3)) {
          case Node.Exercise(
                contractId3,
                `wasmPkgName`,
                `wasmTemplateId`,
                None,
                choiceId,
                true,
                actingParties,
                LfValue.ValueUnit,
                stakeholders,
                signatories,
                choiceObservers,
                Some(choiceAuthorizers),
                children,
                Some(exerciseResult),
                None,
                false,
                `txVersion31`,
              ) =>
            actingParties shouldBe Set(bob)
            stakeholders shouldBe Set(bob)
            signatories shouldBe Set(bob)
            choiceObservers shouldBe Set(alice)
            choiceAuthorizers shouldBe Set(bob)
            children shouldBe ImmArray(node4)
            choiceId shouldBe Ref.ChoiceName.assertFromString("SimpleTemplate_increment")
            tx.consumedContracts should contain(contractId3)
            tx.inactiveContracts should contain(contractId3)
            tx.inputContracts shouldBe Set.empty
            tx.consumedBy should contain(contractId3 -> node3)
            inside(exerciseResult) { case LfValue.ValueContractId(newContractId) =>
              inside(tx.nodes(node4)) {
                case Node.Create(
                      `newContractId`,
                      `wasmPkgName`,
                      _,
                      `wasmTemplateId`,
                      arg @ ValueRecord(None, fields),
                      "",
                      `submitters`,
                      `stakeholders`,
                      None,
                      `txVersion31`,
                    ) =>
                  verify(mockLogger).info(
                    s"result of exercising WASM choice SimpleTemplate_increment on contract ID ${contractId3.coid} is ${newContractId.coid}"
                  )
                  fields.length shouldBe 2
                  fields(0) shouldBe (None, ValueParty(Ref.Party.assertFromString("bob")))
                  fields(1) shouldBe (None, ValueInt64(44L))
                  tx.localContracts.keySet should contain(newContractId)
                  inside(getLocalContractStore(wasm).get(newContractId)) {
                    case Some(contractInfo) =>
                      contractInfo.templateId shouldBe wasmTemplateId
                      contractInfo.arg shouldBe arg
                  }
              }
            }
        }
        tx.version shouldBe txVersionDev
        locationInfo shouldBe empty
        nodeSeeds shouldBe ImmArray(
          node0 -> hashNode0,
          node1 -> hashNode1,
          node2 -> hashNode2,
          node3 -> hashNode3,
          node4 -> hashNode4,
        )
        globalKeyMapping shouldBe empty
        disclosedCreateEvents shouldBe empty
    }
  }

  private def createContextualizedLogger(logger: Logger): ContextualizedLogger = {
    val method = classOf[ContextualizedLogger.type].getDeclaredMethod("createFor", classOf[Logger])
    method.setAccessible(true)
    method.invoke(ContextualizedLogger, logger).asInstanceOf[ContextualizedLogger]
  }

  private def getLocalContractStore(
      runner: WasmRunner
  ): Map[LfValue.ContractId, ContractInfo] = {
    runner.incompleteTransaction._1
  }
}
