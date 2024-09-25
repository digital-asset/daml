// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package wasm

import com.daml.bazeltools.BazelRunfiles
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.daml.lf.archive.{DarReader, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageVersion
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.language.Ast.{
  FeatureFlags,
  ModuleSignature,
  PackageMetadata,
  PackageSignature,
  TemplateSignature,
}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.NoopAuthorizationChecker
import com.digitalasset.daml.lf.speedy.Speedy.{ContractInfo, UpdateMachine}
import com.digitalasset.daml.lf.speedy.wasm.WasmRunner.WasmExpr
import com.digitalasset.daml.lf.transaction.{Node, NodeId, TransactionVersion, Versioned}
import com.digitalasset.daml.lf.value.{Value => LfValue}
import com.digitalasset.daml.lf.value.Value.{
  ContractId,
  ValueInt64,
  ValueParty,
  ValueRecord,
  VersionedContractInstance,
}
import com.google.protobuf.ByteString
import org.mockito.{Mockito, MockitoSugar}
import org.scalatest.{BeforeAndAfterEach, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger

import java.nio.file.{Files, Paths}
import scala.collection.immutable.VectorMap
import scala.concurrent.ExecutionContext.Implicits.global

class WasmRunnerTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with BeforeAndAfterEach {

  private val languages: Map[String, String] = Map("rust" -> "rs", "assemblyscript" -> "as")
  private val mockLogger = mock[Logger]
  private val darFile = Paths
    .get(
      BazelRunfiles.rlocation("daml-lf/interpreter/src/test/resources/data-interoperability.dar")
    )
    .toFile
  private val dar = DarReader.assertReadArchiveFromFile(darFile)
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

  // TODO: validate that host functions have their return data segments GCed

  for (target <- languages.keys) {
    s"$target hello-world" in {
      val wasmModule = ByteString.readFrom(
        Files.newInputStream(
          BazelRunfiles.rlocation(
            Paths.get(
              s"daml-lf/interpreter/src/test/resources/hello-world.${languages(target)}.wasm"
            )
          )
        )
      )
      val submissionTime = Time.Timestamp.now()
      val initialSeeding = InitialSeeding.NoSeed
      val wexpr = WasmExpr(wasmModule, "main")
      val wasm = WasmRunner(
        submitters = Set.empty,
        readAs = Set.empty,
        seeding = initialSeeding,
        submissionTime = submissionTime,
        authorizationChecker = NoopAuthorizationChecker,
        logger = createContextualizedLogger(mockLogger),
        activeContractStore = WasmRunnerTestLib.acs(),
        wasmExpr = wexpr,
        compiledPackages = compiledPackages,
      )(LoggingContext.ForTesting)

      getLocalContractStore(wasm) shouldBe empty

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
          tx.nodes shouldBe empty
          tx.roots shouldBe empty
          tx.version shouldBe TransactionVersion.V31
          locationInfo shouldBe empty
          nodeSeeds shouldBe empty
          globalKeyMapping shouldBe empty
          disclosedCreateEvents shouldBe empty
          verify(mockLogger).info("hello-world")
      }
      getLocalContractStore(wasm) shouldBe empty
    }
  }

  "rust create-contract" in {
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
    val bob = Ref.Party.assertFromString("bob")
    val submitters = Set(bob)
    val readAs = Set.empty[Ref.Party]
    val stakeholders = submitters ++ readAs
    val pkgName = Ref.PackageName.assertFromString("wasm_package")
    val wexpr = WasmExpr(wasmModule, "main")
    val wasm = WasmRunner(
      submitters = submitters,
      readAs = readAs,
      seeding = initialSeeding,
      submissionTime = submissionTime,
      authorizationChecker = DefaultAuthorizationChecker,
      logger = createContextualizedLogger(mockLogger),
      compiledPackages = compiledPackages,
      wasmExpr = wexpr,
      activeContractStore = WasmRunnerTestLib.acs(),
    )(LoggingContext.ForTesting)

    getLocalContractStore(wasm) shouldBe empty

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
        tx.roots.length shouldBe 1
        val nodeId = tx.roots.head
        inside(tx.nodes(nodeId)) {
          case Node.Create(
                contractId,
                `pkgName`,
                _,
                templateId,
                arg @ ValueRecord(None, fields),
                "",
                `submitters`,
                `stakeholders`,
                None,
                TransactionVersion.V31,
              ) =>
            verify(mockLogger).info(s"created contract with ID ${contractId.coid}")
            templateId shouldBe Ref.TypeConName.assertFromString(
              "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833:wasm_module:SimpleTemplate.new"
            )
            fields.length shouldBe 2
            fields(0) shouldBe (None, ValueParty(bob))
            fields(1) shouldBe (None, ValueInt64(42L))
            inside(getLocalContractStore(wasm).get(contractId)) { case Some(contractInfo) =>
              contractInfo.templateId shouldBe templateId
              contractInfo.arg shouldBe arg
            }
        }
        tx.version shouldBe TransactionVersion.V31
        locationInfo shouldBe empty
        nodeSeeds.length shouldBe 1
        nodeSeeds.head shouldBe (nodeId, hashRoot0)
        globalKeyMapping shouldBe empty
        disclosedCreateEvents shouldBe empty
    }
  }

  "rust fetch-global-contract" in {
    val wasmModule = ByteString.readFrom(
      Files.newInputStream(
        BazelRunfiles.rlocation(
          Paths.get(
            s"daml-lf/interpreter/src/test/resources/fetch-contract.${languages("rust")}.wasm"
          )
        )
      )
    )
    val submissionTime = Time.Timestamp.now()
    val hashRoot0 = crypto.Hash.assertFromString("deadbeef" * 8)
    val initialSeeding = InitialSeeding.RootNodeSeeds(ImmArray(Some(hashRoot0)))
    val contractId = ContractId.assertFromString(
      "0083d63f9d6c27eb34b37890d0f365c99505f32f06727fbefa2931f9d99d51f9ac"
    )
    val bob = Ref.Party.assertFromString("bob")
    val actingParties = Set(bob)
    val submitters = Set(bob)
    val readAs = Set.empty[Ref.Party]
    val stakeholders = submitters ++ readAs
    val pkgName = Ref.PackageName.assertFromString("wasm_package")
    // TODO: this should be pulled from the Cargo.toml file?
    val pkgVersion = PackageVersion.assertFromString("0.1.0")
    val templateId = Ref.TypeConName.assertFromString(
      "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833:wasm_module:SimpleTemplate.new"
    )
    val txVersion = TransactionVersion.V31
    val count = 42L
    val argV = ValueRecord(
      None,
      ImmArray(None -> ValueParty(Ref.Party.assertFromString(bob)), None -> ValueInt64(count)),
    )
    val contract =
      VersionedContractInstance(pkgName, Some(pkgVersion), templateId, Versioned(txVersion, argV))
    val wexpr = WasmExpr(wasmModule, "main")
    val wasm = WasmRunner(
      submitters = submitters,
      readAs = readAs,
      seeding = initialSeeding,
      submissionTime = submissionTime,
      authorizationChecker = DefaultAuthorizationChecker,
      logger = createContextualizedLogger(mockLogger),
      compiledPackages = compiledPackages,
      wasmExpr = wexpr,
      activeContractStore = WasmRunnerTestLib.acs({ case `contractId` =>
        contract
      }),
    )(LoggingContext.ForTesting)

    getLocalContractStore(wasm) shouldBe empty

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
        tx.roots.length shouldBe 1
        val nodeId = tx.roots.head
        inside(tx.nodes(nodeId)) {
          case Node.Fetch(
                `contractId`,
                `pkgName`,
                `templateId`,
                `actingParties`,
                `submitters`,
                `stakeholders`,
                None,
                false,
                `txVersion`,
              ) =>
            verify(mockLogger).info(
              s"contract ID ${contractId.coid} has argument record {fields {value {party: \"$bob\"}} fields {value {int64: $count}}}"
            )
        }
        tx.version shouldBe txVersion
        locationInfo shouldBe empty
        nodeSeeds.length shouldBe 0
        globalKeyMapping shouldBe empty
        disclosedCreateEvents shouldBe empty
    }
    getLocalContractStore(wasm) shouldBe empty
  }

  "rust fetch-local-contract" in {
    val wasmModule = ByteString.readFrom(
      Files.newInputStream(
        BazelRunfiles.rlocation(
          Paths.get(
            s"daml-lf/interpreter/src/test/resources/fetch-contract.${languages("rust")}.wasm"
          )
        )
      )
    )
    val submissionTime = Time.Timestamp.now()
    val hashRoot0 = crypto.Hash.assertFromString("deadbeef" * 8)
    val initialSeeding = InitialSeeding.RootNodeSeeds(ImmArray(Some(hashRoot0)))
    val contractId = ContractId.assertFromString(
      "0083d63f9d6c27eb34b37890d0f365c99505f32f06727fbefa2931f9d99d51f9ac"
    )
    val bob = Ref.Party.assertFromString("bob")
    val submitters = Set(bob)
    val readAs = Set.empty[Ref.Party]
    val stakeholders = submitters ++ readAs
    val pkgName = Ref.PackageName.assertFromString("wasm_package")
    // TODO: this should be pulled from the Cargo.toml file?
    val pkgVersion = PackageVersion.assertFromString("0.1.0")
    val templateId = Ref.TypeConName.assertFromString(
      "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833:wasm_module:SimpleTemplate.new"
    )
    val txVersion = TransactionVersion.V31
    val count = 42L
    val argV = ValueRecord(
      None,
      ImmArray(None -> ValueParty(Ref.Party.assertFromString(bob)), None -> ValueInt64(count)),
    )
    val contract =
      VersionedContractInstance(pkgName, Some(pkgVersion), templateId, Versioned(txVersion, argV))
    val contractInfo = ContractInfo(
      txVersion,
      pkgName,
      Some(pkgVersion),
      templateId,
      WasmRunner.toSValue(argV),
      submitters,
      stakeholders,
      None,
    )
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
      activeContractStore = WasmRunnerTestLib.acs({ case `contractId` =>
        contract
      }),
      initialLocalContractStore = Map(contractId -> contractInfo),
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
        tx.roots shouldBe empty
        tx.version shouldBe txVersion
        locationInfo shouldBe empty
        nodeSeeds.length shouldBe 0
        globalKeyMapping shouldBe empty
        disclosedCreateEvents shouldBe empty
    }
    getLocalContractStore(wasm) shouldBe Map(contractId -> contractInfo)
    verify(mockLogger).info(
      s"contract ID ${contractId.coid} has argument record {fields {value {party: \"$bob\"}} fields {value {int64: $count}}}"
    )
  }

  /*
      WasmRunner(ptx)         exercise-contract.wasm
      ---------------         ----------------------
           |
           +            -0->         main()                                                                      [Export function]
                                       |
           +            <-1-     exerciseChoice(templateId, contractId, choiceName, choiceArg, consuming = true) [Host function]
           |
    ptx.beginExercises
           |
SimpleTemplate_increment.exercise -2-> +                                                                         [Export function]
                                       |
                                 updatedContract
                                       |
           +            <-3-     createContract(templateId, updatedContract)                                     [Host function]
           |
    ptx.insertCreate
           |
        newCoId         -R3->          +
                                       |
           +            <-R2-      newCoId
           |
     ptx.endExercises
           |
        newCoId         -R1->          +
                                       |
           +            <-4-  logInfo("..$newCoId..)                                                             [Host function]
           |
logger.info("..$newCoId..") -R4->      +
                                       |
           +            <-R0-          +
           |
   */
  "rust exercise-choice" in {
    val wasmModule = ByteString.readFrom(
      Files.newInputStream(
        BazelRunfiles.rlocation(
          Paths.get(
            s"daml-lf/interpreter/src/test/resources/exercise-choice.${languages("rust")}.wasm"
          )
        )
      )
    )
    val submissionTime = Time.Timestamp.now()
    val node0 = NodeId(0)
    val node1 = NodeId(1)
    val hashNode0 = crypto.Hash.assertFromString("deadbeef" * 8)
    val hashNode1 = crypto.Hash.assertFromString(
      "9e3269759b30a85980bfae0e31d361783c42f01ff4e1912145586b27697d7ef6"
    )
    val initialSeeding = InitialSeeding.RootNodeSeeds(ImmArray(Some(hashNode0)))
    val contractId = ContractId.assertFromString(
      "0083d63f9d6c27eb34b37890d0f365c99505f32f06727fbefa2931f9d99d51f9ac"
    )
    val bob = Ref.Party.assertFromString("bob")
    val alice = Ref.Party.assertFromString("alice")
    val submitters = Set(bob)
    val readAs = Set.empty[Ref.Party]
    val stakeholders = submitters ++ readAs
    val pkgName = Ref.PackageName.assertFromString("wasm_package")
    // TODO: this should be pulled from the Cargo.toml file?
    val pkgVersion = PackageVersion.assertFromString("0.1.0")
    val templateId = Ref.TypeConName.assertFromString(
      "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833:wasm_module:SimpleTemplate.new"
    )
    val txVersion = TransactionVersion.V31
    val count = 42L
    val argV = ValueRecord(
      None,
      ImmArray(None -> ValueParty(Ref.Party.assertFromString(bob)), None -> ValueInt64(count)),
    )
    val contract =
      VersionedContractInstance(pkgName, Some(pkgVersion), templateId, Versioned(txVersion, argV))
    val contractInfo = ContractInfo(
      txVersion,
      pkgName,
      Some(pkgVersion),
      templateId,
      WasmRunner.toSValue(argV),
      submitters,
      stakeholders,
      None,
    )
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
      activeContractStore = WasmRunnerTestLib.acs({ case `contractId` =>
        contract
      }),
      initialLocalContractStore = Map(contractId -> contractInfo),
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
        tx.roots shouldBe ImmArray(node0)
        inside(tx.nodes(node0)) {
          case Node.Exercise(
                `contractId`,
                `pkgName`,
                `templateId`,
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
                `txVersion`,
              ) =>
            actingParties shouldBe Set(bob)
            stakeholders shouldBe Set(bob)
            signatories shouldBe Set(bob)
            choiceObservers shouldBe Set(alice)
            choiceAuthorizers shouldBe Set(bob)
            children shouldBe ImmArray(node1)
            choiceId shouldBe Ref.ChoiceName.assertFromString("SimpleTemplate_increment")
            tx.consumedContracts shouldBe Set(contractId)
            tx.inactiveContracts shouldBe Set(contractId)
            tx.inputContracts shouldBe Set(contractId)
            tx.consumedBy shouldBe Map(contractId -> node0)
            inside(exerciseResult) { case LfValue.ValueContractId(newContractId) =>
              inside(tx.nodes(node1)) {
                case Node.Create(
                      `newContractId`,
                      `pkgName`,
                      _,
                      templateId,
                      arg @ ValueRecord(None, fields),
                      "",
                      `submitters`,
                      `stakeholders`,
                      None,
                      TransactionVersion.V31,
                    ) =>
                  verify(mockLogger).info(
                    s"result of exercising choice SimpleTemplate_increment on contract ID ${contractId.coid} is ${newContractId.coid}"
                  )
                  templateId shouldBe Ref.TypeConName.assertFromString(
                    "cae3f9de0ee19fa89d4b65439865e1942a3a98b50c86156c3ab1b09e8266c833:wasm_module:SimpleTemplate.new"
                  )
                  fields.length shouldBe 2
                  fields(0) shouldBe (None, ValueParty(Ref.Party.assertFromString("bob")))
                  fields(1) shouldBe (None, ValueInt64(43L))
                  tx.localContracts.keySet shouldBe Set(newContractId)
                  inside(getLocalContractStore(wasm).get(newContractId)) {
                    case Some(contractInfo) =>
                      contractInfo.templateId shouldBe templateId
                      contractInfo.arg shouldBe arg
                  }
              }
            }
        }
        tx.version shouldBe TransactionVersion.V31
        locationInfo shouldBe empty
        nodeSeeds shouldBe ImmArray(node0 -> hashNode0, node1 -> hashNode1)
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
