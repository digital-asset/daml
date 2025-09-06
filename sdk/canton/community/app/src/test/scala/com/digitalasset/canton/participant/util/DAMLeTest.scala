// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.store.{ContractAndKeyLookup, ExtendedContractLookup}
import com.digitalasset.canton.participant.util.DAMLe.PackageResolver
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.ContractAuthenticators.ContractAuthenticatorFn
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.{LegacyContractHash, TestEngine}
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasActorSystem,
  HasExecutionContext,
  LfCommand,
  LfPartyId,
}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.engine
import org.scalatest.wordspec.AsyncWordSpec

class DAMLeTest
    extends AsyncWordSpec
    with BaseTest
    with HasActorSystem
    with HasExecutionContext
    with FailOnShutdown {

  def cantonTestsPath: String = CantonTestsPath
  def testDarFileName: String = "CantonTests"

  "DAMLe" should {

    val testEngine =
      new TestEngine(packagePaths = Seq(CantonExamplesPath), iterationsBetweenInterruptions = 10)

    val packageResolver: PackageResolver =
      packageId => _ => FutureUnlessShutdown.pure(testEngine.packageStore.getPackage(packageId))

    val damlE = new DAMLe(
      packageResolver,
      DAMLe.newEngine(
        enableLfDev = false,
        enableLfBeta = false,
        enableStackTraces = false,
        iterationsBetweenInterruptions = 10,
        paranoidMode = false,
      ),
      EngineLoggingConfig(),
      loggerFactory,
    )

    val alice = LfPartyId.assertFromString("Alice")

    def reinterpret(
        command: LfCommand,
        rootSeed: LfHash,
        submitters: Set[LfPartyId],
        contracts: ContractAndKeyLookup = new ExtendedContractLookup(Map.empty, Map.empty),
        contractAuthenticator: ContractAuthenticatorFn = (_, _) => Either.unit,
        getEngineAbortStatus: GetEngineAbortStatus = () => EngineAbortStatus.notAborted,
    ): EitherT[FutureUnlessShutdown, DAMLe.ReinterpretationError, DAMLe.ReInterpretationResult] =
      damlE
        .reinterpret(
          contracts = contracts,
          contractAuthenticator = contractAuthenticator,
          submitters = submitters,
          command = command,
          ledgerTime = CantonTimestamp.now(),
          preparationTime = CantonTimestamp.now(),
          rootSeed = Some(rootSeed),
          packageResolution = Map.empty,
          expectFailure = false,
          getEngineAbortStatus = getEngineAbortStatus,
        )

    def createCycleContract(): (LfNodeCreate, LfHash, GenContractInstance) = {
      val (createTx, createMeta) =
        testEngine.submitAndConsume(new Cycle("id", alice).create().commands.loneElement, alice)
      val createNode = createTx.nodes.values.collect { case c: LfNodeCreate => c }.loneElement
      val (_, createSeed) = createMeta.nodeSeeds.toList.loneElement
      val contract = ExampleContractFactory.fromCreate(createNode)
      (createNode, createSeed, contract)
    }

    def replayCreateCommand(): (LfCommand, LfHash, Set[LfPartyId]) = {
      val (createNode, createSeed, _) = createCycleContract()
      val (replayCreate, submitters) = testEngine.replayCommand(createNode)
      (replayCreate, createSeed, submitters)
    }

    def replayExecuteCommand(contract: NewContractInstance): (LfHash, LfCommand, Set[Party]) = {
      val contractId = new Cycle.ContractId(contract.contractId.coid)
      val (tx, meta) = testEngine.submitAndConsume(
        command = contractId.exerciseRepeat().commands().loneElement,
        actAs = contract.signatories.head,
        storedContracts = Seq(contract.inst),
      )
      val rootId = tx.roots.toSeq.loneElement
      val exerciseNode = tx.nodes.get(rootId).value
      val exerciseSeed = meta.nodeSeeds.toSeq.toMap.get(rootId).value
      val (replayExercise, submitters) = testEngine.replayCommand(exerciseNode)
      (exerciseSeed, replayExercise, submitters)
    }

    "reinterpret create commands" in {

      val (replayCreate, createSeed, submitters) = replayCreateCommand()

      reinterpret(
        submitters = submitters,
        command = replayCreate,
        rootSeed = createSeed,
      ).value
        .map(inside(_) { case Right(_) =>
          succeed
        })

    }

    "reinterpret exercise commands" in {

      val (_, _, contract) = createCycleContract()

      val (exerciseSeed, replayExercise, submitters) = replayExecuteCommand(contract)

      reinterpret(
        submitters = submitters,
        command = replayExercise,
        rootSeed = exerciseSeed,
        contracts = new ExtendedContractLookup(Map(contract.contractId -> contract), Map.empty),
      ).value
        .map(inside(_) { case Right(_) =>
          succeed
        })
    }

    "fail when execution is aborted" in {
      val (replayCreate, createSeed, submitters) = replayCreateCommand()

      val expected = "test execution aborted"

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        reinterpret(
          submitters = submitters,
          command = replayCreate,
          rootSeed = createSeed,
          getEngineAbortStatus = () => EngineAbortStatus(Some(expected)),
        ).swap.map(error => error shouldBe DAMLe.EngineAborted(expected)),
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.warningMessage should include(s"Aborting engine computation, reason = $expected"),
              "engine gets aborted",
            )
          )
        ),
      )
    }

    "authenticate contracts" in {

      val (_, _, contract) = createCycleContract()

      val (exerciseSeed, replayExercise, submitters) = replayExecuteCommand(contract)

      val inst = contract.inst
      val contractHash = LegacyContractHash.fatContractHash(inst).value

      reinterpret(
        submitters = submitters,
        command = replayExercise,
        rootSeed = exerciseSeed,
        contracts = new ExtendedContractLookup(Map(contract.contractId -> contract), Map.empty),
        contractAuthenticator = {
          case (`inst`, `contractHash`) => Either.unit
          case other => fail(s"Unexpected: $other")
        },
      ).value
        .map(inside(_) { case Right(_) =>
          succeed
        })

    }

    "fail if authentication fails" in {

      val (_, _, contract) = createCycleContract()

      val (exerciseSeed, replayExercise, submitters) = replayExecuteCommand(contract)

      val inst = contract.inst
      val contractHash = LegacyContractHash.fatContractHash(inst).value

      reinterpret(
        submitters = submitters,
        command = replayExercise,
        rootSeed = exerciseSeed,
        contracts = new ExtendedContractLookup(Map(contract.contractId -> contract), Map.empty),
        contractAuthenticator = {
          case (`inst`, `contractHash`) => Left("Authentication failed")
          case other => fail(s"Unexpected: $other")
        },
      ).value.map { actual =>
        inside(actual) {
          // TODO(#23876) - improve error matching once non-Dev errors are used
          case Left(
                DAMLe.EngineError(
                  engine.Error.Interpretation(engine.Error.Interpretation.DamlException(e), _)
                )
              ) =>
            succeed
        }
      }
    }
  }
}
