// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.syntax.either.*
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.ledger.api.Commands
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.participant.state.index.{ContractState, ContractStore}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.execution.ContractAuthenticators.ContractAuthenticatorFn
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.InterpretationTimeExceeded
import com.digitalasset.canton.platform.config.CommandServiceConfig
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TestEngine
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfPartyId, LfValue}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{CreationTime, Node as LfNode}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import monocle.Monocle.toAppliedFocusOps
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import scala.concurrent.Future

class StoreBackedCommandInterpreterSpec
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with HasExecutionContext
    with FailOnShutdown
    with BaseTest {

  private val testEngine =
    new TestEngine(packagePaths = Seq(CantonExamplesPath), iterationsBetweenInterruptions = 10)
  private val alice = LfPartyId.assertFromString("Alice")

  private val createCycleApiCommand: Commands =
    testEngine.validateCommand(new Cycle("id", alice).create().commands.loneElement, alice)

  private def repeatCycleApiCommand(cid: ContractId): Commands =
    testEngine.validateCommand(
      new Cycle.ContractId(cid.coid).exerciseRepeat().commands().loneElement,
      alice,
    )

  private def createCycleContract() = {
    val (createTx, createMeta) =
      testEngine.submitAndConsume(new Cycle("id", alice).create().commands.loneElement, alice)
    val createNode = createTx.nodes.values.collect { case c: LfNodeCreate => c }.loneElement
    val (_, createSeed) = createMeta.nodeSeeds.toList.loneElement
    val contract = ExampleContractFactory.fromCreate(createNode)
    (createNode, createSeed, contract)
  }

  private val salt: Bytes = ContractAuthenticationDataV1(TestSalt.generateSalt(36))(
    AuthenticatedContractIdVersionV11
  ).toLfBytes
  private val identifier: Identifier =
    Ref.Identifier(Ref.PackageId.assertFromString("p"), Ref.QualifiedName.assertFromString("m:n"))
  private val packageName: PackageName = PackageName.assertFromString("pkg-name")
  private val disclosedContractId: LfContractId = TransactionBuilder.newCid
  private def mkCreateNode(contractId: Value.ContractId = disclosedContractId) =
    LfNode.Create(
      coid = contractId,
      packageName = packageName,
      templateId = identifier,
      arg = Value.ValueTrue,
      signatories = Set(Ref.Party.assertFromString("unexpectedSig")),
      stakeholders = Set(
        Ref.Party.assertFromString("unexpectedSig"),
        Ref.Party.assertFromString("unexpectedObs"),
      ),
      keyOpt = Some(
        KeyWithMaintainers.assertBuild(
          templateId = identifier,
          LfValue.ValueTrue,
          Set(Ref.Party.assertFromString("unexpectedSig")),
          packageName,
        )
      ),
      version = LfTransactionVersion.StableVersions.max,
    )
  private val disclosedCreateNode = mkCreateNode()
  private val disclosedContractCreateTime = Time.Timestamp.now()

  private val processedDisclosedContracts = ImmArray(
    FatContract.fromCreateNode(
      create = disclosedCreateNode,
      createTime = CreationTime.CreatedAt(disclosedContractCreateTime),
      authenticationData = salt,
    )
  )

  private val submissionSeed = Hash.hashPrivateKey("a key")

  private def mkSut(
      engine: Engine,
      contractStore: ContractStore = mock[ContractStore],
      contractAuthenticator: ContractAuthenticatorFn = (_, _) => Left("Not authorized"),
      tolerance: NonNegativeFiniteDuration = NonNegativeFiniteDuration.tryOfSeconds(60),
  ) =
    new StoreBackedCommandInterpreter(
      engine = engine,
      participant = Ref.ParticipantId.assertFromString("anId"),
      packageResolver = testEngine.packageResolver,
      contractStore = contractStore,
      contractAuthenticator = contractAuthenticator,
      metrics = LedgerApiServerMetrics.ForTesting,
      config = EngineLoggingConfig(),
      prefetchingRecursionLevel = CommandServiceConfig.DefaultContractPrefetchingDepth,
      loggerFactory = loggerFactory,
      dynParamGetter = new TestDynamicSynchronizerParameterGetter(tolerance),
      timeProvider = TimeProvider.UTC,
    )

  "StoreBackedCommandExecutor" should {
    "add interpretation time and used disclosed contracts to result" in {

      val sut = mkSut(testEngine.engine, tolerance = NonNegativeFiniteDuration.Zero)

      sut
        .interpret(createCycleApiCommand, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map { actual =>
          actual.foreach { actualResult =>
            actualResult.interpretationTimeNanos should be > 0L
            actualResult.processedDisclosedContracts shouldBe processedDisclosedContracts
          }
          succeed
        }
    }

    "interpret successfully if time limit is not exceeded" in {
      val tolerance = NonNegativeFiniteDuration.tryOfSeconds(60)
      val sut = mkSut(testEngine.engine, tolerance = tolerance)

      val commands =
        createCycleApiCommand.focus(_.commands.ledgerEffectiveTime).replace(Time.Timestamp.now())
      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Right(_) => succeed
          case other => fail(s"Did not expect: $other")
        }
    }

    "abort interpretation when time limit is exceeded" in {
      val tolerance = NonNegativeFiniteDuration.tryOfSeconds(10)
      val let = Time.Timestamp.now().subtract(Duration.ofSeconds(20))
      val commands = createCycleApiCommand.focus(_.commands.ledgerEffectiveTime).replace(let)
      val sut = mkSut(testEngine.engine, tolerance = tolerance)
      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Left(InterpretationTimeExceeded(`let`, `tolerance`, _)) => succeed
          case other => fail(s"Did not expect: $other")
        }
    }
  }

  "Disclosed contract synchronizer id consideration" should {
    val synchronizerId1 = SynchronizerId.tryFromString("x::synchronizer1")
    val synchronizerId2 = SynchronizerId.tryFromString("x::synchronizer2")
    val disclosedContractId1 = TransactionBuilder.newCid
    val disclosedContractId2 = TransactionBuilder.newCid

    implicit val traceContext: TraceContext = TraceContext.empty

    "not influence the prescribed synchronizer id if no disclosed contracts are attached" in {
      val result = for {
        synchronizerId_from_no_prescribed_no_disclosed <- StoreBackedCommandInterpreter
          .considerDisclosedContractsSynchronizerId(
            prescribedSynchronizerIdO = None,
            disclosedContractsUsedInInterpretation = ImmArray.empty,
            logger,
          )
        synchronizerId_from_prescribed_no_disclosed <-
          StoreBackedCommandInterpreter.considerDisclosedContractsSynchronizerId(
            prescribedSynchronizerIdO = Some(synchronizerId1),
            disclosedContractsUsedInInterpretation = ImmArray.empty,
            logger,
          )
      } yield {
        synchronizerId_from_no_prescribed_no_disclosed shouldBe None
        synchronizerId_from_prescribed_no_disclosed shouldBe Some(synchronizerId1)
      }

      result.value
    }

    "use the disclosed contracts synchronizer id" in {
      StoreBackedCommandInterpreter
        .considerDisclosedContractsSynchronizerId(
          prescribedSynchronizerIdO = None,
          disclosedContractsUsedInInterpretation = ImmArray(
            disclosedContractId1 -> Some(synchronizerId1),
            disclosedContractId2 -> Some(synchronizerId1),
          ),
          logger,
        )
        .map(_ shouldBe Some(synchronizerId1))
        .value
    }

    "return an error if synchronizer ids of disclosed contracts mismatch" in {
      def test(prescribedSynchronizerIdO: Option[SynchronizerId]) =
        inside(
          StoreBackedCommandInterpreter
            .considerDisclosedContractsSynchronizerId(
              prescribedSynchronizerIdO = prescribedSynchronizerIdO,
              disclosedContractsUsedInInterpretation = ImmArray(
                disclosedContractId1 -> Some(synchronizerId1),
                disclosedContractId2 -> Some(synchronizerId2),
              ),
              logger,
            )
        ) { case Left(error: ErrorCause.DisclosedContractsSynchronizerIdsMismatch) =>
          error.mismatchingDisclosedContractSynchronizerIds shouldBe Map(
            disclosedContractId1 -> synchronizerId1,
            disclosedContractId2 -> synchronizerId2,
          )
        }

      test(prescribedSynchronizerIdO = None)
      test(prescribedSynchronizerIdO = Some(SynchronizerId.tryFromString("x::anotherOne")))
    }

    "return an error if the synchronizer id of the disclosed contracts does not match the prescribed synchronizer id" in {
      val synchronizerIdOfDisclosedContracts = synchronizerId1
      val prescribedSynchronizerId = synchronizerId2

      inside(
        StoreBackedCommandInterpreter
          .considerDisclosedContractsSynchronizerId(
            prescribedSynchronizerIdO = Some(prescribedSynchronizerId),
            disclosedContractsUsedInInterpretation = ImmArray(
              disclosedContractId1 -> Some(synchronizerIdOfDisclosedContracts),
              disclosedContractId2 -> Some(synchronizerIdOfDisclosedContracts),
            ),
            logger,
          )
      ) { case Left(error: ErrorCause.PrescribedSynchronizerIdMismatch) =>
        error.commandsSynchronizerId shouldBe prescribedSynchronizerId
        error.synchronizerIdOfDisclosedContracts shouldBe synchronizerIdOfDisclosedContracts
        error.disclosedContractIds shouldBe Set(disclosedContractId1, disclosedContractId2)
      }
    }
  }

  "Contract provision" should {

    s"fail if invalid contract id prefix is used" in {

      val contractStore = mock[ContractStore]

      val invalidCid = ExampleContractFactory.buildContractId().mapCid {
        case Value.ContractId.V1(d, _) =>
          Value.ContractId.V1(d, Bytes.fromByteString(ByteString.copyFrom("invalid".getBytes)))
        case other => fail(s"Unexpected: $other")
      }

      when(
        contractStore.lookupContractState(
          contractId = any[ContractId]
        )(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(ContractState.NotFound)) // prefetch only

      val commands = repeatCycleApiCommand(invalidCid)

      val sut = mkSut(testEngine.engine, contractStore = contractStore)

      // TODO(#27344) - This should not throw an exception but return a failure error cause
      loggerFactory.suppressErrors {
        sut
          .interpret(commands, submissionSeed)(
            LoggingContextWithTrace(loggerFactory),
            executionContext,
          )
          .failed
          .map { _ =>
            succeed
          }
      }

    }

    "complete if contract authentication passes" in {

      val (_, _, contract) = createCycleContract()
      val inst: LfFatContractInst = contract.inst

      val contractStore = mock[ContractStore]

      when(
        contractStore.lookupContractState(
          contractId = any[ContractId]
        )(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(ContractState.NotFound)) // prefetch only

      when(
        contractStore.lookupActiveContract(
          readers = any[Set[Ref.Party]],
          contractId = eqTo(inst.contractId),
        )(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(Some(inst)))

      val commands = repeatCycleApiCommand(inst.contractId)

      val sut = mkSut(
        testEngine.engine,
        contractStore = contractStore,
        contractAuthenticator = (_, _) => Either.unit,
      )

      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Right(_) => succeed
          case other => fail(s"Expected success, got $other")
        }

    }

    "error if contract authentication fails" in {

      val (_, _, contract) = createCycleContract()
      val inst: LfFatContractInst = contract.inst

      val contractStore = mock[ContractStore]

      when(
        contractStore.lookupContractState(
          contractId = any[ContractId]
        )(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(ContractState.NotFound)) // prefetch only

      when(
        contractStore.lookupActiveContract(
          readers = any[Set[Ref.Party]],
          contractId = eqTo(inst.contractId),
        )(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(Some(inst)))

      val commands = repeatCycleApiCommand(inst.contractId)

      val sut = mkSut(
        testEngine.engine,
        contractStore = contractStore,
        contractAuthenticator = (_, _) => Left("Not authorized"),
      )

      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Left(ErrorCause.DamlLf(engine.Error.Interpretation(_, _))) => succeed
          case other => fail(s"Did not expect: $other")
        }

    }
  }

}
