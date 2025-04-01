// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import cats.syntax.either.*
import com.daml.logging.LoggingContext
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{CryptoPureApi, Salt, SaltSeed}
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.{CommandId, Commands, DisclosedContract}
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.ledger.participant.state.index.{ContractState, ContractStore}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.PackageName
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.platform.apiserver.services.ErrorCause
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.InterpretationTimeExceeded
import com.digitalasset.canton.protocol.{DriverContractMetadata, LfContractId, LfTransactionVersion}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfValue}
import com.digitalasset.daml.lf.command.{ApiCommands as LfCommands, ApiContractKey}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, ParticipantId, Party}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.*
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKeyWithMaintainers,
  Node as LfNode,
  SubmittedTransaction,
  Transaction,
  Versioned,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ContractInstance, ValueTrue}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class StoreBackedCommandInterpreterSpec
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with HasExecutionContext
    with FailOnShutdown
    with BaseTest {

  val cryptoApi: CryptoPureApi = new SymbolicPureCrypto()
  val salt: Bytes = DriverContractMetadata(
    Salt.tryDeriveSalt(SaltSeed.generate()(cryptoApi), 0, cryptoApi)
  ).toLfBytes(ProtocolVersion.dev)
  val identifier: Identifier =
    Ref.Identifier(Ref.PackageId.assertFromString("p"), Ref.QualifiedName.assertFromString("m:n"))
  val packageName: PackageName = PackageName.assertFromString("pkg-name")
  private val disclosedContractId: LfContractId = TransactionBuilder.newCid
  private def mkCreateNode(contractId: Value.ContractId = disclosedContractId) =
    LfNode.Create(
      coid = contractId,
      packageName = packageName,
      packageVersion = None,
      templateId = identifier,
      arg = ValueTrue,
      signatories = Set(Ref.Party.assertFromString("unexpectedSig")),
      stakeholders = Set(
        Ref.Party.assertFromString("unexpectedSig"),
        Ref.Party.assertFromString("unexpectedObs"),
      ),
      keyOpt = Some(
        GlobalKeyWithMaintainers.assertBuild(
          templateId = identifier,
          LfValue.ValueTrue,
          Set(Ref.Party.assertFromString("unexpectedSig")),
          packageName,
        )
      ),
      version = LfTransactionVersion.StableVersions.max,
    )
  private val disclosedCreateNode = mkCreateNode()
  private val disclosedContractSynchronizerId: SynchronizerId =
    SynchronizerId.tryFromString("x::synchronizerId")
  private val disclosedContractCreateTime = Time.Timestamp.now()

  private val disclosedContract = DisclosedContract(
    fatContractInstance = FatContractInstance.fromCreateNode(
      disclosedCreateNode,
      createTime = disclosedContractCreateTime,
      cantonData = salt,
    ),
    synchronizerIdO = Some(disclosedContractSynchronizerId),
  )

  private val processedDisclosedContracts = ImmArray(
    FatContractInstance.fromCreateNode(
      create = disclosedCreateNode,
      createTime = disclosedContractCreateTime,
      cantonData = salt,
    )
  )

  private val emptyTransactionMetadata = Transaction.Metadata(
    submissionSeed = None,
    submissionTime = Time.Timestamp.now(),
    usedPackages = Set.empty,
    timeBoundaries = Time.Range.unconstrained,
    nodeSeeds = ImmArray.Empty,
    globalKeyMapping = Map.empty,
    disclosedEvents = ImmArray.empty,
  )
  private val resultDone: ResultDone[(SubmittedTransaction, Transaction.Metadata)] =
    ResultDone[(SubmittedTransaction, Transaction.Metadata)](
      (TransactionBuilder.EmptySubmitted, emptyTransactionMetadata)
    )

  private def mkMockEngine(result: Result[(SubmittedTransaction, Transaction.Metadata)]): Engine = {
    val mockEngine = mock[Engine]
    when(
      mockEngine.submit(
        packageMap = any[Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]],
        packagePreference = any[Set[Ref.PackageId]],
        submitters = any[Set[Ref.Party]],
        readAs = any[Set[Ref.Party]],
        cmds = any[com.digitalasset.daml.lf.command.ApiCommands],
        disclosures = any[ImmArray[FatContractInstance]],
        participantId = any[ParticipantId],
        submissionSeed = any[Hash],
        prefetchKeys = any[Seq[ApiContractKey]],
        engineLogger = any[Option[EngineLogger]],
      )(any[LoggingContext])
    )
      .thenReturn(result)
  }

  private def mkCommands(
      ledgerEffectiveTime: Time.Timestamp,
      disclosedContracts: ImmArray[DisclosedContract] = ImmArray(disclosedContract),
      synchronizerIdO: Option[SynchronizerId] = None,
  ) =
    Commands(
      workflowId = None,
      userId = Ref.UserId.assertFromString("userId"),
      commandId = CommandId(Ref.CommandId.assertFromString("commandId")),
      submissionId = None,
      actAs = Set.empty,
      readAs = Set.empty,
      submittedAt = Time.Timestamp.Epoch,
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      commands = LfCommands(
        commands = ImmArray.Empty,
        ledgerEffectiveTime = ledgerEffectiveTime,
        commandsReference = "",
      ),
      disclosedContracts = disclosedContracts,
      synchronizerId = synchronizerIdO,
      prefetchKeys = Seq.empty,
    )

  private val submissionSeed = Hash.hashPrivateKey("a key")

  private def mkSut(tolerance: NonNegativeFiniteDuration, engine: Engine) =
    new StoreBackedCommandInterpreter(
      engine = engine,
      participant = Ref.ParticipantId.assertFromString("anId"),
      packageSyncService = mock[SyncService],
      contractStore = mock[ContractStore],
      authenticateSerializableContract = _ => Either.unit,
      metrics = LedgerApiServerMetrics.ForTesting,
      config = EngineLoggingConfig(),
      loggerFactory = loggerFactory,
      dynParamGetter = new TestDynamicSynchronizerParameterGetter(tolerance),
      timeProvider = TimeProvider.UTC,
    )

  private def mkInterruptedResult(
      nbSteps: Int
  ): Result[(SubmittedTransaction, Transaction.Metadata)] =
    ResultInterruption(
      { () =>
        Threading.sleep(100)
        if (nbSteps == 0) resultDone
        else mkInterruptedResult(nbSteps - 1)
      },
      () => None,
    )

  "StoreBackedCommandExecutor" should {
    "add interpretation time and used disclosed contracts to result" in {
      val mockEngine = mkMockEngine(resultDone.map { case (tx, meta) =>
        tx -> meta.copy(disclosedEvents = ImmArray(disclosedCreateNode))
      })
      val commands = mkCommands(Time.Timestamp.Epoch)

      val sut = mkSut(NonNegativeFiniteDuration.Zero, mockEngine)

      sut
        .interpret(commands, submissionSeed)(
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
      val result = mkInterruptedResult(10)
      val mockEngine = mkMockEngine(result)
      val tolerance = NonNegativeFiniteDuration.tryOfSeconds(60)

      val sut = mkSut(tolerance, mockEngine)

      val let = Time.Timestamp.now()
      val commands = mkCommands(let)

      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Right(_) => succeed
          case _ => fail()
        }
    }

    "abort interpretation when time limit is exceeded" in {
      val result = mkInterruptedResult(10)
      val mockEngine = mkMockEngine(result)
      val tolerance = NonNegativeFiniteDuration.tryOfMillis(500)

      val sut = mkSut(tolerance, mockEngine)

      val let = Time.Timestamp.now()
      val commands = mkCommands(let)

      sut
        .interpret(commands, submissionSeed)(
          LoggingContextWithTrace(loggerFactory),
          executionContext,
        )
        .map {
          case Left(InterpretationTimeExceeded(`let`, `tolerance`, _)) => succeed
          case _ => fail()
        }
    }
  }

  "Upgrade Verification" should {
    val stakeholderContractId: LfContractId = LfContractId.assertFromString("00" + "00" * 32 + "03")
    val stakeholderContract = ContractState.Active(
      contractInstance = Versioned(
        LfTransactionVersion.minVersion,
        ContractInstance(packageName = packageName, template = identifier, arg = Value.ValueTrue),
      ),
      ledgerEffectiveTime = Timestamp.now(),
      stakeholders = Set(Ref.Party.assertFromString("unexpectedSig")),
      signatories = Set(Ref.Party.assertFromString("unexpectedSig")),
      globalKey = None,
      maintainers = None,
      // Filled below conditionally
      driverMetadata = Array.empty,
    )

    val divulgedContractId: LfContractId = LfContractId.assertFromString("00" + "00" * 32 + "00")

    val archivedContractId: LfContractId = LfContractId.assertFromString("00" + "00" * 32 + "01")

    def doTest(
        contractId: Option[LfContractId],
        expected: Option[Option[String]],
        authenticationResult: Either[String, Unit] = Either.unit,
        stakeholderContractDriverMetadata: Array[Byte] = salt.toByteArray,
    ): Future[Assertion] = {
      val ref: AtomicReference[Option[Option[String]]] = new AtomicReference(None)
      val mockEngine = mock[Engine]

      val engineResult = contractId match {
        case None =>
          resultDone
        case Some(coid) =>
          val signatory = Ref.Party.assertFromString("signatory")
          ResultNeedUpgradeVerification[(SubmittedTransaction, Transaction.Metadata)](
            coid = coid,
            signatories = Set(signatory),
            observers = Set(Ref.Party.assertFromString("observer")),
            keyOpt = Some(
              GlobalKeyWithMaintainers
                .assertBuild(
                  identifier,
                  someContractKey(signatory, "some key"),
                  Set(signatory),
                  packageName,
                )
            ),
            resume = verdict => {
              ref.set(Some(verdict))
              resultDone
            },
          )
      }

      when(
        mockEngine.submit(
          packageMap = any[Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)]],
          packagePreference = any[Set[Ref.PackageId]],
          submitters = any[Set[Ref.Party]],
          readAs = any[Set[Ref.Party]],
          cmds = any[com.digitalasset.daml.lf.command.ApiCommands],
          disclosures = any[ImmArray[FatContractInstance]],
          participantId = any[ParticipantId],
          submissionSeed = any[Hash],
          prefetchKeys = any[Seq[ApiContractKey]],
          engineLogger = any[Option[EngineLogger]],
        )(any[LoggingContext])
      ).thenReturn(engineResult)

      val commands = Commands(
        workflowId = None,
        userId = Ref.UserId.assertFromString("userId"),
        commandId = CommandId(Ref.CommandId.assertFromString("commandId")),
        submissionId = None,
        actAs = Set.empty,
        readAs = Set.empty,
        submittedAt = Time.Timestamp.Epoch,
        deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
        commands = LfCommands(
          commands = ImmArray.Empty,
          ledgerEffectiveTime = Time.Timestamp.Epoch,
          commandsReference = "",
        ),
        disclosedContracts = ImmArray.from(Seq(disclosedContract)),
        synchronizerId = None,
        prefetchKeys = Seq.empty,
      )
      val submissionSeed = Hash.hashPrivateKey("a key")

      val store = mock[ContractStore]
      when(
        store.lookupContractState(any[LfContractId])(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(ContractState.NotFound))
      when(
        store.lookupContractState(same(stakeholderContractId))(
          any[LoggingContextWithTrace]
        )
      ).thenReturn(
        Future.successful(
          stakeholderContract.copy(driverMetadata = stakeholderContractDriverMetadata)
        )
      )
      when(
        store.lookupContractState(same(archivedContractId))(
          any[LoggingContextWithTrace]
        )
      ).thenReturn(Future.successful(ContractState.Archived))

      val sut = new StoreBackedCommandInterpreter(
        mockEngine,
        Ref.ParticipantId.assertFromString("anId"),
        mock[SyncService],
        store,
        metrics = LedgerApiServerMetrics.ForTesting,
        authenticateSerializableContract = _ => authenticationResult,
        EngineLoggingConfig(),
        loggerFactory = loggerFactory,
        dynParamGetter = new TestDynamicSynchronizerParameterGetter(NonNegativeFiniteDuration.Zero),
        TimeProvider.UTC,
      )

      val commandsWithDisclosedContracts = commands
      sut
        .interpret(
          commands = commandsWithDisclosedContracts,
          submissionSeed = submissionSeed,
        )(LoggingContextWithTrace(loggerFactory), executionContext)
        .map(_ => ref.get() shouldBe expected)
    }

    "work with non-upgraded contracts" in {
      doTest(None, None)
    }

    "allow valid stakeholder contracts" in {
      doTest(Some(stakeholderContractId), Some(None))
    }

    "allow valid disclosed contracts" in {
      doTest(Some(disclosedContractId), Some(None))
    }

    "disallow divulged contracts" in {
      doTest(
        Some(divulgedContractId),
        Some(
          Some(
            s"Contract with $divulgedContractId was not found."
          )
        ),
      )
    }

    "disallow archived contracts" in {
      doTest(
        Some(archivedContractId),
        Some(
          Some(
            s"Contract with $archivedContractId was not found."
          )
        ),
      )
    }

    "disallow unauthorized disclosed contracts" in {
      val expected =
        s"Upgrading contract with ContractId(${disclosedContractId.coid}) failed authentication check with error: Not authorized. The following upgrading checks failed: ['signatories mismatch: TreeSet(unexpectedSig) vs Set(signatory)', 'observers mismatch: TreeSet(unexpectedObs) vs Set(observer)', 'key maintainers mismatch: TreeSet(unexpectedSig) vs Set(signatory)', 'key value mismatch: Some(GlobalKey(p:m:n, pkg-name, ValueBool(true))) vs Some(GlobalKey(p:m:n, pkg-name, ValueRecord(None,ImmArray((None,ValueParty(signatory)),(None,ValueText(some key))))))']"
      doTest(
        Some(disclosedContractId),
        Some(Some(expected)),
        authenticationResult = Left("Not authorized"),
      )
    }

    "disallow unauthorized stakeholder contracts" in {
      val errorMessage = "Not authorized"
      val expected =
        s"Upgrading contract with ContractId(${stakeholderContractId.coid}) failed authentication check with error: Not authorized. The following upgrading checks failed: ['signatories mismatch: Set(unexpectedSig) vs Set(signatory)', 'observers mismatch: Set() vs Set(observer)', 'key maintainers mismatch: Set() vs Set(signatory)', 'key value mismatch: None vs Some(GlobalKey(p:m:n, pkg-name, ValueRecord(None,ImmArray((None,ValueParty(signatory)),(None,ValueText(some key))))))']"
      doTest(
        Some(stakeholderContractId),
        Some(Some(expected)),
        authenticationResult = Left(errorMessage),
      )
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

  protected final def someContractKey(party: Party, value: String): LfValue.ValueRecord =
    LfValue.ValueRecord(
      None,
      ImmArray(
        None -> LfValue.ValueParty(party),
        None -> LfValue.ValueText(value),
      ),
    )
}
