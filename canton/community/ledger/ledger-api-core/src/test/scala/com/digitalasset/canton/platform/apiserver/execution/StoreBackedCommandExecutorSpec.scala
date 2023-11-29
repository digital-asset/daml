// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.lf.command.{ApiCommands as LfCommands, DisclosedContract as LfDisclosedContract}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{Identifier, ParticipantId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.engine.{
  Engine,
  Result,
  ResultDone,
  ResultInterruption,
  ResultNeedUpgradeVerification,
}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{
  GlobalKeyWithMaintainers,
  SubmittedTransaction,
  Transaction,
  Versioned,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractInstance, ValueTrue}
import com.daml.logging.LoggingContext
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{CryptoPureApi, Salt, SaltSeed}
import com.digitalasset.canton.ledger.api.domain.{CommandId, Commands, LedgerId}
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.api.{DeduplicationPeriod, domain}
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.participant.state.index.v2.{
  ContractState,
  ContractStore,
  IndexPackagesService,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.ErrorCause.InterpretationTimeExceeded
import com.digitalasset.canton.protocol.{DriverContractMetadata, LfContractId, LfTransactionVersion}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfValue}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class StoreBackedCommandExecutorSpec
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with BaseTest {

  val cryptoApi: CryptoPureApi = new SymbolicPureCrypto()
  val salt: Bytes = DriverContractMetadata(
    Salt.tryDeriveSalt(SaltSeed.generate()(cryptoApi), 0, cryptoApi)
  ).toLfBytes(ProtocolVersion.dev)
  val identifier: Identifier =
    Ref.Identifier(Ref.PackageId.assertFromString("p"), Ref.QualifiedName.assertFromString("m:n"))

  private val processedDisclosedContracts = ImmArray(
  )

  private val emptyTransactionMetadata = Transaction.Metadata(
    submissionSeed = None,
    submissionTime = Time.Timestamp.now(),
    usedPackages = Set.empty,
    dependsOnTime = false,
    nodeSeeds = ImmArray.Empty,
    globalKeyMapping = Map.empty,
    disclosedEvents = processedDisclosedContracts,
  )

  private val resultDone: ResultDone[(SubmittedTransaction, Transaction.Metadata)] =
    ResultDone[(SubmittedTransaction, Transaction.Metadata)](
      (TransactionBuilder.EmptySubmitted, emptyTransactionMetadata)
    )

  private def mkMockEngine(result: Result[(SubmittedTransaction, Transaction.Metadata)]): Engine = {
    val mockEngine = mock[Engine]
    when(
      mockEngine.submit(
        submitters = any[Set[Ref.Party]],
        readAs = any[Set[Ref.Party]],
        cmds = any[com.daml.lf.command.ApiCommands],
        participantId = any[ParticipantId],
        submissionSeed = any[Hash],
        disclosures = any[ImmArray[LfDisclosedContract]],
      )(any[LoggingContext])
    )
      .thenReturn(result)
  }

  private def mkCommands(ledgerEffectiveTime: Time.Timestamp) =
    Commands(
      ledgerId = Some(LedgerId("ledgerId")),
      workflowId = None,
      applicationId = Ref.ApplicationId.assertFromString("applicationId"),
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
      disclosedContracts = ImmArray.empty,
    )

  private val submissionSeed = Hash.hashPrivateKey("a key")
  private val configuration = Configuration(
    generation = 1,
    timeModel = LedgerTimeModel(
      avgTransactionLatency = Duration.ZERO,
      minSkew = Duration.ZERO,
      maxSkew = Duration.ZERO,
    ).get,
    maxDeduplicationDuration = Duration.ZERO,
  )

  private def mkSut(tolerance: NonNegativeFiniteDuration, engine: Engine) =
    new StoreBackedCommandExecutor(
      engine,
      Ref.ParticipantId.assertFromString("anId"),
      mock[IndexPackagesService],
      mock[ContractStore],
      AuthorityResolver(),
      authenticateContract = _ => Right(()),
      metrics = Metrics.ForTesting,
      loggerFactory = loggerFactory,
      dynParamGetter = new TestDynamicDomainParameterGetter(tolerance),
      TimeProvider.UTC,
    )

  private def mkInterruptedResult(
      nbSteps: Int
  ): Result[(SubmittedTransaction, Transaction.Metadata)] =
    ResultInterruption { () =>
      Threading.sleep(100)
      if (nbSteps == 0) resultDone
      else mkInterruptedResult(nbSteps - 1)
    }

  "StoreBackedCommandExecutor" should {
    "add interpretation time and used disclosed contracts to result" in {
      val mockEngine = mkMockEngine(resultDone)
      val commands = mkCommands(Time.Timestamp.Epoch)

      val sut = mkSut(NonNegativeFiniteDuration.Zero, mockEngine)

      sut
        .execute(commands, submissionSeed, configuration)(
          LoggingContextWithTrace(loggerFactory)
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
        .execute(commands, submissionSeed, configuration)(
          LoggingContextWithTrace(loggerFactory)
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
        .execute(commands, submissionSeed, configuration)(
          LoggingContextWithTrace(loggerFactory)
        )
        .map {
          case Left(InterpretationTimeExceeded(`let`, `tolerance`)) => succeed
          case _ => fail()
        }
    }
  }

  "Upgrade Verification" should {

    val stakeholderContractId: LfContractId = LfContractId.assertFromString("00" + "00" * 32 + "03")
    val stakeholderContract = ContractState.Active(
      contractInstance =
        Versioned(LfTransactionVersion.maxVersion, ContractInstance(identifier, Value.ValueTrue)),
      ledgerEffectiveTime = Timestamp.now(),
      stakeholders = Set(Ref.Party.assertFromString("unexpectedSig")),
      signatories = Set(Ref.Party.assertFromString("unexpectedSig")),
      agreementText = None,
      globalKey = None,
      maintainers = None,
      // Filled below conditionally
      driverMetadata = None,
    )

    val divulgedContractId: LfContractId = LfContractId.assertFromString("00" + "00" * 32 + "00")

    val archivedContractId: LfContractId = LfContractId.assertFromString("00" + "00" * 32 + "01")

    val disclosedContractId: LfContractId = LfContractId.assertFromString("00" + "00" * 32 + "02")

    val disclosedContract: domain.DisclosedContract = domain.UpgradableDisclosedContract(
      templateId = identifier,
      contractId = disclosedContractId,
      argument = ValueTrue,
      createdAt = mock[Timestamp],
      keyHash = None,
      driverMetadata = salt,
      signatories = Set(Ref.Party.assertFromString("unexpectedSig")),
      stakeholders = Set(
        Ref.Party.assertFromString("unexpectedSig"),
        Ref.Party.assertFromString("unexpectedObs"),
      ),
      keyMaintainers = Some(Set(Ref.Party.assertFromString("unexpectedSig"))),
      keyValue = Some(LfValue.ValueTrue),
    )

    def doTest(
        contractId: Option[LfContractId],
        expected: Option[Option[String]],
        authenticationResult: Either[String, Unit] = Right(()),
        stakeholderContractDriverMetadata: Option[Array[Byte]] = Some(salt.toByteArray),
        upgradableDisclosedContract: Boolean = true,
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
                  shared = true,
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
          submitters = any[Set[Party]],
          readAs = any[Set[Party]],
          cmds = any[LfCommands],
          participantId = any[ParticipantId],
          submissionSeed = any[Hash],
          disclosures = any[ImmArray[LfDisclosedContract]],
        )(any[LoggingContext])
      ).thenReturn(engineResult)

      val commands = Commands(
        ledgerId = Some(LedgerId("ledgerId")),
        workflowId = None,
        applicationId = Ref.ApplicationId.assertFromString("applicationId"),
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
      )
      val submissionSeed = Hash.hashPrivateKey("a key")
      val configuration = Configuration(
        generation = 1,
        timeModel = LedgerTimeModel(
          avgTransactionLatency = Duration.ZERO,
          minSkew = Duration.ZERO,
          maxSkew = Duration.ZERO,
        ).get,
        maxDeduplicationDuration = Duration.ZERO,
      )

      val store = mock[ContractStore]
      when(
        store.lookupContractStateWithoutDivulgence(any[LfContractId])(any[LoggingContextWithTrace])
      ).thenReturn(Future.successful(ContractState.NotFound))
      when(
        store.lookupContractStateWithoutDivulgence(same(stakeholderContractId))(
          any[LoggingContextWithTrace]
        )
      ).thenReturn(
        Future.successful(
          stakeholderContract.copy(driverMetadata = stakeholderContractDriverMetadata)
        )
      )
      when(
        store.lookupContractStateWithoutDivulgence(same(archivedContractId))(
          any[LoggingContextWithTrace]
        )
      ).thenReturn(Future.successful(ContractState.Archived))

      val sut = new StoreBackedCommandExecutor(
        mockEngine,
        Ref.ParticipantId.assertFromString("anId"),
        mock[IndexPackagesService],
        store,
        AuthorityResolver(),
        authenticateContract = _ => authenticationResult,
        metrics = Metrics.ForTesting,
        loggerFactory = loggerFactory,
        dynParamGetter = new TestDynamicDomainParameterGetter(NonNegativeFiniteDuration.Zero),
        TimeProvider.UTC,
      )

      val commandsWithUpgradableDisclosedContracts = commands
      val commandsWithDeprecatedDisclosedContracts = commands.copy(disclosedContracts =
        ImmArray(
          domain.NonUpgradableDisclosedContract(
            templateId = identifier,
            contractId = disclosedContractId,
            argument = ValueTrue,
            createdAt = mock[Timestamp],
            keyHash = None,
            driverMetadata = salt,
          )
        )
      )
      sut
        .execute(
          commands =
            if (upgradableDisclosedContract) commandsWithUpgradableDisclosedContracts
            else commandsWithDeprecatedDisclosedContracts,
          submissionSeed = submissionSeed,
          ledgerConfiguration = configuration,
        )(LoggingContextWithTrace(loggerFactory))
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
            s"Contract with $divulgedContractId was not found or it refers to a divulged contract."
          )
        ),
      )
    }

    "disallow archived contracts" in {
      doTest(Some(archivedContractId), Some(Some("Contract archived")))
    }

    "disallow unauthorized disclosed contracts" in {
      val expected =
        s"Upgrading contract with $disclosedContractId failed authentication check with error: Not authorized. The following upgrading checks failed: ['signatories mismatch: Set(unexpectedSig) vs Set(signatory)', 'observers mismatch: Set(unexpectedObs) vs Set(observer)', 'key maintainers mismatch: Set(unexpectedSig) vs Set(signatory)', 'key value mismatch: Some(GlobalKey(p:m:n, ValueBool(true))) vs Some(GlobalKey(p:m:n, ValueRecord(None,ImmArray((None,ValueParty(signatory)),(None,ValueText(some key))))))']"
      doTest(
        Some(disclosedContractId),
        Some(Some(expected)),
        authenticationResult = Left("Not authorized"),
      )
    }

    "disallow unauthorized stakeholder contracts" in {
      val errorMessage = "Not authorized"
      val expected =
        s"Upgrading contract with $stakeholderContractId failed authentication check with error: Not authorized. The following upgrading checks failed: ['signatories mismatch: Set(unexpectedSig) vs Set(signatory)', 'observers mismatch: Set() vs Set(observer)', 'key maintainers mismatch: Set() vs Set(signatory)', 'key value mismatch: None vs Some(GlobalKey(p:m:n, ValueRecord(None,ImmArray((None,ValueParty(signatory)),(None,ValueText(some key))))))']"
      doTest(
        Some(stakeholderContractId),
        Some(Some(expected)),
        authenticationResult = Left(errorMessage),
      )
    }

    "fail upgrade on stakeholder contracts without contract driver metadata" in {
      val errorMessage = "Doesn't matter"
      val expected =
        s"Contract with $stakeholderContractId is missing the driver metadata and cannot be upgraded. This can happen for contracts created with older Canton versions"
      doTest(
        Some(stakeholderContractId),
        Some(Some(expected)),
        authenticationResult = Left(errorMessage),
        stakeholderContractDriverMetadata = None,
      )
    }

    "disallow upgrading deprecated disclosed contract formats" in {
      doTest(
        Some(disclosedContractId),
        Some(
          Some(
            s"Contract with $disclosedContractId was provided via the deprecated DisclosedContract create_argument_blob field and cannot be upgraded. Use the create_argument_payload instead and retry the submission"
          )
        ),
        upgradableDisclosedContract = false,
      )
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
