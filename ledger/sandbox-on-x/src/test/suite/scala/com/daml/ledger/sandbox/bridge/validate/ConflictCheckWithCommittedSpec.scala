// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import java.time.Duration
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{ContractState, IndexService, MaximumLedgerTime}
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.validate.ConflictCheckWithCommittedSpec._
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission
import com.daml.lf.command.{EngineEnrichedContractMetadata, ProcessedDisclosedContract}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.transaction._
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ValueTrue, VersionedContractInstance}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.FixtureContext
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class ConflictCheckWithCommittedSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with IntegrationPatience
    with ArgumentMatchersSugar {

  behavior of classOf[ConflictCheckWithCommittedImpl].getSimpleName

  it should "validate causal monotonicity, key usages and disclosed contracts" in new TestContext {
    conflictCheckWithCommitted(input).futureValue shouldBe input
  }

  it should "pass causal monotonicity check if referred contracts is empty" in new TestContext {
    val submissionWithEmptyReferredContracts: PreparedTransactionSubmission =
      preparedTransactionSubmission.copy(inputContracts = inputContracts -- referredContracts)

    private val validationResult = conflictCheckWithCommitted(
      Right(offset -> submissionWithEmptyReferredContracts)
    ).futureValue

    verify(indexServiceMock, never)
      .lookupMaximumLedgerTimeAfterInterpretation(any[Set[ContractId]])(any[LoggingContext])
    validationResult shouldBe Right(offset -> submissionWithEmptyReferredContracts)
  }

  it should "handle causal monotonicity violation" in new TestContext {
    val lateTxLet: Timestamp = contractMaxLedgerTime.subtract(Duration.ofSeconds(1L))
    val nonCausalTxSubmission: PreparedTransactionSubmission =
      preparedTransactionSubmission.copy(submission =
        txSubmission.copy(transactionMeta = transactionMeta.copy(ledgerEffectiveTime = lateTxLet))
      )

    private val validationResult =
      conflictCheckWithCommitted(Right(offset -> nonCausalTxSubmission)).futureValue

    validationResult match {
      case Left(CausalMonotonicityViolation(actualContractMaxLedgerTime, actualTxLet)) =>
        actualContractMaxLedgerTime shouldBe contractMaxLedgerTime
        actualTxLet shouldBe lateTxLet
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "handle missing contracts" in new TestContext {
    val missingContracts: Set[ContractId] = referredContracts
    when(
      indexServiceMock.lookupMaximumLedgerTimeAfterInterpretation(referredContracts)(loggingContext)
    )
      .thenReturn(Future.successful(MaximumLedgerTime.Archived(missingContracts)))
    private val validationResult =
      conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(UnknownContracts(actualMissingContracts)) =>
        actualMissingContracts shouldBe missingContracts
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "handle a generic lookupMaximumLedgerTime error" in new TestContext {
    val someInternalError = new RuntimeException("oh")

    when(
      indexServiceMock.lookupMaximumLedgerTimeAfterInterpretation(referredContracts)(loggingContext)
    )
      .thenReturn(Future.failed(someInternalError))
    private val validationResult = conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(LedgerBridgeInternalError(actualInternalError, _)) =>
        actualInternalError shouldBe someInternalError
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "handle an inconsistent contract key (on key active input)" in new TestContext {
    when(indexServiceMock.lookupContractKey(informeesSet, activeKey)(loggingContext))
      .thenReturn(Future.successful(None))
    private val validationResult =
      conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(InconsistentContractKey(Some(actualInputContract), None)) =>
        actualInputContract shouldBe inputContract
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "handle an inconsistent contract key (on negative lookup input)" in new TestContext {
    val existingContractForKey: ContractId = cid(1337)

    when(indexServiceMock.lookupContractKey(informeesSet, nonExistingKey)(loggingContext))
      .thenReturn(Future.successful(Some(existingContractForKey)))
    private val validationResult =
      conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(InconsistentContractKey(None, Some(actualExistingContractForKey))) =>
        actualExistingContractForKey shouldBe existingContractForKey
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "handle a duplicate contract key" in new TestContext {
    val existingContractForKey: ContractId = cid(1337)

    when(indexServiceMock.lookupContractKey(informeesSet, keyCreated)(loggingContext))
      .thenReturn(Future.successful(Some(existingContractForKey)))
    private val validationResult = conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(DuplicateKey(actualDuplicateKey)) => actualDuplicateKey shouldBe keyCreated
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "fail validation mismatching let in disclosed contract" in new TestContext {
    when(
      indexServiceMock.lookupContractStateWithoutDivulgence(eqTo(disclosedContract.contractId))(
        any[LoggingContext]
      )
    )
      .thenReturn(
        Future.successful(
          ContractState.Active(
            VersionedContractInstance(
              templateId,
              Versioned(TransactionVersion.VDev, disclosedContract.argument),
            ),
            disclosedContract.metadata.createdAt.add(Duration.ofSeconds(1000L)),
          )
        )
      )

    private val validationResult = conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(DisclosedContractInvalid(contractId, _)) =>
        contractId shouldBe disclosedContract.contractId
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "fail validation mismatching contract argument in disclosed contract" in new TestContext {
    when(
      indexServiceMock.lookupContractStateWithoutDivulgence(eqTo(disclosedContract.contractId))(
        any[LoggingContext]
      )
    )
      .thenReturn(
        Future.successful(
          ContractState.Active(
            VersionedContractInstance(templateId, Versioned(TransactionVersion.VDev, ValueTrue)),
            disclosedContract.metadata.createdAt,
          )
        )
      )

    private val validationResult = conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(DisclosedContractInvalid(contractId, _)) =>
        contractId shouldBe disclosedContract.contractId
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "fail validation mismatching template id in disclosed contract" in new TestContext {
    when(
      indexServiceMock.lookupContractStateWithoutDivulgence(eqTo(disclosedContract.contractId))(
        any[LoggingContext]
      )
    )
      .thenReturn(
        Future.successful(
          ContractState.Active(
            VersionedContractInstance(
              templateId.copy(packageId = Ref.PackageId.assertFromString("anotherPackageId")),
              Versioned(TransactionVersion.VDev, disclosedContract.argument),
            ),
            disclosedContract.metadata.createdAt,
          )
        )
      )

    private val validationResult = conflictCheckWithCommitted(input).futureValue

    validationResult match {
      case Left(DisclosedContractInvalid(contractId, _)) =>
        contractId shouldBe disclosedContract.contractId
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  it should "fail validation on invalid contract driver metadata" in new TestContext {
    private val submissionWithInvalidContractDriverMetadata =
      preparedTransactionSubmission.copy(submission =
        preparedTransactionSubmission.submission.copy(disclosedContracts =
          ImmArray(
            Versioned(
              TransactionVersion.VDev,
              disclosedContract.copy(metadata =
                disclosedContract.metadata.copy(driverMetadata = Bytes.Empty)
              ),
            )
          )
        )
      )

    private val validationResult = conflictCheckWithCommitted(
      Right(offset -> submissionWithInvalidContractDriverMetadata)
    ).futureValue

    validationResult match {
      case Left(DisclosedContractInvalid(contractId, _)) =>
        contractId shouldBe disclosedContract.contractId
      case failure => fail(s"Expectation mismatch: got $failure")
    }
  }

  private class TestContext extends FixtureContext {
    implicit val loggingContext: LoggingContext =
      LoggingContext.ForTesting
    implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
      DamlContextualizedErrorLogger.forTesting(getClass)

    val indexServiceMock: IndexService = mock[IndexService]

    val conflictCheckWithCommitted: ConflictCheckWithCommittedImpl =
      new ConflictCheckWithCommittedImpl(
        indexService = indexServiceMock,
        bridgeMetrics = new BridgeMetrics(Metrics.ForTesting.dropwizardFactory),
      )(scala.concurrent.ExecutionContext.global)

    val inputContract: ContractId = cid(1)
    val anotherInputContract: ContractId = cid(2)
    val divulgedContract: ContractId = cid(3)

    val activeKey: GlobalKey = contractKey(1L)
    val keyCreated: GlobalKey = contractKey(2L)
    val nonExistingKey: GlobalKey = contractKey(3L)

    val inputContracts = Set(inputContract, anotherInputContract, divulgedContract)
    val referredContracts = Set(inputContract, anotherInputContract)

    val keyInputs = Map(
      activeKey -> Transaction.KeyActive(inputContract),
      keyCreated -> Transaction.KeyCreate,
      nonExistingKey -> Transaction.NegativeKeyLookup,
    )

    val submitterInfo: SubmitterInfo = dummySubmitterInfo

    val txLedgerEffectiveTime: Timestamp = Time.Timestamp.now()

    val transactionMeta: TransactionMeta = dummyTransactionMeta(txLedgerEffectiveTime)

    val transactionInformees @ Seq(informee1, _) = Seq("p-1", "p-2").map(Ref.Party.assertFromString)
    val informeesSet: Set[Ref.Party] = transactionInformees.toSet
    val blindingInfo: BlindingInfo = BlindingInfo(Map(), Map(divulgedContract -> Set(informee1)))

    val disclosedContract: ProcessedDisclosedContract = {
      val contractId = cid(1)
      ProcessedDisclosedContract(
        templateId = templateId,
        contractId = contractId,
        argument = Value.ValueText("Some contract value"),
        metadata = EngineEnrichedContractMetadata(
          createdAt = Time.Timestamp.now(),
          driverMetadata = contractId.toBytes,
          signatories = Set.empty,
          stakeholders = Set.empty,
          maybeKeyWithMaintainers = None,
          agreementText = "",
        ),
      )
    }

    val txSubmission: Submission.Transaction = Submission.Transaction(
      submitterInfo = submitterInfo,
      transactionMeta = transactionMeta,
      transaction = TransactionBuilder.EmptySubmitted,
      estimatedInterpretationCost = 0L,
      disclosedContracts = ImmArray(Versioned(TransactionVersion.VDev, disclosedContract)),
    )(loggingContext)

    val preparedTransactionSubmission: PreparedTransactionSubmission =
      PreparedTransactionSubmission(
        keyInputs = keyInputs,
        inputContracts = inputContracts,
        updatedKeys = Map.empty,
        consumedContracts = Set.empty,
        blindingInfo = blindingInfo,
        transactionInformees = informeesSet,
        submission = txSubmission,
      )

    val contractMaxLedgerTime: Timestamp = txLedgerEffectiveTime.addMicros(-1L)
    val input = Right(offset -> preparedTransactionSubmission)

    when(
      indexServiceMock.lookupMaximumLedgerTimeAfterInterpretation(referredContracts)(loggingContext)
    ).thenReturn(
      Future.successful(MaximumLedgerTime.Max(contractMaxLedgerTime))
    )

    when(indexServiceMock.lookupContractKey(informeesSet, keyCreated))
      .thenReturn(Future.successful(None))
    when(indexServiceMock.lookupContractKey(informeesSet, activeKey))
      .thenReturn(Future.successful(Some(inputContract)))
    when(indexServiceMock.lookupContractKey(informeesSet, nonExistingKey))
      .thenReturn(Future.successful(None))

    when(
      indexServiceMock.lookupContractStateWithoutDivulgence(eqTo(disclosedContract.contractId))(
        any[LoggingContext]
      )
    )
      .thenReturn(
        Future.successful(
          ContractState.Active(
            VersionedContractInstance(
              templateId,
              Versioned(TransactionVersion.VDev, disclosedContract.argument),
            ),
            disclosedContract.metadata.createdAt,
          )
        )
      )
  }
}

object ConflictCheckWithCommittedSpec {
  private val templateId = Ref.Identifier.assertFromString("pkg:Mod:Template")
  private val offsetString = Ref.HexString.assertFromString("ab")
  private val offset = Offset.fromHexString(offsetString)

  private def cid(i: Int): Value.ContractId.V1 =
    Value.ContractId.V1(Hash.hashPrivateKey(i.toString))
  private def contractKey(idx: Long) = GlobalKey.assertBuild(
    templateId = templateId,
    key = Value.ValueInt64(idx),
  )

  private def dummyTransactionMeta(txLedgerEffectiveTime: Time.Timestamp) =
    TransactionMeta(
      txLedgerEffectiveTime,
      None,
      Time.Timestamp.Epoch,
      Hash.hashPrivateKey("dummy"),
      None,
      None,
      None,
    )

  private val dummySubmitterInfo = SubmitterInfo(
    actAs = List.empty,
    readAs = List.empty,
    applicationId = Ref.ApplicationId.assertFromString("application-id"),
    commandId = Ref.CommandId.assertFromString("command-id"),
    deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(0L)),
    submissionId = Some(Ref.SubmissionId.assertFromString("some-submission-id")),
    ledgerConfiguration =
      Configuration(0L, LedgerTimeModel.reasonableDefault, Duration.ofSeconds(0L)),
  )
}
