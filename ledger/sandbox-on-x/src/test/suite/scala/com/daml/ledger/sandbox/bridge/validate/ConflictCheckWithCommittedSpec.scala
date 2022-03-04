// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.codahale.metrics.MetricRegistry
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{IndexService, MaximumLedgerTime}
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.ledger.sandbox.bridge.validate.ConflictCheckWithCommittedSpec._
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{BlindingInfo, GlobalKey, Transaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.FixtureContext
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Duration

import scala.concurrent.Future

class ConflictCheckWithCommittedSpec
    extends AsyncFlatSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  behavior of classOf[ConflictCheckWithCommittedImpl].getSimpleName

  it should "validate causal monotonicity and key usages" in new TestContext {
    conflictCheckWithCommitted(input).map(_ shouldBe input)
  }

  it should "pass causal monotonicity check if referred contracts is empty" in new TestContext {
    val submissionWithEmptyReferredContracts: PreparedTransactionSubmission =
      preparedTransactionSubmission.copy(inputContracts = inputContracts -- referredContracts)
    for {
      validationResult <- conflictCheckWithCommitted(
        Right(offset -> submissionWithEmptyReferredContracts)
      )
    } yield {
      verify(indexServiceMock, never).lookupMaximumLedgerTimeAfterInterpretation(
        any[Set[ContractId]]
      )(
        any[LoggingContext]
      )
      validationResult shouldBe Right(offset -> submissionWithEmptyReferredContracts)
    }
  }

  it should "handle causal monotonicity violation" in new TestContext {
    val lateTxLet: Timestamp = contractMaxLedgerTime.subtract(Duration.ofSeconds(1L))
    val nonCausalTxSubmission: PreparedTransactionSubmission =
      preparedTransactionSubmission.copy(submission =
        txSubmission.copy(transactionMeta = transactionMeta.copy(ledgerEffectiveTime = lateTxLet))
      )

    conflictCheckWithCommitted(Right(offset -> nonCausalTxSubmission))
      .map {
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

    conflictCheckWithCommitted(input)
      .map {
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

    conflictCheckWithCommitted(input)
      .map {
        case Left(LedgerBridgeInternalError(actualInternalError, _)) =>
          actualInternalError shouldBe someInternalError
        case failure => fail(s"Expectation mismatch: got $failure")
      }
  }

  it should "handle an inconsistent contract key (on key active input)" in new TestContext {
    when(indexServiceMock.lookupContractKey(informeesSet, activeKey)(loggingContext))
      .thenReturn(Future.successful(None))

    conflictCheckWithCommitted(input)
      .map {
        case Left(InconsistentContractKey(Some(actualInputContract), None)) =>
          actualInputContract shouldBe inputContract
        case failure => fail(s"Expectation mismatch: got $failure")
      }
  }

  it should "handle an inconsistent contract key (on negative lookup input)" in new TestContext {
    val existingContractForKey: ContractId = cid(1337)

    when(indexServiceMock.lookupContractKey(informeesSet, nonExistingKey)(loggingContext))
      .thenReturn(Future.successful(Some(existingContractForKey)))

    conflictCheckWithCommitted(input)
      .map {
        case Left(InconsistentContractKey(None, Some(actualExistingContractForKey))) =>
          actualExistingContractForKey shouldBe existingContractForKey
        case failure => fail(s"Expectation mismatch: got $failure")
      }
  }

  it should "handle a duplicate contract key" in new TestContext {
    val existingContractForKey: ContractId = cid(1337)

    when(indexServiceMock.lookupContractKey(informeesSet, keyCreated)(loggingContext))
      .thenReturn(Future.successful(Some(existingContractForKey)))

    conflictCheckWithCommitted(input)
      .map {
        case Left(DuplicateKey(actualDuplicateKey)) => actualDuplicateKey shouldBe keyCreated
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
        bridgeMetrics = new BridgeMetrics(new Metrics(new MetricRegistry())),
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

    val txSubmission: Submission.Transaction = Submission.Transaction(
      submitterInfo = submitterInfo,
      transactionMeta = transactionMeta,
      transaction = TransactionBuilder.EmptySubmitted,
      estimatedInterpretationCost = 0L,
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
  }
}

object ConflictCheckWithCommittedSpec {
  private val templateId = Ref.Identifier.assertFromString("pkg:Mod:Template")
  private val offsetString = Ref.HexString.assertFromString("ab")
  private val offset = Offset.fromHexString(offsetString)

  private def cid(i: Int): Value.ContractId = Value.ContractId.V1(Hash.hashPrivateKey(i.toString))
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
