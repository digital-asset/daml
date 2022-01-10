// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimeProvider
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.{DeduplicationPeriod, domain}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v2.{CompletionInfo, SubmitterInfo, TransactionMeta}
import com.daml.ledger.sandbox.bridge.ConflictCheckingLedgerBridgeTest._
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.{Rejection, Submission}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{BlindingInfo, GlobalKey, Transaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.server.api.validation.ErrorFactories
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import scala.concurrent.Future
import scala.util.chaining.scalaUtilChainingOps

class ConflictCheckingLedgerBridgeTest
    extends AsyncFlatSpec
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar {

  // TODO SoX: assert prepareSubmission

  "tagWithLedgerEnd" should "tag the incoming submissions with the index service ledger end" in newFixture {
    fixture =>
      import fixture._
      val somePreparedSubmission = mock[PreparedSubmission].tap { preparedSubmission =>
        val someSubmission = mock[Submission].tap { submission =>
          when(submission.loggingContext).thenReturn(loggingContext)
        }
        when(preparedSubmission.submission).thenReturn(someSubmission)
      }

      when(indexServiceMock.currentLedgerEnd()).thenReturn(
        Future.successful(domain.LedgerOffset.Absolute(offsetString))
      )

      conflictCheckingLedgerBridge
        .tagWithLedgerEnd(Right(somePreparedSubmission))
        .map(
          _ shouldBe Right(
            offset -> somePreparedSubmission
          )
        )
  }

  "tagWithLedgerEnd" should "propagate a rejection" in newFixture { fixture =>
    import fixture._
    val rejectionIn = Left(Rejection.NoLedgerConfiguration(mock[CompletionInfo], errorFactories))
    conflictCheckingLedgerBridge
      .tagWithLedgerEnd(rejectionIn)
      .map(_ shouldBe rejectionIn)
  }

  "conflictCheckWithCommitted" should "validate causal monotonicity and key usages" in newFixture {
    fixture =>
      import fixture._

      conflictCheckingLedgerBridge
        .conflictCheckWithCommitted(validationIn)
        .map(_ shouldBe validationIn)
  }

  "conflictCheckWithCommitted" should "pass causal monotonicity check if referred contracts is empty" in newFixture {
    fixture =>
      import fixture._

      val validationIn =
        Right(
          offset -> preparedTransactionSubmission.copy(inputContracts = Set(divulgedContract))
        )

      conflictCheckingLedgerBridge
        .conflictCheckWithCommitted(validationIn)
        .map { validationResult =>
          verify(indexServiceMock, never)
            .lookupMaximumLedgerTime(any[Set[ContractId]])(any[LoggingContext])
          validationResult shouldBe validationIn
        }
  }

  "conflictCheckWithCommitted" should "handle causal monotonicity violation" in newFixture {
    fixture =>
      import fixture._

      val lateTxLet = txLedgerEffectiveTime.subtract(Duration.ofNanos(7000L))

      val validationIn = Right(
        offset -> preparedTransactionSubmission.copy(submission =
          txSubmission.copy(transactionMeta = transactionMeta.copy(ledgerEffectiveTime = lateTxLet))
        )
      )

      conflictCheckingLedgerBridge
        .conflictCheckWithCommitted(validationIn)
        .map {
          case Left(CausalMonotonicityViolation(actualContractMaxLedgerTime, actualTxLet)) =>
            actualContractMaxLedgerTime shouldBe contractMaxLedgerTime
            actualTxLet shouldBe lateTxLet
          case failure => fail(s"Expectation mismatch: got $failure")
        }
  }

  "conflictCheckWithCommitted" should "handle missing contracts" in newFixture { fixture =>
    import fixture._

    val missingContracts = referredContracts

    when(indexServiceMock.lookupMaximumLedgerTime(referredContracts)(loggingContext))
      .thenReturn(Future.failed(MissingContracts(missingContracts)))

    conflictCheckingLedgerBridge
      .conflictCheckWithCommitted(validationIn)
      .map {
        case Left(UnknownContracts(actualMissingContracts)) =>
          actualMissingContracts shouldBe missingContracts
        case failure => fail(s"Expectation mismatch: got $failure")
      }
  }

  "conflictCheckWithCommitted" should "handle a generic lookupMaximumLedgerTime error" in newFixture {
    fixture =>
      import fixture._

      val someInternalError = new RuntimeException("oh")

      when(indexServiceMock.lookupMaximumLedgerTime(referredContracts)(loggingContext))
        .thenReturn(Future.failed(someInternalError))

      conflictCheckingLedgerBridge
        .conflictCheckWithCommitted(validationIn)
        .map {
          case Left(LedgerBridgeInternalError(actualInternalError, _)) =>
            actualInternalError shouldBe someInternalError
          case failure => fail(s"Expectation mismatch: got $failure")
        }
  }

  "conflictCheckWithCommitted" should "handle an inconsistent contract key (on key active input)" in newFixture {
    fixture =>
      import fixture._

      when(indexServiceMock.lookupContractKey(informeesSet, activeKey)(loggingContext))
        .thenReturn(Future.successful(None))

      conflictCheckingLedgerBridge
        .conflictCheckWithCommitted(validationIn)
        .map {
          case Left(InconsistentContractKey(Some(actualInputContract), None)) =>
            actualInputContract shouldBe inputContract
          case failure => fail(s"Expectation mismatch: got $failure")
        }
  }

  "conflictCheckWithCommitted" should "handle an inconsistent contract key (on negative lookup input)" in newFixture {
    fixture =>
      import fixture._

      val existingContractForKey = cid(1337)

      when(indexServiceMock.lookupContractKey(informeesSet, nonExistingKey)(loggingContext))
        .thenReturn(Future.successful(Some(existingContractForKey)))

      conflictCheckingLedgerBridge
        .conflictCheckWithCommitted(validationIn)
        .map {
          case Left(InconsistentContractKey(None, Some(actualExistingContractForKey))) =>
            actualExistingContractForKey shouldBe existingContractForKey
          case failure => fail(s"Expectation mismatch: got $failure")
        }
  }

  "conflictCheckWithCommitted" should "handle a duplicate contract key" in newFixture { fixture =>
    import fixture._

    val existingContractForKey = cid(1337)

    when(indexServiceMock.lookupContractKey(informeesSet, keyCreated)(loggingContext))
      .thenReturn(Future.successful(Some(existingContractForKey)))

    conflictCheckingLedgerBridge
      .conflictCheckWithCommitted(validationIn)
      .map {
        case Left(DuplicateKey(actualDuplicateKey)) => actualDuplicateKey shouldBe keyCreated
        case failure => fail(s"Expectation mismatch: got $failure")
      }
  }

  private def newFixture(
      f: ConflictCheckWithCommittedUtils => Future[Assertion]
  ) = new ConflictCheckWithCommittedUtils pipe f

  private class ConflictCheckWithCommittedUtils {
    private[ConflictCheckingLedgerBridgeTest] implicit val logger: ContextualizedLogger =
      ContextualizedLogger.get(getClass)
    private[ConflictCheckingLedgerBridgeTest] implicit val loggingContext: LoggingContext =
      LoggingContext.ForTesting
    private[ConflictCheckingLedgerBridgeTest] implicit val contextualizedErrorLogger
        : ContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)

    private val participantId = Ref.ParticipantId.assertFromString("participant")

    private[ConflictCheckingLedgerBridgeTest] val indexServiceMock = mock[IndexService]
    private[ConflictCheckingLedgerBridgeTest] val errorFactories =
      ErrorFactories(useSelfServiceErrorCodes = true)

    private[ConflictCheckingLedgerBridgeTest] val conflictCheckingLedgerBridge =
      new ConflictCheckingLedgerBridge(
        participantId = participantId,
        indexService = indexServiceMock,
        timeProvider = mock[TimeProvider],
        // TODO Sox: Test different ledger time
        initialLedgerEnd = Offset.beforeBegin,
        // TODO Sox: Test initial allocated parties
        initialAllocatedParties = Set.empty,
        initialLedgerConfiguration = None,
        bridgeMetrics = new BridgeMetrics(new Metrics(new MetricRegistry())),
        errorFactories = errorFactories,
        // TODO SoX: Test with implicit party allocation
        validatePartyAllocation = true,
        servicesThreadPoolSize = 16,
        // TODO SoX: Consider using a dedicated execution context
      )(scala.concurrent.ExecutionContext.global)

    private[ConflictCheckingLedgerBridgeTest] val inputContract = cid(1)
    private[ConflictCheckingLedgerBridgeTest] val divulgedContract = cid(3)
    private[ConflictCheckingLedgerBridgeTest] val inputContracts =
      Set(inputContract, cid(2), divulgedContract)
    private[ConflictCheckingLedgerBridgeTest] val referredContracts = Set(inputContract, cid(2))

    private[ConflictCheckingLedgerBridgeTest] val activeKey =
      GlobalKey(templateId, Value.ValueInt64(1L))
    private[ConflictCheckingLedgerBridgeTest] val keyCreated =
      GlobalKey(templateId, Value.ValueInt64(2L))
    private[ConflictCheckingLedgerBridgeTest] val nonExistingKey =
      GlobalKey(templateId, Value.ValueInt64(3L))

    private[ConflictCheckingLedgerBridgeTest] val keyInputs = Map(
      activeKey -> Transaction.KeyActive(inputContract),
      keyCreated -> Transaction.KeyCreate,
      nonExistingKey -> Transaction.NegativeKeyLookup,
    )

    private[ConflictCheckingLedgerBridgeTest] val submitterInfo = SubmitterInfo(
      actAs = List.empty,
      readAs = List.empty,
      applicationId = Ref.ApplicationId.assertFromString("applicationId"),
      commandId = Ref.CommandId.assertFromString("commandId"),
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(0L)),
      submissionId = Some(Ref.SubmissionId.assertFromString("some-submission-id")),
      ledgerConfiguration =
        Configuration(0L, LedgerTimeModel.reasonableDefault, Duration.ofSeconds(0L)),
    )

    private[ConflictCheckingLedgerBridgeTest] val txLedgerEffectiveTime: Timestamp =
      Time.Timestamp.assertFromLong(1337L)

    private[ConflictCheckingLedgerBridgeTest] val transactionMeta = TransactionMeta(
      txLedgerEffectiveTime,
      None,
      Time.Timestamp.Epoch,
      Hash.hashPrivateKey("a key"),
      None,
      None,
      None,
    )

    private[ConflictCheckingLedgerBridgeTest] val transactionInformees @ Seq(informee1, _) =
      Seq("informee-1", "informee-2").map(Ref.Party.assertFromString)
    private[ConflictCheckingLedgerBridgeTest] val informeesSet = transactionInformees.toSet
    private[ConflictCheckingLedgerBridgeTest] val blindingInfo =
      BlindingInfo(Map(), Map(divulgedContract -> Set(informee1)))

    private[ConflictCheckingLedgerBridgeTest] val txSubmission = Submission.Transaction(
      submitterInfo = submitterInfo,
      transactionMeta = transactionMeta,
      transaction = TransactionBuilder.EmptySubmitted,
      estimatedInterpretationCost = 0L,
    )(loggingContext)

    private[ConflictCheckingLedgerBridgeTest] val preparedTransactionSubmission =
      PreparedTransactionSubmission(
        keyInputs = keyInputs,
        inputContracts = inputContracts,
        updatedKeys = Map.empty,
        consumedContracts = Set.empty,
        blindingInfo = blindingInfo,
        transactionInformees = informeesSet,
        submission = txSubmission,
      )

    private[ConflictCheckingLedgerBridgeTest] val contractMaxLedgerTime: Timestamp =
      Time.Timestamp.assertFromLong(1336L)

    private[ConflictCheckingLedgerBridgeTest] val validationIn = Right(
      offset -> preparedTransactionSubmission
    )

    when(indexServiceMock.lookupMaximumLedgerTime(referredContracts)(loggingContext)).thenReturn(
      Future.successful(Some(contractMaxLedgerTime))
    )

    when(indexServiceMock.lookupContractKey(informeesSet, keyCreated))
      .thenReturn(Future.successful(None))
    when(indexServiceMock.lookupContractKey(informeesSet, activeKey))
      .thenReturn(Future.successful(Some(inputContract)))
    when(indexServiceMock.lookupContractKey(informeesSet, nonExistingKey))
      .thenReturn(Future.successful(None))
  }
}

object ConflictCheckingLedgerBridgeTest {
  private val templateId = Ref.Identifier.assertFromString("pkg:Mod:Template")

  private val offsetString = Ref.HexString.assertFromString("ab")
  private val offset = Offset.fromHexString(offsetString)

  private def cid(i: Int): Value.ContractId = Value.ContractId.V0.assertFromString(s"#$i")
}
