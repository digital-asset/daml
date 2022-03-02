// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.codahale.metrics.MetricRegistry
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.validate.PrepareSubmissionSpec._
import com.daml.ledger.sandbox.domain.Rejection.{
  TransactionInternallyDuplicateKeys,
  TransactionInternallyInconsistentKey,
}
import com.daml.ledger.sandbox.domain.Submission
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{GlobalKey, SubmittedTransaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ValueNil}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.MockitoSugar.mock
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class PrepareSubmissionSpec extends AsyncFlatSpec with Matchers {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val errorLogger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forTesting(getClass)

  private val prepareSubmission = new PrepareSubmissionImpl(
    new BridgeMetrics(new Metrics(new MetricRegistry))
  )

  private def cid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  behavior of classOf[PrepareSubmissionImpl].getSimpleName

  it should "forward the correct failure on inconsistent keys" in {
    val txBuilder = TransactionBuilder()

    val templateId = Ref.Identifier.assertFromString("pkg:mod:template")
    val keyValue = Value.ValueText("key-1")

    val createNode = txBuilder.create(
      id = cid("#1"),
      templateId = templateId,
      argument = Value.ValueInt64(1),
      signatories = Set.empty,
      observers = Set.empty,
      key = Some(keyValue),
    )

    txBuilder.add(createNode)

    val contractKey = GlobalKey.assertBuild(templateId, keyValue)
    val otherCreateNode = txBuilder.create(
      id = cid("#2"),
      templateId = templateId,
      argument = Value.ValueInt64(1),
      signatories = Set.empty,
      observers = Set.empty,
      key = Some(keyValue),
    )
    val exerciseNode =
      txBuilder.exercise(
        contract = otherCreateNode,
        choice = Ref.Name.assertFromString("choice"),
        consuming = false,
        actingParties = Set.empty,
        argument = ValueNil,
      )

    txBuilder.add(exerciseNode)

    val validationResult = prepareSubmission(
      Submission.Transaction(
        submitterInfo = submitterInfo,
        transactionMeta = txMeta,
        transaction = SubmittedTransaction(txBuilder.build()),
        estimatedInterpretationCost = 0L,
      )
    )
    validationResult.map(
      _ shouldBe Left(
        TransactionInternallyInconsistentKey(contractKey, submitterInfo.toCompletionInfo())
      )
    )
  }

  it should "forward the correct failure on duplicate keys" in {
    val txBuilder = TransactionBuilder()

    val templateId = Ref.Identifier.assertFromString("pkg:mod:template")
    val keyValue = Value.ValueText("key-1")

    val createNode = txBuilder.create(
      id = cid("#1"),
      templateId = templateId,
      argument = Value.ValueInt64(1),
      signatories = Set.empty,
      observers = Set.empty,
      key = Some(keyValue),
    )

    txBuilder.add(createNode)
    txBuilder.add(createNode)

    val contractKey = GlobalKey.assertBuild(templateId, keyValue)

    val validationResult = prepareSubmission(
      Submission.Transaction(
        submitterInfo = submitterInfo,
        transactionMeta = txMeta,
        transaction = SubmittedTransaction(txBuilder.build()),
        estimatedInterpretationCost = 0L,
      )
    )
    validationResult.map(
      _ shouldBe Left(
        TransactionInternallyDuplicateKeys(contractKey, submitterInfo.toCompletionInfo())
      )
    )
  }

  it should "forward no-op prepared submission for non-transaction submissions" in {
    val otherSubmission = mock[Submission]

    prepareSubmission(otherSubmission).map(
      _ shouldBe Right(NoOpPreparedSubmission(otherSubmission))
    )
  }
}

object PrepareSubmissionSpec {
  private val txMeta = TransactionMeta(
    Time.Timestamp.now(),
    None,
    Time.Timestamp.Epoch,
    Hash.hashPrivateKey("dummy"),
    None,
    None,
    None,
  )

  private val submitterInfo = SubmitterInfo(
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
