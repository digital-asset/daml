// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.{Duration, Instant}

import akka.stream.Materializer
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.TestLoggers
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.ApplicationId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.kvutils.deduplication.DeduplicationPeriodSupport
import com.daml.ledger.participant.state.v2.SubmissionResult.Acknowledged
import com.daml.ledger.participant.state.v2.{
  SubmissionResult,
  SubmitterInfo,
  TransactionMeta,
  WriteService,
}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.{SubmittedTransaction, TransactionVersion, VersionedTransaction}
import com.daml.logging.LoggingContext
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._

class WriteServiceWithDeduplicationSupportSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with AkkaBeforeAndAfterAll
    with TestLoggers
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {
  implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext
  private val mockWriteService: WriteService = mock[WriteService]
  private val mockDeduplicationPeriodSupport: DeduplicationPeriodSupport =
    mock[DeduplicationPeriodSupport]
  private val service = new WriteServiceWithDeduplicationSupport(
    mockWriteService,
    mockDeduplicationPeriodSupport,
  )
  private val submitterInfo = SubmitterInfo(
    List.empty,
    List.empty,
    Ref.ApplicationId.assertFromString("applicationid"),
    Ref.CommandId.assertFromString("commandid"),
    DeduplicationPeriod.DeduplicationDuration(
      Duration.ofSeconds(1)
    ),
    None,
    Configuration.reasonableInitialConfiguration,
  )
  private val transactionMeta = TransactionMeta(
    Timestamp.now(),
    None,
    Timestamp.now(),
    Hash.hashPrivateKey("key"),
    None,
    None,
    None,
  )
  private val submittedTransaction = SubmittedTransaction(
    VersionedTransaction(TransactionVersion.minVersion, Map.empty, ImmArray.empty)
  )
  override protected def afterEach(): Unit = reset(mockWriteService, mockDeduplicationPeriodSupport)

  "use the returned deduplication period" in {
    val convertedDeduplicationPeriod =
      DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(4))
    when(
      mockDeduplicationPeriodSupport.supportedDeduplicationPeriod(
        eqTo(submitterInfo.deduplicationPeriod),
        eqTo(Configuration.reasonableMaxDeduplicationTime),
        eqTo(LedgerTimeModel.reasonableDefault),
        eqTo(ApplicationId(submitterInfo.applicationId)),
        any[Set[Ref.Party]],
        eqTo(transactionMeta.submissionTime.toInstant),
      )(
        any[Materializer],
        any[ExecutionContext],
        any[LoggingContext],
        any[ContextualizedErrorLogger],
      )
    ).thenReturn(Future.successful(convertedDeduplicationPeriod))
    when(
      mockWriteService.submitTransaction(
        any[SubmitterInfo],
        any[TransactionMeta],
        any[SubmittedTransaction],
        any[Long],
      )(any[LoggingContext], any[TelemetryContext])
    ).thenReturn(Future.successful[SubmissionResult](Acknowledged).asJava)

    service
      .submitTransaction(
        submitterInfo,
        transactionMeta,
        submittedTransaction,
        0,
      )
      .asScala
      .map { _ =>
        verify(mockWriteService).submitTransaction(
          submitterInfo.copy(deduplicationPeriod = convertedDeduplicationPeriod),
          transactionMeta,
          submittedTransaction,
          0,
        )
        succeed
      }
  }

  "use all parties in `actAs` and `readAs` as readers for the completion stream" in {
    val submitterInfoWithParties = submitterInfo.copy(
      actAs = List(Ref.Party.assertFromString("party1")),
      readAs = List(Ref.Party.assertFromString("party2")),
    )
    when(
      mockDeduplicationPeriodSupport.supportedDeduplicationPeriod(
        any[DeduplicationPeriod],
        any[Duration],
        any[LedgerTimeModel],
        any[ApplicationId],
        any[Set[Ref.Party]],
        any[Instant],
      )(
        any[Materializer],
        any[ExecutionContext],
        any[LoggingContext],
        any[ContextualizedErrorLogger],
      )
    ).thenReturn(Future.successful(submitterInfo.deduplicationPeriod))
    when(
      mockWriteService.submitTransaction(
        any[SubmitterInfo],
        any[TransactionMeta],
        any[SubmittedTransaction],
        any[Long],
      )(any[LoggingContext], any[TelemetryContext])
    ).thenReturn(Future.successful[SubmissionResult](Acknowledged).asJava)
    service
      .submitTransaction(
        submitterInfoWithParties,
        transactionMeta,
        submittedTransaction,
        0,
      )
      .asScala
      .map { _ =>
        val expectedReaders =
          (submitterInfoWithParties.readAs ++ submitterInfoWithParties.actAs).toSet
        verify(mockDeduplicationPeriodSupport).supportedDeduplicationPeriod(
          any[DeduplicationPeriod],
          any[Duration],
          any[LedgerTimeModel],
          any[ApplicationId],
          eqTo(expectedReaders),
          any[Instant],
        )(
          any[Materializer],
          any[ExecutionContext],
          any[LoggingContext],
          any[ContextualizedErrorLogger],
        )
        succeed
      }
  }

}
