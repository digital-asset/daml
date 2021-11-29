// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.Duration

import akka.stream.Materializer
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.TestLoggers
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.api.domain.ApplicationId
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.deduplication.DeduplicationPeriodService
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
  private val mockDeduplicationPeriodService: DeduplicationPeriodService =
    mock[DeduplicationPeriodService]
  val service = new WriteServiceWithDeduplicationSupport(
    mockWriteService,
    mockDeduplicationPeriodService,
  )

  override protected def afterEach(): Unit = reset(mockWriteService, mockDeduplicationPeriodService)

  "use the returned deduplication period" in {
    val submitterInfo = SubmitterInfo(
      List.empty,
      Ref.ApplicationId.assertFromString("applicationid"),
      Ref.CommandId.assertFromString("commandid"),
      DeduplicationPeriod.DeduplicationDuration(
        Duration.ofSeconds(1)
      ),
      None,
      Configuration.reasonableInitialConfiguration,
    )
    val transactionMeta = TransactionMeta(
      Timestamp.now(),
      None,
      Timestamp.now(),
      Hash.hashPrivateKey("key"),
      None,
      None,
      None,
    )
    val convertedDeduplicationPeriod =
      DeduplicationPeriod.DeduplicationDuration(Duration.ofSeconds(4))
    when(
      mockDeduplicationPeriodService.supportedDeduplicationPeriod(
        eqTo(submitterInfo.deduplicationPeriod),
        eqTo(Configuration.reasonableMaxDeduplicationTime),
        eqTo(ApplicationId(submitterInfo.applicationId)),
        eqTo(submitterInfo.actAs.toSet),
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

    val submittedTransaction = SubmittedTransaction(
      VersionedTransaction(TransactionVersion.minVersion, Map.empty, ImmArray.empty)
    )
    service
      .submitTransaction(
        submitterInfo,
        transactionMeta,
        submittedTransaction,
        0,
      )
      .asScala
      .map(_ => {
        verify(mockWriteService).submitTransaction(
          submitterInfo.copy(deduplicationPeriod = convertedDeduplicationPeriod),
          transactionMeta,
          submittedTransaction,
          0,
        )
        succeed
      })
  }

}
