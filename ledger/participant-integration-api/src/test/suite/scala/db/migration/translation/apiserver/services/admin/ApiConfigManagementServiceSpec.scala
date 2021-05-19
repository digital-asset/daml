// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.{Duration => jDuration}
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerOffset.Absolute
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.config_management_service.{SetTimeModelRequest, TimeModel}
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SubmissionId,
  SubmissionResult,
  WriteConfigService,
}
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.telemetry.{TelemetryContext, TelemetrySpecBase}
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ApiConfigManagementServiceSpec
    extends AsyncWordSpec
    with TelemetrySpecBase
    with MockitoSugar
    with Matchers
    with ArgumentMatchersSugar
    with AkkaBeforeAndAfterAll {

  import ApiConfigManagementServiceSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "ApiConfigManagementService" should {
    "propagate trace context" in {
      val mockReadBackend = mockIndexConfigManagementService()

      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        mockReadBackend,
        TestWriteConfigService,
        TimeProvider.UTC,
        LedgerConfiguration.defaultLocalLedger,
      )

      val span = anEmptySpan()
      val scope = span.makeCurrent()
      apiConfigManagementService
        .setTimeModel(aSetTimeModelRequest)
        .andThen { case _ =>
          scope.close()
          span.end()
        }
        .map { _ =>
          spanExporter.finishedSpanAttributes should contain(anApplicationIdSpanAttribute)
        }
    }
  }

  private def mockIndexConfigManagementService() = {
    val mockIndexConfigManagementService = mock[IndexConfigManagementService]
    when(mockIndexConfigManagementService.lookupConfiguration()(any[LoggingContext]))
      .thenReturn(Future.successful(None))
    when(
      mockIndexConfigManagementService.configurationEntries(any[Option[Absolute]])(
        any[LoggingContext]
      )
    ).thenReturn(configurationEntries)
    mockIndexConfigManagementService
  }

  private object TestWriteConfigService extends WriteConfigService {
    override def submitConfiguration(
        maxRecordTime: Time.Timestamp,
        submissionId: SubmissionId,
        config: Configuration,
    )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] = {
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      CompletableFuture.completedFuture(SubmissionResult.Acknowledged)
    }
  }
}

object ApiConfigManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private val aConfigurationGeneration = 0L

  private val configurationEntries = Source.single(
    Absolute(Ref.LedgerString.assertFromString("0")) -> domain.ConfigurationEntry.Accepted(
      aSubmissionId,
      Configuration(
        aConfigurationGeneration,
        v1.TimeModel.reasonableDefault,
        jDuration.ZERO,
      ),
    )
  )

  private val aSetTimeModelRequest = SetTimeModelRequest(
    aSubmissionId,
    Some(Timestamp.defaultInstance),
    aConfigurationGeneration,
    Some(
      TimeModel(
        Some(Duration.defaultInstance),
        Some(Duration.defaultInstance),
        Some(Duration.defaultInstance),
      )
    ),
  )
}
