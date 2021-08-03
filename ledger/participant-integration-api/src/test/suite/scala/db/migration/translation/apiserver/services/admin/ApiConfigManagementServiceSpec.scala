// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import java.time.{Duration => jDuration}
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.v1.admin.config_management_service.{SetTimeModelRequest, TimeModel}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.admin.ApiConfigManagementServiceSpec._
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.telemetry.TelemetrySpecBase._
import com.daml.telemetry.{TelemetryContext, TelemetrySpecBase}
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.immutable
import scala.concurrent.Future

class ApiConfigManagementServiceSpec
    extends AsyncWordSpec
    with Matchers
    with TelemetrySpecBase
    with AkkaBeforeAndAfterAll {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "ApiConfigManagementService" should {
    "propagate trace context" in {
      val apiConfigManagementService = ApiConfigManagementService.createApiService(
        new FakeStreamingIndexConfigManagementService(someConfigurationEntries),
        TestWriteConfigService,
        TimeProvider.UTC,
        LedgerConfiguration.defaultLocalLedger,
        _ => Ref.SubmissionId.assertFromString("aSubmission"),
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
}

object ApiConfigManagementServiceSpec {
  private val aSubmissionId = "aSubmission"

  private val aConfigurationGeneration = 0L

  private val someConfigurationEntries = List(
    LedgerOffset.Absolute(Ref.LedgerString.assertFromString("0")) ->
      domain.ConfigurationEntry.Accepted(
        aSubmissionId,
        Configuration(
          aConfigurationGeneration,
          LedgerTimeModel.reasonableDefault,
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

  private final class FakeStreamingIndexConfigManagementService(
      entries: immutable.Iterable[(LedgerOffset.Absolute, domain.ConfigurationEntry)]
  ) extends IndexConfigManagementService {
    override def lookupConfiguration()(implicit
        loggingContext: LoggingContext
    ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
      Future.successful(None)

    override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
        loggingContext: LoggingContext
    ): Source[(LedgerOffset.Absolute, domain.ConfigurationEntry), NotUsed] =
      Source(entries)
  }

  private object TestWriteConfigService extends state.WriteConfigService {
    override def submitConfiguration(
        maxRecordTime: Time.Timestamp,
        submissionId: Ref.SubmissionId,
        config: Configuration,
    )(implicit telemetryContext: TelemetryContext): CompletionStage[state.SubmissionResult] = {
      telemetryContext.setAttribute(
        anApplicationIdSpanAttribute._1,
        anApplicationIdSpanAttribute._2,
      )
      completedFuture(state.SubmissionResult.Acknowledged)
    }
  }
}
