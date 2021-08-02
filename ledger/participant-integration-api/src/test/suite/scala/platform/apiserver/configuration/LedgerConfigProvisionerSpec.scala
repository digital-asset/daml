// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference

import com.daml.api.util.TimeProvider
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.SubmissionId
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.telemetry.TelemetryContext
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.DurationInt

final class LedgerConfigProvisionerSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with AkkaBeforeAndAfterAll
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "Ledger Config Provider" should {
    "write a ledger configuration to the index if one is not provided" in {
      val configurationToSubmit =
        Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofDays(1))
      val ledgerConfiguration = LedgerConfiguration(
        configurationToSubmit,
        initialConfigurationSubmitDelay = Duration.ofMillis(100),
        configurationLoadTimeout = Duration.ZERO,
      )
      val submissionId = Ref.SubmissionId.assertFromString("the submission ID")

      val currentLedgerConfiguration = new CurrentLedgerConfiguration {
        override def latestConfiguration: Option[Configuration] = None
      }
      val writeService = mock[state.WriteConfigService]
      val timeProvider = TimeProvider.Constant(Instant.EPOCH)
      val submissionIdGenerator = new SubmissionIdGenerator {
        override def generate(): SubmissionId = submissionId
      }

      LedgerConfigProvisioner
        .owner(
          currentLedgerConfiguration = currentLedgerConfiguration,
          writeService = writeService,
          timeProvider = timeProvider,
          submissionIdGenerator = submissionIdGenerator,
          ledgerConfiguration = ledgerConfiguration,
        )
        .use { _ =>
          eventually(PatienceConfiguration.Timeout(1.second)) {
            verify(writeService).submitConfiguration(
              eqTo(Timestamp.assertFromInstant(timeProvider.getCurrentTime.plusSeconds(60))),
              eqTo(submissionId),
              eqTo(configurationToSubmit),
            )(any[TelemetryContext])
          }
          succeed
        }
    }

    "not write a configuration if one is provided" in {
      val currentConfiguration =
        Configuration(6, LedgerTimeModel.reasonableDefault, Duration.ofHours(12))
      val ledgerConfiguration = LedgerConfiguration(
        initialConfiguration =
          Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofDays(1)),
        initialConfigurationSubmitDelay = Duration.ofMillis(100),
        configurationLoadTimeout = Duration.ZERO,
      )

      val currentLedgerConfiguration = new CurrentLedgerConfiguration {
        override def latestConfiguration: Option[Configuration] = Some(currentConfiguration)
      }
      val writeService = mock[state.WriteConfigService]
      val timeProvider = TimeProvider.Constant(Instant.EPOCH)
      val submissionIdGenerator = new SubmissionIdGenerator {
        override def generate(): SubmissionId = {
          fail("We should never generate a submission.")
        }
      }

      LedgerConfigProvisioner
        .owner(
          currentLedgerConfiguration = currentLedgerConfiguration,
          writeService = writeService,
          timeProvider = timeProvider,
          submissionIdGenerator = submissionIdGenerator,
          ledgerConfiguration = ledgerConfiguration,
        )
        .use { _ =>
          verify(writeService, after(1.second.toMillis.toInt).never())
            .submitConfiguration(
              any[Timestamp],
              any[Ref.SubmissionId],
              any[Configuration],
            )(any[TelemetryContext])
          succeed
        }
    }
  }

  "not write a configuration if one is provided within the time window" in {
    val eventualConfiguration =
      Configuration(8, LedgerTimeModel.reasonableDefault, Duration.ofDays(3))
    val ledgerConfiguration = LedgerConfiguration(
      initialConfiguration =
        Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofDays(1)),
      initialConfigurationSubmitDelay = Duration.ofSeconds(1),
      configurationLoadTimeout = Duration.ZERO,
    )

    val currentConfiguration = new AtomicReference[Option[Configuration]](None)
    val currentLedgerConfiguration = new CurrentLedgerConfiguration {
      override def latestConfiguration: Option[Configuration] = currentConfiguration.get
    }
    val writeService = mock[state.WriteConfigService]
    val timeProvider = TimeProvider.Constant(Instant.EPOCH)
    val submissionIdGenerator = new SubmissionIdGenerator {
      override def generate(): SubmissionId = {
        fail("We should never generate a submission.")
      }
    }

    LedgerConfigProvisioner
      .owner(
        currentLedgerConfiguration = currentLedgerConfiguration,
        writeService = writeService,
        timeProvider = timeProvider,
        submissionIdGenerator = submissionIdGenerator,
        ledgerConfiguration = ledgerConfiguration,
      )
      .use { _ =>
        materializer.scheduleOnce(
          100.millis,
          { () => currentConfiguration.set(Some(eventualConfiguration)) },
        )
        verify(writeService, after(1.second.toMillis.toInt).never())
          .submitConfiguration(
            any[Timestamp],
            any[Ref.SubmissionId],
            any[Configuration],
          )(any[TelemetryContext])
        succeed
      }
  }
}
