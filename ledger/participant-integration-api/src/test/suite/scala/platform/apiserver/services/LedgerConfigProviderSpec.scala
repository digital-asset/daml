// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import java.time.{Duration, Instant}
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{Materializer, OverflowStrategy}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain.{ConfigurationEntry, LedgerOffset}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.LedgerConfigProviderSpec._
import com.daml.platform.configuration.LedgerConfiguration
import com.daml.telemetry.TelemetryContext
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class LedgerConfigProviderSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with MockitoSugar {

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "Ledger Config Provider" should {
    "read an existing ledger configuration from the index" in {
      val index = mock[IndexConfigManagementService]
      val writeService = mock[state.WriteConfigService]
      val configuration = configurationWith(generation = 7)
      when(index.lookupConfiguration())
        .thenReturn(Future.successful(Some(offset("0001") -> configuration)))

      LedgerConfigProvider
        .owner(
          index,
          optWriteService = Some(writeService),
          timeProvider = someTimeProvider,
          config = LedgerConfiguration(
            initialConfiguration = configurationWith(generation = 3),
            initialConfigurationSubmitDelay = Duration.ofSeconds(1),
            configurationLoadTimeout = Duration.ofSeconds(5),
          ),
        )
        .use { currentLedgerConfiguration =>
          verifyZeroInteractions(writeService)
          currentLedgerConfiguration.latestConfiguration should be(Some(configuration))
        }
    }

    "write a ledger configuration to the index if one is not provided" in {
      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      val writeService = new FakeWriteConfigService
      when(index.configurationEntries(None)).thenReturn(writeService.configurationSource)
      val configurationToSubmit = configurationWith(generation = 1)

      LedgerConfigProvider
        .owner(
          index,
          optWriteService = Some(writeService),
          timeProvider = someTimeProvider,
          config = LedgerConfiguration(
            configurationToSubmit,
            initialConfigurationSubmitDelay = Duration.ofMillis(100),
            configurationLoadTimeout = Duration.ofSeconds(5),
          ),
        )
        .use { ledgerConfigProvider =>
          ledgerConfigProvider.latestConfiguration should be(Some(configurationToSubmit))
        }
    }

    "if the write takes too long, give up waiting" in {
      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      val writeService = new FakeWriteConfigService(delay = 5.seconds)
      when(index.configurationEntries(None)).thenReturn(writeService.configurationSource)
      val configurationToSubmit = configurationWith(generation = 1)

      LedgerConfigProvider
        .owner(
          index,
          optWriteService = Some(writeService),
          timeProvider = someTimeProvider,
          config = LedgerConfiguration(
            configurationToSubmit,
            initialConfigurationSubmitDelay = Duration.ZERO,
            configurationLoadTimeout = Duration.ofMillis(500),
          ),
        )
        .use { ledgerConfigProvider =>
          ledgerConfigProvider.latestConfiguration should be(None)
        }
    }
  }
}

object LedgerConfigProviderSpec {
  private type ConfigurationSourceEntry = (LedgerOffset.Absolute, ConfigurationEntry)

  private val someTimeProvider = TimeProvider.Constant(Instant.EPOCH)

  private def offset(value: String): LedgerOffset.Absolute = {
    LedgerOffset.Absolute(Ref.LedgerString.assertFromString(value))
  }

  private def configurationWith(generation: Long): Configuration = {
    Configuration(generation, LedgerTimeModel.reasonableDefault, Duration.ofDays(1))
  }

  private final class FakeWriteConfigService(
      delay: FiniteDuration = scala.concurrent.duration.Duration.Zero
  )(implicit materializer: Materializer)
      extends state.WriteConfigService {
    private var currentOffset = 0

    private val (queue, source) = Source
      .queue[ConfigurationSourceEntry](bufferSize = 8, OverflowStrategy.backpressure)
      .preMaterialize()

    val configurationSource: Source[ConfigurationSourceEntry, NotUsed] = source

    override def submitConfiguration(
        maxRecordTime: Timestamp,
        submissionId: Ref.SubmissionId,
        config: Configuration,
    )(implicit telemetryContext: TelemetryContext): CompletionStage[state.SubmissionResult] =
      CompletableFuture.supplyAsync { () =>
        Thread.sleep(delay.toMillis)
        currentOffset += 1
        queue.offer(
          offset(currentOffset.toString) -> ConfigurationEntry.Accepted(submissionId, config)
        )
        state.SubmissionResult.Acknowledged
      }
  }

}
