// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import java.time.Duration

import akka.event.NoLogging
import akka.stream.scaladsl.Source
import akka.testkit.ExplicitlyTriggeredScheduler
import com.daml.ledger.api.domain.{ConfigurationEntry, LedgerOffset}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.configuration.IndexStreamingCurrentLedgerConfigurationSpec._
import com.daml.platform.configuration.LedgerConfiguration
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

final class IndexStreamingCurrentLedgerConfigurationSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with Inside
    with AkkaBeforeAndAfterAll
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "the current ledger configuration" should {
    "look up the latest configuration from the index on startup" in {
      val currentConfiguration =
        Configuration(7, LedgerTimeModel.reasonableDefault, Duration.ofDays(1))
      val ledgerConfiguration = LedgerConfiguration(
        initialConfiguration = Configuration(0, LedgerTimeModel.reasonableDefault, Duration.ZERO),
        initialConfigurationSubmitDelay = Duration.ZERO,
        configurationLoadTimeout = Duration.ofSeconds(5),
      )

      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration())
        .thenReturn(Future.successful(Some(offset("0001") -> currentConfiguration)))
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      IndexStreamingCurrentLedgerConfiguration
        .owner(
          ledgerConfiguration = ledgerConfiguration,
          index = index,
          scheduler = scheduler,
          materializer = materializer,
          servicesExecutionContext = system.dispatcher,
        )
        .use { currentLedgerConfiguration =>
          currentLedgerConfiguration.ready.map { _ =>
            currentLedgerConfiguration.latestConfiguration should be(Some(currentConfiguration))
            succeed
          }
        }
    }

    "stream the latest configuration from the index" in {
      val configurations = List(
        offset("000a") -> Configuration(
          generation = 3,
          timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(1)),
          maxDeduplicationTime = Duration.ofDays(1),
        ),
        offset("0023") -> Configuration(
          generation = 4,
          timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(2)),
          maxDeduplicationTime = Duration.ofDays(1),
        ),
        offset("01ef") -> Configuration(
          generation = 5,
          timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(2)),
          maxDeduplicationTime = Duration.ofHours(6),
        ),
      )
      val configurationEntries = configurations.zipWithIndex.map {
        case ((offset, configuration), index) =>
          offset -> ConfigurationEntry.Accepted(s"submission ID #$index", configuration)
      }
      val ledgerConfiguration = LedgerConfiguration(
        initialConfiguration = Configuration(0, LedgerTimeModel.reasonableDefault, Duration.ZERO),
        initialConfigurationSubmitDelay = Duration.ZERO,
        configurationLoadTimeout = Duration.ofSeconds(5),
      )

      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      when(index.configurationEntries(None))
        .thenReturn(Source(configurationEntries).concat(Source.never))
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      IndexStreamingCurrentLedgerConfiguration
        .owner(
          ledgerConfiguration = ledgerConfiguration,
          index = index,
          scheduler = scheduler,
          materializer = materializer,
          servicesExecutionContext = system.dispatcher,
        )
        .use { currentLedgerConfiguration =>
          currentLedgerConfiguration.ready.map { _ =>
            eventually {
              currentLedgerConfiguration.latestConfiguration should be(Some(configurations.last._2))
            }
            succeed
          }
        }
    }

    "give up waiting if the configuration takes too long to appear" in {
      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      when(index.configurationEntries(None)).thenReturn(Source.never)

      val ledgerConfiguration = LedgerConfiguration(
        initialConfiguration = Configuration(0, LedgerTimeModel.reasonableDefault, Duration.ZERO),
        initialConfigurationSubmitDelay = Duration.ZERO,
        configurationLoadTimeout = Duration.ofMillis(500),
      )
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      IndexStreamingCurrentLedgerConfiguration
        .owner(
          ledgerConfiguration = ledgerConfiguration,
          index = index,
          scheduler = scheduler,
          materializer = materializer,
          servicesExecutionContext = system.dispatcher,
        )
        .use { currentLedgerConfiguration =>
          currentLedgerConfiguration.ready.isCompleted should be(false)
          scheduler.timePasses(1.second)
          currentLedgerConfiguration.ready.isCompleted should be(true)
          currentLedgerConfiguration.ready.map { _ =>
            currentLedgerConfiguration.latestConfiguration should be(None)
          }
        }
    }

    "never becomes ready if stopped" in {
      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      when(index.configurationEntries(None)).thenReturn(Source.never)

      val ledgerConfiguration = LedgerConfiguration(
        initialConfiguration = Configuration(0, LedgerTimeModel.reasonableDefault, Duration.ZERO),
        initialConfigurationSubmitDelay = Duration.ZERO,
        configurationLoadTimeout = Duration.ofSeconds(1),
      )
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      val owner = IndexStreamingCurrentLedgerConfiguration.owner(
        ledgerConfiguration = ledgerConfiguration,
        index = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
      )
      val resource = owner.acquire()

      resource.asFuture
        .flatMap { currentLedgerConfiguration =>
          currentLedgerConfiguration.ready.isCompleted should be(false)
          resource
            .release() // Will cancel reading the configuration
            .map(_ => currentLedgerConfiguration)
        }
        .map { currentLedgerConfiguration =>
          scheduler.timePasses(5.seconds)
          currentLedgerConfiguration.ready.isCompleted should be(false)
        }
    }

    "readiness fails if lookup fails" in {
      val failure = new RuntimeException("It failed.")

      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.failed(failure))
      when(index.configurationEntries(None)).thenReturn(Source.never)

      val ledgerConfiguration = LedgerConfiguration(
        initialConfiguration = Configuration(0, LedgerTimeModel.reasonableDefault, Duration.ZERO),
        initialConfigurationSubmitDelay = Duration.ZERO,
        configurationLoadTimeout = Duration.ofSeconds(1),
      )
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      IndexStreamingCurrentLedgerConfiguration
        .owner(ledgerConfiguration, index, scheduler, materializer, system.dispatcher)
        .use { currentLedgerConfiguration =>
          currentLedgerConfiguration.ready.transform(Success.apply).map { result =>
            inside(result) { case Failure(exception) =>
              exception should be(failure)
            }
          }
        }
    }
  }
}

object IndexStreamingCurrentLedgerConfigurationSpec {
  private def offset(value: String): LedgerOffset.Absolute =
    LedgerOffset.Absolute(Ref.LedgerString.assertFromString(value))
}
