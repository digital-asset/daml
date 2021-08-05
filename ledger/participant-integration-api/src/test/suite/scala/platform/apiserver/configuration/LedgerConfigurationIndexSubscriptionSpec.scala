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
import com.daml.platform.apiserver.configuration.LedgerConfigurationIndexSubscriptionSpec._
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

final class LedgerConfigurationIndexSubscriptionSpec
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
      val configurationLoadTimeout = 5.seconds

      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration())
        .thenReturn(Future.successful(Some(offset("0001") -> currentConfiguration)))
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      new LedgerConfigurationIndexSubscription(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
      ).subscription(configurationLoadTimeout).use { currentLedgerConfiguration =>
        currentLedgerConfiguration.ready.map { _ =>
          currentLedgerConfiguration.latestConfiguration() should be(Some(currentConfiguration))
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
      val configurationLoadTimeout = 5.seconds

      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      when(index.configurationEntries(None))
        .thenReturn(Source(configurationEntries).concat(Source.never))
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      new LedgerConfigurationIndexSubscription(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
      ).subscription(configurationLoadTimeout).use { currentLedgerConfiguration =>
        currentLedgerConfiguration.ready.map { _ =>
          eventually {
            currentLedgerConfiguration.latestConfiguration() should be(
              Some(configurations.last._2)
            )
          }
          succeed
        }
      }
    }

    "give up waiting if the configuration takes too long to appear" in {
      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      when(index.configurationEntries(None)).thenReturn(Source.never)

      val configurationLoadTimeout = 500.millis
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      new LedgerConfigurationIndexSubscription(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
      ).subscription(configurationLoadTimeout).use { currentLedgerConfiguration =>
        currentLedgerConfiguration.ready.isCompleted should be(false)
        scheduler.timePasses(1.second)
        currentLedgerConfiguration.ready.isCompleted should be(true)
        currentLedgerConfiguration.ready.map { _ =>
          currentLedgerConfiguration.latestConfiguration() should be(None)
        }
      }
    }

    "never becomes ready if stopped" in {
      val index = mock[IndexConfigManagementService]
      when(index.lookupConfiguration()).thenReturn(Future.successful(None))
      when(index.configurationEntries(None)).thenReturn(Source.never)

      val configurationLoadTimeout = 1.second
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      val resource = new LedgerConfigurationIndexSubscription(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
      ).subscription(configurationLoadTimeout).acquire()

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

      val configurationLoadTimeout = 1.second
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      new LedgerConfigurationIndexSubscription(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
      ).subscription(configurationLoadTimeout).use { currentLedgerConfiguration =>
        currentLedgerConfiguration.ready.transform(Success.apply).map { result =>
          inside(result) { case Failure(exception) =>
            exception should be(failure)
          }
        }
      }
    }
  }
}

object LedgerConfigurationIndexSubscriptionSpec {
  private def offset(value: String): LedgerOffset.Absolute =
    LedgerOffset.Absolute(Ref.LedgerString.assertFromString(value))
}
