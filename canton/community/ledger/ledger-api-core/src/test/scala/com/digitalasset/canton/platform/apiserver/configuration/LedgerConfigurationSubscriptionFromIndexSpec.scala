// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.data.Ref
import com.daml.timer.Delayed
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.domain.{ConfigurationEntry, ParticipantOffset}
import com.digitalasset.canton.ledger.configuration.{Configuration, LedgerTimeModel}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigManagementService
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.apiserver.configuration.LedgerConfigurationSubscriptionFromIndexSpec.*
import org.apache.pekko.NotUsed
import org.apache.pekko.event.NoLogging
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.testkit.ExplicitlyTriggeredScheduler
import org.scalatest.Inside
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

final class LedgerConfigurationSubscriptionFromIndexSpec
    extends AsyncWordSpec
    with Eventually
    with Inside
    with PekkoBeforeAndAfterAll
    with BaseTest {

  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private implicit val loggingContext: LoggingContextWithTrace =
    LoggingContextWithTrace.ForTesting

  override implicit def patienceConfig: PatienceConfig =
    super.patienceConfig.copy(timeout = 1.second)

  "the current ledger configuration" should {
    "look up the latest configuration from the index on startup" in {
      val currentConfiguration =
        Configuration(7, LedgerTimeModel.reasonableDefault, Duration.ofDays(1))
      val configurationLoadTimeout = 5.seconds

      val index = new FakeIndexConfigManagementService(
        currentConfiguration = Some(offset("0001") -> currentConfiguration),
        streamingConfigurations = List.empty,
      )
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)
      val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
        loggerFactory = loggerFactory,
      )

      subscriptionBuilder
        .subscription(configurationLoadTimeout)
        .use { currentLedgerConfiguration =>
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
          maxDeduplicationDuration = Duration.ofDays(1),
        ),
        offset("0023") -> Configuration(
          generation = 4,
          timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(2)),
          maxDeduplicationDuration = Duration.ofDays(1),
        ),
        offset("01ef") -> Configuration(
          generation = 5,
          timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(2)),
          maxDeduplicationDuration = Duration.ofHours(6),
        ),
      )
      val configurationEntries = configurations.zipWithIndex.map {
        case ((offset, configuration), index) =>
          offset -> ConfigurationEntry.Accepted(s"submission ID #$index", configuration)
      }
      val configurationLoadTimeout = 5.seconds

      val index = new FakeIndexConfigManagementService(
        currentConfiguration = None,
        streamingConfigurations = configurationEntries,
      )
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)
      val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
        loggerFactory = loggerFactory,
      )

      subscriptionBuilder
        .subscription(configurationLoadTimeout)
        .use { currentLedgerConfiguration =>
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

    "not use the configuration from a rejection" in {
      val configurationEntries = List(
        offset("0123") -> ConfigurationEntry.Rejected(
          submissionId = "submission ID",
          rejectionReason = "rejected because we felt like it",
          proposedConfiguration = Configuration(
            generation = 10,
            timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ZERO),
            maxDeduplicationDuration = Duration.ZERO,
          ),
        )
      )
      val configurationLoadTimeout = 5.seconds

      val index = new FakeIndexConfigManagementService(
        currentConfiguration = None,
        streamingConfigurations = configurationEntries,
      )
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)
      val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
        loggerFactory = loggerFactory,
      )

      subscriptionBuilder
        .subscription(configurationLoadTimeout)
        .use { currentLedgerConfiguration =>
          Delayed.by(100.millis)(()).map { _ =>
            currentLedgerConfiguration.latestConfiguration() should be(None)
            currentLedgerConfiguration.ready.isCompleted should be(false)
            succeed
          }
        }
    }

    "discard rejections once an accepted configuration has been found" in {
      val acceptedConfiguration = Configuration(
        generation = 10,
        timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(10)),
        maxDeduplicationDuration = Duration.ZERO,
      )
      val configurationEntries = List(
        offset("0010") -> ConfigurationEntry.Rejected(
          submissionId = "submission ID #1",
          rejectionReason = "rejected #1",
          proposedConfiguration = Configuration(
            generation = 9,
            timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(5)),
            maxDeduplicationDuration = Duration.ZERO,
          ),
        ),
        offset("0020") -> ConfigurationEntry.Accepted(
          submissionId = "submission ID #2",
          configuration = acceptedConfiguration,
        ),
        offset("01ef") -> ConfigurationEntry.Rejected(
          "submission ID #3",
          "rejected #3",
          Configuration(
            generation = 11,
            timeModel = LedgerTimeModel.reasonableDefault.copy(maxSkew = Duration.ofMinutes(15)),
            maxDeduplicationDuration = Duration.ZERO,
          ),
        ),
      )
      val configurationLoadTimeout = 5.seconds

      val index = new FakeIndexConfigManagementService(
        currentConfiguration = None,
        streamingConfigurations = configurationEntries,
      )
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)
      val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
        loggerFactory = loggerFactory,
      )

      subscriptionBuilder
        .subscription(configurationLoadTimeout)
        .use { currentLedgerConfiguration =>
          currentLedgerConfiguration.ready.map { _ =>
            eventually {
              currentLedgerConfiguration.latestConfiguration() should be(
                Some(acceptedConfiguration)
              )
            }
            succeed
          }
        }
    }

    "give up waiting if the configuration takes too long to appear" in {
      val configurationLoadTimeout = 500.millis
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)
      val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
        indexService = EmptyIndexConfigManagementService,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
        loggerFactory = loggerFactory,
      )

      loggerFactory.assertLogs(
        within = subscriptionBuilder
          .subscription(configurationLoadTimeout)
          .use { currentLedgerConfiguration =>
            currentLedgerConfiguration.ready.isCompleted should be(false)
            scheduler.timePasses(1.second)
            currentLedgerConfiguration.ready.isCompleted should be(true)
            currentLedgerConfiguration.ready.map { _ =>
              currentLedgerConfiguration.latestConfiguration() should be(None)
            }
          },
        assertions = _.warningMessage should include(
          s"No ledger configuration found after $configurationLoadTimeout."
        ),
      )
    }

    "never becomes ready if stopped" in {
      val configurationLoadTimeout = 1.second
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)

      val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
        indexService = EmptyIndexConfigManagementService,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
        loggerFactory = loggerFactory,
      )
      val resource = subscriptionBuilder
        .subscription(configurationLoadTimeout)
        .acquire()

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

    "fail to subscribe if the initial lookup fails" in {
      val lookupFailure = new RuntimeException("It failed.")
      val index = new FailingIndexConfigManagementService(lookupFailure)

      val configurationLoadTimeout = 1.second
      val scheduler = new ExplicitlyTriggeredScheduler(null, NoLogging, null)
      val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
        indexService = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = system.dispatcher,
        loggerFactory = loggerFactory,
      )
      val resource = subscriptionBuilder
        .subscription(configurationLoadTimeout)
        .acquire()

      resource.asFuture
        .andThen { case _ => resource.release() }
        .transform(Success.apply)
        .map { result =>
          inside(result) { case Failure(exception) =>
            exception should be(lookupFailure)
          }
        }
    }
  }
}

object LedgerConfigurationSubscriptionFromIndexSpec {
  private def offset(value: String): ParticipantOffset.Absolute =
    ParticipantOffset.Absolute(Ref.LedgerString.assertFromString(value))

  object EmptyIndexConfigManagementService extends IndexConfigManagementService {
    override def lookupConfiguration()(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[(ParticipantOffset.Absolute, Configuration)]] =
      Future.successful(None)

    override def configurationEntries(
        startExclusive: Option[ParticipantOffset.Absolute]
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(ParticipantOffset.Absolute, ConfigurationEntry), NotUsed] =
      Source.never
  }

  final class FakeIndexConfigManagementService(
      currentConfiguration: Option[(ParticipantOffset.Absolute, Configuration)],
      streamingConfigurations: List[(ParticipantOffset.Absolute, ConfigurationEntry)],
  ) extends IndexConfigManagementService {
    override def lookupConfiguration()(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[(ParticipantOffset.Absolute, Configuration)]] =
      Future.successful(currentConfiguration)

    override def configurationEntries(
        startExclusive: Option[ParticipantOffset.Absolute]
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(ParticipantOffset.Absolute, ConfigurationEntry), NotUsed] = {
      val futureConfigurations = startExclusive match {
        case None => streamingConfigurations
        case Some(offset) => streamingConfigurations.dropWhile(offset.value > _._1.value)
      }
      Source(futureConfigurations).concat(Source.never)
    }
  }

  final class FailingIndexConfigManagementService(lookupFailure: Exception)
      extends IndexConfigManagementService {
    override def lookupConfiguration()(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[(ParticipantOffset.Absolute, Configuration)]] =
      Future.failed(lookupFailure)

    override def configurationEntries(
        startExclusive: Option[ParticipantOffset.Absolute]
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(ParticipantOffset.Absolute, ConfigurationEntry), NotUsed] =
      Source.never
  }
}
