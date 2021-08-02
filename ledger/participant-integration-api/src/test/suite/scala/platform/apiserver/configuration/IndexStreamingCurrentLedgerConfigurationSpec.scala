// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import java.time.Duration

import akka.stream.scaladsl.Source
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
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

final class IndexStreamingCurrentLedgerConfigurationSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
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

      IndexStreamingCurrentLedgerConfiguration
        .owner(index, ledgerConfiguration)
        .use { currentLedgerConfiguration =>
          currentLedgerConfiguration.ready.map { _ =>
            currentLedgerConfiguration.latestConfiguration should be(Some(currentConfiguration))
            succeed
          }
        }
    }

    "stream the latest configuration from the index" in {
      val configurations = Seq(
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

      IndexStreamingCurrentLedgerConfiguration
        .owner(index, ledgerConfiguration)
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

      IndexStreamingCurrentLedgerConfiguration
        .owner(index, ledgerConfiguration)
        .use { ledgerConfigProvider =>
          ledgerConfigProvider.latestConfiguration should be(None)
        }
    }
  }
}

object IndexStreamingCurrentLedgerConfigurationSpec {
  private def offset(value: String): LedgerOffset.Absolute =
    LedgerOffset.Absolute(Ref.LedgerString.assertFromString(value))
}
