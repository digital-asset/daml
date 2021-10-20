// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.nio.file.Paths

import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

import com.daml.logging.LoggingContext

final class IntegrityCheckerSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "compareStateUpdates" should {
    "call compare if not in index-only mode" in {
      val mockStateUpdates = mock[StateUpdateComparison]
      when(mockStateUpdates.compare()).thenReturn(Future.unit)
      val config = Config.ParseInput.copy(indexOnly = false)
      val instance = createMockIntegrityChecker()

      instance
        .compareStateUpdates(config, mockStateUpdates)
        .transform(
          _ => {
            verify(mockStateUpdates, times(1)).compare()
            succeed
          },
          _ => fail(),
        )
    }

    "skip compare if in index-only mode" in {
      val mockStateUpdates = mock[StateUpdateComparison]
      when(mockStateUpdates.compare()).thenReturn(Future.unit)
      val config = Config.ParseInput.copy(indexOnly = true)
      val instance = createMockIntegrityChecker()

      instance
        .compareStateUpdates(config, mockStateUpdates)
        .transform(
          _ => {
            verify(mockStateUpdates, times(0)).compare()
            succeed
          },
          _ => fail(),
        )
    }
  }

  "createIndexerConfig" should {
    "use the configured jdbcUrl if available" in {
      val configuredJdbcUrl = "aJdbcUrl"
      val config = Config.ParseInput.copy(jdbcUrl = Some(configuredJdbcUrl))
      IntegrityChecker.createIndexerConfig(config).jdbcUrl should be(configuredJdbcUrl)
    }

    "use the default jdbcUrl if none is configured" in {
      val aFilePath = "aFilePath"
      val config = Config.ParseInput.copy(exportFilePath = Paths.get(aFilePath))
      IntegrityChecker.createIndexerConfig(config).jdbcUrl should be(
        IntegrityChecker.defaultJdbcUrl(aFilePath)
      )
    }
  }

  private def createMockIntegrityChecker(): IntegrityChecker[Unit] = {
    val mockCommitStrategySupport = mock[CommitStrategySupport[Unit]]
    val instance = new IntegrityChecker[Unit](_ => mockCommitStrategySupport)
    instance
  }

}
