// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.sequencing.SequencerConnectionXPool.SequencerConnectionXPoolError
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class SequencerConnectionXPoolConfigTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with FailOnShutdown
    with ConnectionPoolTestHelpers {

  "SequencerConnectionXPoolConfig" when {
    "validating" should {
      "check the trust threshold" in {
        val config = mkPoolConfig(
          nbConnections = PositiveInt.tryCreate(3),
          trustThreshold = PositiveInt.tryCreate(5),
        )

        inside(config.validate) {
          case Left(SequencerConnectionXPoolError.InvalidConfigurationError(message)) =>
            message shouldBe "Trust threshold (5) must not exceed the number of connections (3)"

        }
      }

      "check uniqueness of connection names" in {
        val config = mkPoolConfig(
          nbConnections = PositiveInt.tryCreate(3),
          trustThreshold = PositiveInt.tryCreate(3),
        )
        val invalidConfig = config.copy(connections =
          config.connections :+ mkDummyConnectionConfig(0, endpointIndexO = Some(100))
        )

        inside(invalidConfig.validate) {
          case Left(SequencerConnectionXPoolError.InvalidConfigurationError(message)) =>
            message shouldBe """Connection name "test-0" is used for more than one connection"""
        }
      }

      "check uniqueness of endpoints" in {
        val config = mkPoolConfig(
          nbConnections = PositiveInt.tryCreate(3),
          trustThreshold = PositiveInt.tryCreate(3),
        )
        val invalidConfig = config.copy(connections =
          config.connections :+ mkDummyConnectionConfig(3, endpointIndexO = Some(1))
        )

        inside(invalidConfig.validate) {
          case Left(SequencerConnectionXPoolError.InvalidConfigurationError(message)) =>
            message shouldBe """Connection endpoint "does-not-exist-1:0" is used for more than one connection"""
        }
      }
    }
  }
}
