// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.integration.CommunityIntegrationTest

import scala.concurrent.duration.*

trait BasicSequencerTest { self: CommunityIntegrationTest =>
  def name: String

  s"environment using a $name sequencer" should {
    "be able to run a ping" in { implicit env =>
      import env.*

      val duration = participant1.health.ping(participant2, timeout = 30.seconds)

      logger.info(s"Successful ping took ${duration.toMillis}ms")
    }

    "be able to run a bong" in { implicit env =>
      import env.*

      val duration =
        participant1.testing.bong(
          Set(participant1, participant2),
          timeout = 60.seconds,
          levels = 3,
        )

      logger.info(s"Successful bong took ${duration.toMillis}ms")
    }
  }
}
