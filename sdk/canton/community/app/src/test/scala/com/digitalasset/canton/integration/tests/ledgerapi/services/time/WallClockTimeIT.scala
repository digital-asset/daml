// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services.time

import com.daml.grpc.GrpcException
import com.daml.ledger.api.v2.testing.time_service.{GetTimeRequest, SetTimeRequest, TimeServiceGrpc}
import com.digitalasset.canton.config.AuthServiceConfig.Wildcard
import com.digitalasset.canton.config.{CantonConfig, ClockConfig}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentSetupPlugin}
import com.digitalasset.canton.ledger.api.util.TimestampConversion.fromInstant
import com.digitalasset.canton.logging.NamedLoggerFactory
import monocle.macros.syntax.lens.*

import java.time.Instant

final class WallClockTimeIT extends CantonFixture {

  registerPlugin(WallClockOverrideConfig(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val unimplemented: PartialFunction[Any, Unit] = { case GrpcException.UNIMPLEMENTED() =>
    ()
  }

  "Time Service" when {
    "server is not in static mode" should {
      "not have getTime available" in { _ =>
        TimeServiceGrpc
          .stub(channel)
          .getTime(GetTimeRequest())
          .failed
          .futureValue should matchPattern(unimplemented)
      }

      "not have setTime available" in { _ =>
        TimeServiceGrpc
          .stub(channel)
          .setTime(
            SetTimeRequest(
              Some(fromInstant(Instant.EPOCH)),
              Some(fromInstant(Instant.EPOCH.plusSeconds(1))),
            )
          )
          .failed
          .futureValue should matchPattern(unimplemented)
      }
    }
  }

  case class WallClockOverrideConfig(
      protected val loggerFactory: NamedLoggerFactory
  ) extends EnvironmentSetupPlugin {
    override def beforeEnvironmentCreated(
        config: CantonConfig
    ): CantonConfig =
      ConfigTransforms
        .updateParticipantConfig("participant1")(
          _.focus(_.testingTime)
            .replace(None)
            .focus(_.ledgerApi.authServices)
            .replace(Seq(Wildcard))
        )(config)
        .focus(_.parameters.clock)
        .replace(ClockConfig.WallClock())
  }
}
