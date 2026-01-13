// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.testing.time_service.{SetTimeRequest, TimeServiceGrpc}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class SetTimeAuthIT extends AdminServiceCallAuthTests with TimeAuth {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "TimeService#SetTime"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    for {
      response <- loadTimeNow(context.token)
      currentTime = response
        .getOrElse(throw new RuntimeException("No response was received."))
        .currentTime
      _ <- stub(TimeServiceGrpc.stub(channel), context.token)
        .setTime(
          new SetTimeRequest(
            currentTime = currentTime,
            newTime = currentTime,
          )
        )
    } yield ()

  }

}
