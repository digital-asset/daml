// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitMultiPartyDummyCommand

import scala.concurrent.Future

final class SubmitAndWaitMultiPartyAuthIT
    extends MultiPartyServiceCallAuthTests
    with SubmitAndWaitMultiPartyDummyCommand {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "CommandService#SubmitAndWait(<multi-party>)"

  override def serviceCall(
      context: ServiceCallContext,
      requestSubmitters: RequestSubmitters,
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    submitAndWait(
      context.token,
      requestSubmitters.actAs,
      requestSubmitters.readAs,
      context.userId,
    )
  }

}
