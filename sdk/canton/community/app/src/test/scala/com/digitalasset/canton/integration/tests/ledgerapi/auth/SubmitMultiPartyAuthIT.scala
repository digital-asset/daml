// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitMultiPartyDummyCommand

import scala.concurrent.Future

final class SubmitMultiPartyAuthIT
    extends MultiPartyServiceCallAuthTests
    with SubmitMultiPartyDummyCommand {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "CommandSubmissionService#Submit(<multi-party>)"

  override def serviceCall(
      context: ServiceCallContext,
      requestSubmitters: RequestSubmitters,
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    submit(
      context.token,
      requestSubmitters.actAs,
      requestSubmitters.readAs,
      context.userId,
    )

}
