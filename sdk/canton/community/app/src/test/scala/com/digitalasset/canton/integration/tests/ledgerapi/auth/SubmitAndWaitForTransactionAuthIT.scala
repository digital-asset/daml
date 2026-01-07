// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommand

import scala.concurrent.Future

final class SubmitAndWaitForTransactionAuthIT
    extends SyncServiceCallAuthTests
    with SubmitAndWaitDummyCommand
    with ExecuteAsAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "CommandService#SubmitAndWaitForTransaction"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    submitAndWaitForTransaction(
      context.token,
      context.userId,
      party = getMainActorId,
    )
  }
}
