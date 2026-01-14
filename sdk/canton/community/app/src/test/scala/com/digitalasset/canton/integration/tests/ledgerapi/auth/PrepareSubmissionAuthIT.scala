// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitDummyPreparedSubmission

import scala.concurrent.Future

// PrepareSubmission authorizes like a read only command
final class PrepareSubmissionAuthIT
    extends ReadOnlyServiceCallAuthTests
    with SubmitDummyPreparedSubmission
    with ExecuteAsAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "InteractiveSubmissionService#PrepareSubmission"

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    prepareSubmission(context.token, getMainActorId, context.userId)
}
