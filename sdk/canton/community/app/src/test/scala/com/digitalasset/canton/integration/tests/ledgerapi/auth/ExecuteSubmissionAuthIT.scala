// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitDummyPreparedSubmission
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

// ExecuteSubmission authorizes like a submission command
final class ExecuteSubmissionAuthIT
    extends SyncServiceCallAuthTests
    with SubmitDummyPreparedSubmission {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectInvalidArgument(f)

  override def serviceCallName: String = "InteractiveSubmissionService#ExecuteSubmission"

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    for {
      preparedSubmission <- prepareSubmission(
        canActAsMainActor.token,
        getMainActorId,
        context.userId,
      )
      executeResp <- executeSubmission(context.token, context.userId, preparedSubmission)
    } yield executeResp

  }

}
