// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.util.UUID

import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.platform.sandbox.auth.ServiceCallWithMainActorAuthTests
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

trait SubmitDummyCommand extends TestCommands { self: ServiceCallWithMainActorAuthTests =>

  protected def dummySubmitRequest(applicationId: String): SubmitRequest =
    SubmitRequest(
      dummyCommands(wrappedLedgerId, s"$serviceCallName-${UUID.randomUUID}", mainActor)
        .update(_.commands.applicationId := applicationId, _.commands.party := mainActor)
        .commands
    )

  protected def submit(
      token: Option[String],
      applicationId: String = serviceCallName,
  ): Future[Empty] =
    stub(CommandSubmissionServiceGrpc.stub(channel), token)
      .submit(dummySubmitRequest(applicationId))

}
