// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services

import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
  SubmitResponse,
}
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallWithMainActorAuthTests

import java.util.UUID
import scala.concurrent.Future

trait SubmitDummyCommand extends TestCommands { self: ServiceCallWithMainActorAuthTests =>

  protected def dummySubmitRequest(party: String, userId: String): SubmitRequest =
    SubmitRequest(
      dummyCommands(s"$serviceCallName-${UUID.randomUUID}", party)
        .update(_.commands.userId := userId, _.commands.actAs := Seq(party))
        .commands
    )

  protected def submit(
      token: Option[String],
      party: String,
      userId: String,
  ): Future[SubmitResponse] =
    stub(CommandSubmissionServiceGrpc.stub(channel), token)
      .submit(dummySubmitRequest(party, userId))

}
