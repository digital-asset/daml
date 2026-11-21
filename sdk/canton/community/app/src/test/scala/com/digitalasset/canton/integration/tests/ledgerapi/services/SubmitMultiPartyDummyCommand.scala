// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.services

import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
  SubmitResponse,
}
import com.digitalasset.canton.integration.tests.ledgerapi.auth.ServiceCallAuthTests

import java.util.UUID
import scala.concurrent.Future

trait SubmitMultiPartyDummyCommand extends TestCommands { self: ServiceCallAuthTests =>

  protected def dummySubmitRequest(
      actAs: Seq[String],
      readAs: Seq[String],
      userId: String,
  ): SubmitRequest =
    SubmitRequest(
      dummyMultiPartyCommands(
        s"${serviceCallName.filter(_.isLetterOrDigit)}-${UUID.randomUUID}",
        actAs,
        readAs,
      )
        .update(_.commands.userId := userId)
        .commands
    )

  protected def submit(
      token: Option[String],
      actAs: Seq[String],
      readAs: Seq[String],
      userId: String,
  ): Future[SubmitResponse] =
    stub(CommandSubmissionServiceGrpc.stub(channel), token)
      .submit(dummySubmitRequest(actAs, readAs, userId))

}
