// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services

import java.util.UUID

import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest,
}
import com.daml.platform.sandbox.auth.ServiceCallAuthTests
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

trait SubmitMultiPartyDummyCommand extends TestCommands { self: ServiceCallAuthTests =>

  protected def dummySubmitRequest(
      party: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): SubmitRequest = {
    SubmitRequest(
      dummyMultiPartyCommands(
        wrappedLedgerId,
        s"$serviceCallName-${UUID.randomUUID}",
        party,
        actAs,
        readAs,
      )
        .update(_.commands.applicationId := serviceCallName)
        .commands
    )
  }

  protected def submit(
      token: Option[String],
      party: String,
      actAs: Seq[String],
      readAs: Seq[String],
  ): Future[Empty] =
    stub(CommandSubmissionServiceGrpc.stub(channel), token)
      .submit(dummySubmitRequest(party, actAs, readAs))

}
