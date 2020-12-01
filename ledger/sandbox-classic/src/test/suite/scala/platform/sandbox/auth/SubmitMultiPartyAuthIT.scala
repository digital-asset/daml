// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

import com.daml.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.daml.platform.sandbox.services.TestCommands

import scala.concurrent.Future

final class SubmitMultiPartyAuthIT extends MultiPartyServiceCallAuthTests with TestCommands {

  override def serviceCallName: String = "CommandSubmissionService#Submit"

  override def serviceCallWithToken(token: Option[String]): Future[Any] = {
    val request = SubmitRequest(
      dummyCommands(wrappedLedgerId, s"$serviceCallName-${UUID.randomUUID}", actAs.head)
        .update(
          _.commands.applicationId := serviceCallName,
          _.commands.party := "",
          _.commands.actAs := actAs,
          _.commands.readAs := readAs,
        )
        .commands)

    stub(CommandSubmissionServiceGrpc.stub(channel), token).submit(request)
  }
}
