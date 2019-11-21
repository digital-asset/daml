// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import java.util.UUID

import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

trait SubmitAndWaitDummyCommand extends TestCommands { self: SandboxFixtureWithAuth =>

  def submitter: String

  def appId: String

  protected def issueCommand() = submitAndWait(Option(rwToken(submitter).asHeader()))

  protected def dummySubmitAndWaitRequest: SubmitAndWaitRequest =
    SubmitAndWaitRequest(
      dummyCommands(wrappedLedgerId, s"$appId-${UUID.randomUUID}", submitter)
        .update(_.commands.applicationId := appId, _.commands.party := submitter)
        .commands)

  protected def submitAndWait(token: Option[String]): Future[Empty] =
    stub(CommandServiceGrpc.stub(channel), token).submitAndWait(dummySubmitAndWaitRequest)

  protected lazy val command = issueCommand()

}
