// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.{SandboxFixtureWithAuth, TestCommands}
import com.google.protobuf.empty.Empty
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class CommandSubmissionServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with TestCommands
    with Matchers
    with Expect {

  behavior of "CommandSubmissionService with authorization"

  it should "deny unauthorized calls" in {
    expect(submit(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(submit(Some(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(submit(Some(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(submit(Some(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(submit(Some(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }

  private val submitter = "alice"

  private def dummySubmitRequest: SubmitRequest =
    SubmitRequest(
      dummyCommands(
        wrappedLedgerId,
        s"${classOf[CommandSubmissionServiceAuthIT].getSimpleName}-${UUID.randomUUID}",
        submitter)
        .update(
          _.commands.applicationId := classOf[CommandSubmissionServiceAuthIT].getSimpleName,
          _.commands.party := submitter)
        .commands)

  private def submit(token: Option[String]): Future[Empty] =
    stub(CommandSubmissionServiceGrpc.stub(channel), token).submit(dummySubmitRequest)

}
