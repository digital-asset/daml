// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import java.util.UUID

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.command_service._
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.{SandboxFixtureWithAuth, TestCommands}
import com.google.protobuf.empty.Empty
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class CommandServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with TestCommands
    with Matchers
    with Expect {

  behavior of "CommandService#SubmitAndWait with authorization"

  it should "deny unauthorized calls" in {
    expect(submitAndWait(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(submitAndWait(Some(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(submitAndWait(Some(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(submitAndWait(Some(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "deny calls with read-only tokens" in {
    expect(submitAndWait(Some(roToken(submitter).asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(submitAndWait(Some(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }

  behavior of "CommandService#SubmitAndWaitForTransaction with authorization"

  it should "deny unauthorized calls" in {
    expect(submitAndWaitForTransaction(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(submitAndWaitForTransaction(Some(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(submitAndWaitForTransaction(Some(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(submitAndWaitForTransaction(Some(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "deny calls with read-only tokens" in {
    expect(submitAndWaitForTransaction(Some(roToken(submitter).asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(submitAndWaitForTransaction(Some(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }

  behavior of "CommandService#SubmitAndWaitForTransactionId with authorization"

  it should "deny unauthorized calls" in {
    expect(submitAndWaitForTransactionId(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(submitAndWaitForTransactionId(Some(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(submitAndWaitForTransactionId(Some(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(submitAndWaitForTransactionId(Some(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "deny calls with read-only tokens" in {
    expect(submitAndWaitForTransactionId(Some(roToken(submitter).asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(submitAndWaitForTransactionId(Some(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }

  behavior of "CommandService#SubmitAndWaitForTransactionTree with authorization"

  it should "deny unauthorized calls" in {
    expect(submitAndWaitForTransactionTree(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(submitAndWaitForTransactionTree(Some(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(submitAndWaitForTransactionTree(Some(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(submitAndWaitForTransactionTree(Some(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "deny calls with read-only tokens" in {
    expect(submitAndWaitForTransactionTree(Some(roToken(submitter).asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(submitAndWaitForTransactionTree(Some(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }

  private val submitter = "alice"

  private def service(token: Option[String]) = stub(CommandServiceGrpc.stub(channel), token)

  private lazy val dummySubmitAndWaitRequest: SubmitAndWaitRequest =
    SubmitAndWaitRequest(
      dummyCommands(
        wrappedLedgerId,
        s"${classOf[CommandServiceAuthIT].getSimpleName}-${UUID.randomUUID}",
        submitter)
        .update(
          _.commands.applicationId := classOf[CommandServiceAuthIT].getSimpleName,
          _.commands.party := submitter)
        .commands)

  private def submitAndWait(token: Option[String]): Future[Empty] =
    service(token).submitAndWait(dummySubmitAndWaitRequest)

  private def submitAndWaitForTransaction(
      token: Option[String]): Future[SubmitAndWaitForTransactionResponse] =
    service(token).submitAndWaitForTransaction(dummySubmitAndWaitRequest)

  private def submitAndWaitForTransactionId(
      token: Option[String]): Future[SubmitAndWaitForTransactionIdResponse] =
    service(token).submitAndWaitForTransactionId(dummySubmitAndWaitRequest)

  private def submitAndWaitForTransactionTree(
      token: Option[String]): Future[SubmitAndWaitForTransactionTreeResponse] =
    service(token).submitAndWaitForTransactionTree(dummySubmitAndWaitRequest)

}
