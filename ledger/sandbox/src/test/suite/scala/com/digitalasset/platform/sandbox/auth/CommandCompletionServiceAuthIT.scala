// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.auth

import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.v1.command_completion_service._
import com.digitalasset.platform.sandbox.Expect
import com.digitalasset.platform.sandbox.services.{
  SandboxFixtureWithAuth,
  SubmitAndWaitDummyCommand,
  TestCommands
}
import com.digitalasset.timer.Delayed
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

final class CommandCompletionServiceAuthIT
    extends AsyncFlatSpec
    with SandboxFixtureWithAuth
    with SuiteResourceManagementAroundAll
    with TestCommands
    with Matchers
    with Expect
    with SubmitAndWaitDummyCommand {

  override val appId = classOf[CommandCompletionServiceAuthIT].getSimpleName

  override val submitter = "alice"

  private def completionEnd(token: Option[String]): Future[CompletionEndResponse] =
    stub(CommandCompletionServiceGrpc.stub(channel), token)
      .completionEnd(new CompletionEndRequest(unwrappedLedgerId))

  behavior of "CommandCompletionService#CompletionEnd with authorization"

  it should "deny unauthorized calls" in {
    expect(completionEnd(None)).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(completionEnd(Option(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(completionEnd(Option(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(completionEnd(Option(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }

  private lazy val completionStreamRequest =
    new CompletionStreamRequest(unwrappedLedgerId, appId, List(submitter), Some(ledgerBegin))

  private def completionStream(token: Option[String]): Future[Unit] =
    command.flatMap(
      _ =>
        streamResult[CompletionStreamResponse](observer =>
          stub(CommandCompletionServiceGrpc.stub(channel), token)
            .completionStream(completionStreamRequest, observer)))

  private def expiringCompletionStream(token: String): Future[Throwable] =
    command.flatMap(
      _ =>
        expectExpiration[CompletionStreamResponse](observer =>
          stub(CommandCompletionServiceGrpc.stub(channel), Some(token))
            .completionStream(completionStreamRequest, observer)))

  behavior of "CommandCompletionService#CompletionStream with authorization"

  it should "deny unauthorized calls" in {
    expect(completionStream(None)).toBeDenied
  }
  it should "deny calls authorized for the wrong party" in {
    expect(completionStream(Option(rwToken("bob").asHeader()))).toBeDenied
  }
  it should "allow authenticated calls" in {
    expect(completionStream(Option(rwToken(submitter).asHeader()))).toSucceed
  }
  it should "deny calls with expired tokens" in {
    expect(completionStream(Option(rwToken(submitter).expired.asHeader()))).toBeDenied
  }
  it should "allow calls with non-expired tokens" in {
    expect(completionStream(Option(rwToken(submitter).expiresTomorrow.asHeader()))).toSucceed
  }
  it should "break a stream in flight upon token expiration" in {
    val _ = Delayed.Future.by(10.seconds)(issueCommand())
    expect(expiringCompletionStream(rwToken(submitter).expiresInFiveSeconds.asHeader())).toSucceed
  }

}
