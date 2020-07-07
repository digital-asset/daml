// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.completion

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.testing.StreamConsumer
import org.scalatest.{AsyncWordSpec, Inspectors, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scalaz.syntax.tag._

class EmptyLedgerIT
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxNextFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SuiteResourceManagementAroundAll {

  // Start with empty daml packages and a large configuration delay, such that we can test the API's behavior
  // on an empty index
  override protected def config: SandboxConfig =
    super.config.copy(
      damlPackages = List.empty,
      ledgerConfig = super.config.ledgerConfig.copy(
        initialConfigurationSubmitDelay = Duration.ofDays(5)
      ),
      implicitPartyAllocation = false
    )

  private[this] val applicationId = getClass.getSimpleName

  // How long it takes to download the entire completion stream.
  // Because the stream does not terminate, we use a timeout to determine when the stream
  // is done emitting elements.
  private[this] val completionTimeout = 2.seconds

  private[this] def completionsFromOffset(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      ledgerId: String,
      parties: Seq[String],
      offset: LedgerOffset,
  ): Future[Vector[String]] = {
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(ledgerId, applicationId, parties, Some(offset)),
        _
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completions).map(_.commandId))
  }

  "CommandCompletionService gives sensible ledger end on an empty ledger" in {

    val partyA = "partyA"
    val lid = ledgerId().unwrap
    val completionService = CommandCompletionServiceGrpc.stub(channel)
    for {
      end <- completionService.completionEnd(CompletionEndRequest(lid))
      completions <- completionsFromOffset(completionService, lid, List(partyA), end.getOffset)
    } yield {
      completions shouldBe empty
    }
  }
}
