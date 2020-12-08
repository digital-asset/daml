// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext.services.completion

import java.io.File
import java.nio.file.Files
import java.time.Duration

import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.{MockMessages, SuiteResourceManagementAroundEach}
import com.daml.ledger.api.v1.admin.package_management_service.{
  PackageManagementServiceGrpc,
  UploadDarFileRequest,
  UploadDarFileResponse
}
import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  AllocatePartyResponse,
  PartyManagementServiceGrpc
}
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.platform.ApiOffset
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandbox.services.TimeModelHelpers.publishATimeModel
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.sandboxnext.services.completion.CompletionServiceWithEmptyLedgerIT._
import com.daml.platform.testing.StreamConsumer
import com.google.protobuf.ByteString
import io.grpc.Channel
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

final class CompletionServiceWithEmptyLedgerIT
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxNextFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SuiteResourceManagementAroundEach {

  // Start with no DAML packages and a large configuration delay, such that we can test the API's
  // behavior on an empty index.
  override protected def config: SandboxConfig =
    super.config.copy(
      damlPackages = List.empty,
      ledgerConfig = super.config.ledgerConfig.copy(
        initialConfigurationSubmitDelay = Duration.ofDays(5),
      ),
      implicitPartyAllocation = false,
    )

  "CommandCompletionService gives sensible ledger end on an empty ledger" in {
    val lid = ledgerId()
    val party = "partyA"
    val completionService = CommandCompletionServiceGrpc.stub(channel)
    for {
      end <- completionService.completionEnd(CompletionEndRequest(lid.unwrap))
      completions <- completionsFromOffset(
        completionService = completionService,
        ledgerId = lid,
        parties = List(party),
        offset = end.getOffset,
        completionTimeout = 2.seconds,
      )
    } yield {
      end.getOffset.value.absolute.get shouldBe ApiOffset.begin.toHexString
      completions shouldBe empty
    }
  }

  "CommandCompletionService can stream completions from the beginning" in {
    val lid = ledgerId()
    val party = "partyA"
    val commandId = "commandId"

    val submissionService = CommandSubmissionServiceGrpc.stub(channel)
    val completionService = CommandCompletionServiceGrpc.stub(channel)
    for {
      end <- completionService.completionEnd(CompletionEndRequest(lid.unwrap))
      _ <- publishATimeModel(channel)
      _ <- uploadDarFile(darFile, channel)
      _ <- allocateParty(party, channel)
      _ <- submissionService.submit(dummyCommands(lid, commandId, party))
      completions <- completionsFromOffset(
        completionService = completionService,
        ledgerId = lid,
        parties = List(party),
        offset = end.getOffset,
        completionTimeout = 2.seconds,
      )
    } yield {
      completions shouldBe Vector(commandId)
    }
  }
}

object CompletionServiceWithEmptyLedgerIT {
  private def uploadDarFile(darFile: File, channel: Channel): Future[UploadDarFileResponse] = {
    val packageService = PackageManagementServiceGrpc.stub(channel)
    val darContents = {
      val inputStream = Files.newInputStream(darFile.toPath)
      try {
        ByteString.readFrom(inputStream)
      } finally {
        inputStream.close()
      }
    }
    packageService.uploadDarFile(UploadDarFileRequest(darContents, "uploadSubmissionId"))
  }

  private def allocateParty(party: String, channel: Channel): Future[AllocatePartyResponse] = {
    val partyService = PartyManagementServiceGrpc.stub(channel)
    partyService.allocateParty(AllocatePartyRequest(party))
  }

  private def completionsFromOffset(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      ledgerId: domain.LedgerId,
      parties: Seq[String],
      offset: LedgerOffset,
      // Because the stream does not terminate, we use a timeout to cut it off when it *should* be
      // done. Obviously, this is prone to error, so the timeouts are fairly generous.
      completionTimeout: FiniteDuration,
  )(implicit ec: ExecutionContext): Future[Vector[String]] = {
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(ledgerId.unwrap, MockMessages.applicationId, parties, Some(offset)),
        _
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completions).map(_.commandId))
  }
}
