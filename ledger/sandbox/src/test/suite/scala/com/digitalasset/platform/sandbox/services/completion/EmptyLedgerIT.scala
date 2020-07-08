// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.completion

import java.time.Duration

import com.daml.api.util.DurationConversion
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundEach
import com.daml.ledger.api.v1.admin.config_management_service.{
  ConfigManagementServiceGrpc,
  GetTimeModelRequest,
  SetTimeModelRequest,
  TimeModel => ProtobufTimeModel
}
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
  CompletionStreamRequest,
  CompletionStreamResponse
}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.participant.state.v1.TimeModel
import com.daml.platform.ApiOffset
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandbox.services.completion.EmptyLedgerIT._
import com.daml.platform.sandboxnext.SandboxNextFixture
import com.daml.platform.testing.StreamConsumer
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.{AsyncWordSpec, Inspectors, Matchers}
import scalaz.syntax.tag._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

final class EmptyLedgerIT
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxNextFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SuiteResourceManagementAroundEach {

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

  "CommandCompletionService gives sensible ledger end on an empty ledger" in {
    val partyA = "partyA"
    val lid = ledgerId().unwrap
    val completionService = CommandCompletionServiceGrpc.stub(channel)
    for {
      end <- completionService.completionEnd(CompletionEndRequest(lid))
      completions <- completionsFromOffset(completionService, lid, List(partyA), end.getOffset)
    } yield {
      end.getOffset.value.absolute.get shouldBe ApiOffset.begin.toHexString
      completions shouldBe empty
    }
  }

  "ConfigManagementService accepts a configuration when none is set" in {
    val partyA = "partyA"
    val lid = ledgerId().unwrap
    val completionService = CommandCompletionServiceGrpc.stub(channel)
    val configService = ConfigManagementServiceGrpc.stub(channel)
    for {
      end <- completionService.completionEnd(CompletionEndRequest(lid))
      completions <- completionsFromOffset(completionService, lid, List(partyA), end.getOffset)
      current <- configService.getTimeModel(GetTimeModelRequest())
      generation = current.configurationGeneration
      timeModel = TimeModel.reasonableDefault
      _ <- configService.setTimeModel(
        SetTimeModelRequest(
          "config-submission",
          Some(Timestamp(30, 0)),
          generation,
          Some(ProtobufTimeModel(
            avgTransactionLatency =
              Some(DurationConversion.toProto(timeModel.avgTransactionLatency)),
            minSkew = Some(DurationConversion.toProto(timeModel.minSkew)),
            maxSkew = Some(DurationConversion.toProto(timeModel.maxSkew))
          ))
        ))
      newEnd <- completionService.completionEnd(CompletionEndRequest(lid))
    } yield {
      newEnd.getOffset.value.absolute.get should not be ApiOffset.begin.toHexString
      completions shouldBe empty
    }
  }
}

object EmptyLedgerIT {
  private val applicationId = classOf[EmptyLedgerIT].getSimpleName

  // How long it takes to download the entire completion stream.
  // Because the stream does not terminate, we use a timeout to determine when the stream
  // is done emitting elements.
  private val completionTimeout = 2.seconds

  private def completionsFromOffset(
      completionService: CommandCompletionServiceGrpc.CommandCompletionServiceStub,
      ledgerId: String,
      parties: Seq[String],
      offset: LedgerOffset,
  )(implicit ec: ExecutionContext): Future[Vector[String]] = {
    new StreamConsumer[CompletionStreamResponse](
      completionService.completionStream(
        CompletionStreamRequest(ledgerId, applicationId, parties, Some(offset)),
        _
      )
    ).within(completionTimeout)
      .map(_.flatMap(_.completions).map(_.commandId))
  }
}
