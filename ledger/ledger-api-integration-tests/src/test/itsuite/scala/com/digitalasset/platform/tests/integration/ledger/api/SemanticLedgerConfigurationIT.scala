// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.concurrent.atomic.AtomicInteger

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.MockMessages.ledgerEffectiveTime
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.client.services.commands.{CommandClient, SynchronousCommandClient}
import com.digitalasset.ledger.client.services.configuration.LedgerConfigurationClient
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.tests.integration.ledger.api.commands.MultiLedgerCommandUtils
import com.google.protobuf.empty.Empty
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scalaz.syntax.tag._
import scalapb.lenses
import scalapb.lenses.Lens

class SemanticLedgerConfigurationIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with MultiLedgerCommandUtils
    with SuiteResourceManagementAroundAll
    with TestExecutionSequencerFactory
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {

  override def timeLimit: Span = 5.seconds

  private def configClient(
      ctx: LedgerContext): LedgerConfigurationClient =
    new LedgerConfigurationClient(ctx.ledgerId, ctx.ledgerConfigurationService)

  private def newSyncClient(commandService: CommandService) =
    new SynchronousCommandClient(commandService)

  private val newCommandId: () => String = {
    val atomicInteger = new AtomicInteger()
    () =>
      atomicInteger.incrementAndGet().toString
  }

  private implicit class CommandClientOps(commandClient: CommandClient) {
    def send(request: SubmitRequest) =
      commandClient
        .withTimeProvider(None)
        .trackSingleCommand(request)

  }

  private def createRequest(
                           ctx: LedgerContext,
      changes: Lens[SubmitAndWaitRequest, SubmitAndWaitRequest] => lenses.Mutation[
        SubmitAndWaitRequest]): SubmitAndWaitRequest = {
    submitAndWaitRequest.update(
      _.commands.ledgerId := ctx.ledgerId.unwrap,
      _.commands.commandId := newCommandId(),
      changes
    )
  }

  "a ledger" when {
    "returning a minTTL in LedgerConfiguration" should {
      "accept an MRT just minTTL greater than LET" in allFixtures { context =>
        for {
          config <- configClient(context).getLedgerConfiguration.runWith(Sink.head)(materializer)
          syncClient = newSyncClient(context.commandService)
          request = createRequest(context,
            _.commands.maximumRecordTime.seconds := (ledgerEffectiveTime.seconds + config.minTtl.value.seconds))
          resp <- syncClient.submitAndWait(request)
        } yield (resp should equal(Empty()))
      }

      "not accept an MRT less than minTTL greater than LET" in allFixtures { context =>
        val resF = for {
          config <- configClient(context).getLedgerConfiguration.runWith(Sink.head)(materializer)
          syncClient = newSyncClient(context.commandService)
          request = createRequest(context,
            _.commands.maximumRecordTime.seconds := ledgerEffectiveTime.seconds + config.minTtl.value.seconds - 1)

          resp <- syncClient.submitAndWait(request)
        } yield (resp)

        resF.failed.map { failure =>
          val exception = failure.asInstanceOf[StatusRuntimeException]
          exception.getStatus.getCode should equal(Status.Code.INVALID_ARGUMENT)
        }
      }
    }

    "returning a maxTTL in LedgerConfiguration" should {
      "accept an MRT exactly maxTTL greater than LET" in allFixtures { context =>
        for {
          config <- configClient(context).getLedgerConfiguration.runWith(Sink.head)(materializer)
          syncClient = newSyncClient(context.commandService)
          request = createRequest(context,
            _.commands.maximumRecordTime.seconds := ledgerEffectiveTime.seconds + config.maxTtl.value.seconds)
          resp <- syncClient.submitAndWait(request)
        } yield (resp should equal(Empty()))
      }

      "not accept an MRT more than maxTTL greater than LET" in allFixtures { context =>
        val resF = for {
          config <- configClient(context).getLedgerConfiguration.runWith(Sink.head)(materializer)
          syncClient = newSyncClient(context.commandService)
          request = createRequest(context,
            _.commands.maximumRecordTime.seconds := ledgerEffectiveTime.seconds + config.maxTtl.value.seconds + 1)
          resp <- syncClient.submitAndWait(request)
        } yield (resp)

        resF.failed.map { failure =>
          val exception = failure.asInstanceOf[StatusRuntimeException]
          exception.getStatus.getCode should equal(Status.Code.INVALID_ARGUMENT)
        }
      }

    }

  }

}
