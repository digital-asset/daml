// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.commands

import java.time.Duration

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll,
  MockMessages => M
}
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.platform.sandbox.TestExecutionSequencerFactory
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import com.digitalasset.platform.sandbox.services.SandboxFixtureSQL
import com.digitalasset.platform.sandbox.utils.LedgerTestingHelpers
import com.digitalasset.util.Ctx
import org.scalatest.{OptionValues, Suite}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CommandTransactionSQLIT
    extends Suite
    with PostgresAroundAll
    with SandboxFixtureSQL
    with SuiteResourceManagementAroundAll
    with OptionValues
    with AkkaBeforeAndAfterAll
    with TestExecutionSequencerFactory {

  private lazy val timeProviderForClient = getTimeProviderForClient(materializer, esf)

  override def nestedSuites: immutable.IndexedSeq[Suite] =
    Vector(
      new CommandTransactionChecks(
        submitAsync,
        "submitting via low level services",
        testPackageId,
        ledgerIdOnServer,
        channel,
        timeProviderForClient,
        materializer,
        esf),
      new CommandTransactionChecks(
        submitSync,
        "submitting via high level service",
        testPackageId,
        ledgerIdOnServer,
        channel,
        timeProviderForClient,
        materializer,
        esf)
    )

  private val testPackageId = config.damlPackageContainer.packageIds.head

  private lazy val commandClient = newCommandClient()

  private lazy val syncCommandClient = CommandServiceGrpc.stub(channel)

  private def newCommandClient(applicationId: String = M.applicationId) =
    new CommandClient(
      CommandSubmissionServiceGrpc.stub(channel),
      CommandCompletionServiceGrpc.stub(channel),
      ledgerIdOnServer,
      applicationId,
      CommandClientConfiguration(1, 1, true, Duration.ofSeconds(10L)),
      Some(timeProviderForClient)
    )

  private def submitAsync(submitRequest: SubmitRequest): Future[Completion] = {
    for {
      tracker <- commandClient.trackCommands[Int](List(submitRequest.getCommands.party))
      completion <- Source
        .single(Ctx(0, submitRequest))
        .via(tracker)
        .runWith(Sink.head)
    } yield completion.value
  }

  private def submitSync(submitRequest: SubmitRequest): Future[Completion] = {
    LedgerTestingHelpers.emptyToCompletion(
      submitRequest.commands.value.commandId,
      syncCommandClient.submitAndWait(
        SubmitAndWaitRequest(submitRequest.commands, submitRequest.traceContext)))
  }
}
