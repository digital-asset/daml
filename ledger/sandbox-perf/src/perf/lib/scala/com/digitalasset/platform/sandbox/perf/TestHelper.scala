// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import java.io.File
import java.util.UUID

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.commands.{Command, Commands}
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.v1.trace_context.TraceContext
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.perf.util.DarUtil
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.Future
import scala.concurrent.duration._

trait TestHelper {

  val darFile: File = new File("ledger/sandbox-perf/LargeTransaction.dar")

  val largeTxPackageId: PackageId = DarUtil.getPackageId(darFile)

  val ledgerId: String = "ledger server"
  val applicationId: String = "app1"
  val ledgerEffectiveTime = Some(Timestamp(0L, 0))
  val maximumRecordTime = ledgerEffectiveTime.map(x => x.copy(seconds = x.seconds + 30L))
  val traceContext = Some(TraceContext(1L, 2L, 3L, Some(4L)))

  val party = "party"
  val rangeOfIntsTemplateId =
    Identifier(
      packageId = largeTxPackageId.underlyingString,
      name = "LargeTransaction.RangeOfInts",
      moduleName = "LargeTransaction",
      entityName = "RangeOfInts")

  val listUtilTemplateId = Identifier(
    packageId = largeTxPackageId.underlyingString,
    name = "LargeTransaction.ListUtil",
    moduleName = "LargeTransaction",
    entityName = "ListUtil")

  val setupTimeout = 30.seconds
  val perfTestTimeout = 5.minutes

  val transactionFilter = TransactionFilter(Map(party -> Filters()))

  val IdentifierEqual = new scalaz.Equal[Identifier] {
    override def equal(a1: Identifier, a2: Identifier): Boolean =
      a1.packageId == a2.packageId &&
        a1.moduleName == a2.moduleName &&
        a1.entityName == a2.entityName
  }

  def submitAndWaitRequest(
      command: Command.Command,
      commandId: String,
      workflowId: String): SubmitAndWaitRequest = {
    val commands = Commands(
      ledgerId = ledgerId,
      workflowId = workflowId,
      applicationId = applicationId,
      commandId = commandId,
      ledgerEffectiveTime = ledgerEffectiveTime,
      maximumRecordTime = maximumRecordTime,
      party = party,
      commands = Seq(Command(command))
    )
    SubmitAndWaitRequest(Some(commands), traceContext = traceContext)
  }

  def rangeOfIntsCreateCommand(
      state: PerfBenchState,
      workflowId: String,
      contractSize: Int): Future[Unit] = {
    val createCmd =
      LargeTransactionCommands.rangeOfIntsCreateCommand(rangeOfIntsTemplateId, 0, 1, contractSize)
    submit(state, createCmd, "create-" + uniqueId(), workflowId)
  }

  def rangeOfIntsExerciseCommand(
      state: PerfBenchState,
      workflowId: String,
      choice: String,
      args: Option[Value]): Future[Unit] = {
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val ec = state.mat.executionContext
    for {
      contractId <- firstActiveContractId(state, rangeOfIntsTemplateId, workflowId)
      exerciseCmd = LargeTransactionCommands.exerciseCommand(
        rangeOfIntsTemplateId,
        contractId,
        choice,
        args)
      _ <- submit(state, exerciseCmd, "exercise-" + uniqueId(), workflowId)
    } yield ()
  }

  def firstActiveContractId(
      state: PerfBenchState,
      templateId: Identifier,
      workflowId: String): Future[String] =
    activeContractIds(state, workflowId, templateId).runWith(Sink.head)(state.mat)

  def submit(
      state: PerfBenchState,
      command: Command.Command,
      commandId: String,
      workflowId: String = ""): Future[Unit] = {
    val request: SubmitAndWaitRequest = submitAndWaitRequest(command, commandId, workflowId)
    state.ledger.commandService.submitAndWait(request).map(_ => ())(DirectExecutionContext)
  }

  def activeContractIds(
      state: PerfBenchState,
      workflowId: String,
      templateId: Identifier): Source[String, Future[String]] =
    new ActiveContractSetClient(state.ledger.ledgerId, state.ledger.acsService)(state.esf)
      .getActiveContracts(transactionFilter)
      .filter(_.workflowId == workflowId)
      .mapConcat(extractContractId(templateId))

  def extractContractId(templateId: Identifier)(
      response: GetActiveContractsResponse): List[String] =
    response.activeContracts.toList.collect {
      case CreatedEvent(_, contractId, Some(actualTemplateId), _, _)
          if IdentifierEqual.equal(actualTemplateId, templateId) =>
        contractId
    }

  def listUtilCreateCommand(state: PerfBenchState, workflowId: String): Future[Unit] = {
    val createCmd = LargeTransactionCommands.createCommand(listUtilTemplateId)
    submit(state, createCmd, "create-" + uniqueId(), workflowId)
  }

  def listUtilExerciseSizeCommand(
      state: PerfBenchState,
      templateId: Identifier,
      workflowId: String,
      n: Int): Future[Unit] = {
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val ec = state.mat.executionContext
    for {
      contractId <- firstActiveContractId(state, templateId, workflowId)
      exerciseCmd = LargeTransactionCommands.exerciseSizeCommand(templateId, contractId, n)
      _ <- submit(state, exerciseCmd, "exercise-" + uniqueId(), workflowId)
    } yield ()
  }

  def uniqueId(): String = UUID.randomUUID().toString

}

object TestHelper extends TestHelper
