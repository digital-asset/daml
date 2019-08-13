// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.example

import java.io.File
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.example.Util.{findOpenPort, toFuture}
import com.digitalasset.example.daml.{Main => M}
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.ledger.api.refinements.ApiTypes.{CommandId, WorkflowId}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.trace_context.TraceContext
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.binding.{Template, Primitive => P}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import com.google.protobuf.empty.Empty

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

import com.digitalasset.ledger.api.domain.LedgerId

object ExampleMain extends App {

  private val dar = new File("./scala-codegen/target/repository/daml-codegen/Main.dar")

  private val ledgerId = Ref.LedgerString.assertFromString("codegen-sbt-example-with-sandbox")

  private val port: Int = findOpenPort().fold(e => throw new IllegalStateException(e), identity)

  private val serverConfig = SandboxConfig.default.copy(
    port = port,
    damlPackages = List(dar),
    timeProviderType = TimeProviderType.WallClock,
    ledgerIdMode = LedgerIdMode.Static(LedgerId(ledgerId)),
  )

  private val server = SandboxServer(serverConfig)
  sys.addShutdownHook(server.close())

  private val asys = ActorSystem()
  private val amat = ActorMaterializer()(asys)
  private val aesf = new AkkaExecutionSequencerPool("clientPool")(asys)

  private implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  private val clientConfig = LedgerClientConfiguration(
    applicationId = ledgerId,
    ledgerIdRequirement = LedgerIdRequirement(ledgerId, enabled = true),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )
  private val clientF: Future[LedgerClient] =
    LedgerClient.singleHost("127.0.0.1", port, clientConfig)(ec, aesf)

  private val alice = P.Party("Alice")
  private val bob = P.Party("Bob")
  private val charlie = P.Party("Charlie")

  private val applicationId = this.getClass.getSimpleName
  private val timeProvider = TimeProvider.UTC
  private val traceContext = TraceContext(1L, 2L, 3L, Some(4L))

  private def submitCommand[T](commandClient: CommandClient)(
      sender: P.Party,
      ttl: Duration,
      command: P.Update[P.ContractId[T]]): Future[Empty] = {
    val commandId = CommandId(uniqueId)
    val workflowId = WorkflowId(uniqueId)
    commandClient.submitSingleCommand(submitRequest(workflowId, commandId, sender, ttl, command))
  }

  private def uniqueId = UUID.randomUUID.toString

  private def ledgerEnd(transactionClient: TransactionClient): Future[LedgerOffset] =
    transactionClient.getLedgerEnd.flatMap(response => toFuture(response.offset))

  private def nextTransaction(transactionClient: TransactionClient)(
      party: P.Party,
      offset: LedgerOffset): Future[Transaction] =
    transactionClient
      .getTransactions(offset, None, transactionFilter(party))
      .take(1)
      .runWith(Sink.head)(amat)

  private def submitRequest[T](
      workflowId: WorkflowId,
      commandId: CommandId,
      party: P.Party,
      ttl: Duration,
      seq: P.Update[P.ContractId[T]]*): SubmitRequest = {
    val now = timeProvider.getCurrentTime
    val commands = Commands(
      ledgerId = ledgerId,
      workflowId = WorkflowId.unwrap(workflowId),
      applicationId = applicationId,
      commandId = CommandId.unwrap(commandId),
      party = P.Party.unwrap(party),
      ledgerEffectiveTime = Some(fromInstant(now)),
      maximumRecordTime = Some(fromInstant(now.plusNanos(ttl.toNanos))),
      commands = seq.map(_.command)
    )
    SubmitRequest(Some(commands), Some(traceContext))
  }

  private def transactionFilter(ps: P.Party*) =
    TransactionFilter(P.Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap)

  private def getContractId[T <: Template[_]](transaction: Transaction): Option[P.ContractId[T]] =
    for {
      event <- transaction.events.headOption: Option[Event]
      created <- event.event.created: Option[CreatedEvent]
      contractId = P.ContractId(created.contractId): P.ContractId[T]
    } yield contractId

  val commandTtl = 2.seconds

  val doneF = for {
    client <- clientF
    commandClient = client.commandClient
    transactionClient = client.transactionClient
    _ = println("Client API initialization completed")

    offset0 <- ledgerEnd(transactionClient): Future[LedgerOffset]
    createCommand = M.CallablePayout(giver = alice, receiver = bob).create
    _ <- submitCommand(commandClient)(alice, commandTtl, createCommand)
    _ = println(s"$alice sent create command: $createCommand")

    tx0 <- nextTransaction(transactionClient)(bob, offset0)
    _ = println(s"$bob received transaction: $tx0")

    contractId <- toFuture(getContractId[M.CallablePayout](tx0))
    exerciseCommand = contractId.exerciseTransfer(actor = bob, newReceiver = charlie)
    offset1 <- ledgerEnd(transactionClient): Future[LedgerOffset]
    _ <- submitCommand(commandClient)(bob, commandTtl, exerciseCommand)
    _ = println(s"$bob sent exercise command: $exerciseCommand")

    tx1 <- nextTransaction(transactionClient)(charlie, offset1): Future[Transaction]
    _ = println(s"$charlie received transaction: $tx1")
  } yield ()

  Await.result(doneF, Duration.Inf)
  Await.result(asys.terminate(), Duration.Inf)
  server.close()
}
