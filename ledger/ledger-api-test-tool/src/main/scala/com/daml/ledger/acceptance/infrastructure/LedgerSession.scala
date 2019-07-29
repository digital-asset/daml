// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import java.time.Clock
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.digitalasset.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  PartyManagementServiceGrpc
}
import com.digitalasset.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.digitalasset.ledger.api.v1.commands.Command.Command.{Create, Exercise}
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, CreateCommand, ExerciseCommand}
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TransactionFilter
}
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetLedgerEndRequest,
  GetTransactionsRequest,
  GetTransactionsResponse,
  TransactionServiceGrpc
}
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.google.protobuf.timestamp.Timestamp
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

private[acceptance] sealed abstract case class LedgerSession(
    configuration: LedgerSessionConfiguration) {

  protected val channel: ManagedChannel

  def close(): Unit

  private lazy val commandService = CommandServiceGrpc.stub(channel)
  private lazy val transactionService = TransactionServiceGrpc.stub(channel)
  private lazy val ledgerIdentityService = LedgerIdentityServiceGrpc.stub(channel)
  private lazy val partyManagementService = PartyManagementServiceGrpc.stub(channel)
  private lazy val timeService = TimeServiceGrpc.stub(channel)

  private[infrastructure] def ledgerId()(implicit context: ExecutionContext): Future[String] =
    for {
      response <- ledgerIdentityService.getLedgerIdentity(new GetLedgerIdentityRequest)
    } yield response.ledgerId

  private[infrastructure] def ledgerEnd()(
      implicit context: ExecutionContext): Future[LedgerOffset] =
    for {
      id <- ledgerId
      response <- transactionService.getLedgerEnd(new GetLedgerEndRequest(id))
    } yield response.offset.get

  private[infrastructure] def transactionFilter(party: String, templateIds: Seq[Identifier]) =
    new TransactionFilter(Map(party -> filter(templateIds)))

  private[infrastructure] def filter(templateIds: Seq[Identifier]): Filters =
    new Filters(if (templateIds.isEmpty) None else Some(new InclusiveFilters(templateIds)))

  private[infrastructure] def allocateParty()(implicit context: ExecutionContext): Future[String] =
    partyManagementService.allocateParty(new AllocatePartyRequest()).map(_.partyDetails.get.party)

  private[infrastructure] def allocateParties(n: Int)(
      implicit context: ExecutionContext): Future[Vector[String]] =
    Future.sequence(Vector.fill(n)(allocateParty()))

  private[infrastructure] def transactionsUntilNow(
      begin: LedgerOffset,
      party: String,
      templateIds: Identifier*)(implicit context: ExecutionContext): Future[Vector[Transaction]] =
    for {
      id <- ledgerId()
      responses <- FiniteStreamObserver[GetTransactionsResponse](
        transactionService
          .getTransactions(
            new GetTransactionsRequest(
              ledgerId = id,
              begin = Some(begin),
              end = Some(LedgerSession.ledgerEnd),
              filter = Some(transactionFilter(party, templateIds)),
              verbose = true
            ),
            _
          ))
    } yield responses.flatMap(_.transactions)

  private[infrastructure] def clock()(implicit context: ExecutionContext): Future[Clock] =
    for (id <- ledgerId()) yield new LedgerClock(id, timeService)

  private def submitAndWaitCommand[A](service: SubmitAndWaitRequest => Future[A])(
      party: String,
      command: Command.Command)(implicit context: ExecutionContext): Future[A] =
    for {
      id <- ledgerId()
      clock <- clock()
      now = clock.instant()
      inFiveSeconds = now.plusSeconds(5)
      a <- service(
        new SubmitAndWaitRequest(
          Some(new Commands(
            ledgerId = id,
            applicationId = UUID.randomUUID().toString,
            commandId = UUID.randomUUID().toString,
            party = party,
            ledgerEffectiveTime = Some(new Timestamp(now.getEpochSecond, now.getNano)),
            maximumRecordTime =
              Some(new Timestamp(inFiveSeconds.getEpochSecond, inFiveSeconds.getNano)),
            commands = Seq(new Command(command))
          ))))
    } yield a

  private def submitAndWait(party: String, command: Command.Command)(
      implicit ec: ExecutionContext): Future[Unit] =
    submitAndWaitCommand(commandService.submitAndWait)(party, command).map(_ => ())

  private def submitAndWaitForTransaction[A](party: String, command: Command.Command)(
      f: Transaction => A)(implicit ec: ExecutionContext): Future[A] =
    submitAndWaitCommand(commandService.submitAndWaitForTransaction)(party, command).map(r =>
      f(r.transaction.get))

  private def createCommand(templateId: Identifier, args: Map[String, Value.Sum]): Command.Command =
    Create(
      new CreateCommand(
        Some(templateId),
        Some(
          new Record(
            fields = args.map {
              case (label, value) =>
                new RecordField(label, Some(new Value(value)))
            }(collection.breakOut)
          ))
      )
    )

  private[infrastructure] def create(
      party: String,
      templateId: Identifier,
      args: Map[String, Value.Sum])(implicit context: ExecutionContext): Future[String] =
    submitAndWaitForTransaction(party, createCommand(templateId, args)) {
      _.events.collect { case Event(Created(e)) => e.contractId }.head
    }

  private def exerciseCommand(
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum]): Command.Command =
    Exercise(
      new ExerciseCommand(
        templateId = Some(templateId),
        contractId = contractId,
        choice = choice,
        choiceArgument = Some(
          new Value(
            Value.Sum.Record(new Record(
              fields = args.map {
                case (label, value) =>
                  new RecordField(label, Some(new Value(value)))
              }(collection.breakOut)
            ))))
      )
    )

  private[infrastructure] def exercise(
      party: String,
      templateId: Identifier,
      contractId: String,
      choice: String,
      args: Map[String, Value.Sum]
  )(implicit context: ExecutionContext): Future[Unit] =
    submitAndWait(party, exerciseCommand(templateId, contractId, choice, args))

}

private[acceptance] object LedgerSession {

  private val logger = LoggerFactory.getLogger(classOf[LedgerSession])

  final case class UnsupportedConfiguration(configuration: LedgerSessionConfiguration)
      extends RuntimeException {
    override val getMessage: String =
      s"Unsupported configuration to instantiate the channel ($configuration)"
  }

  private[this] val channels = TrieMap.empty[LedgerSessionConfiguration, LedgerSession]

  private val ledgerEnd = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))

  private def create(configuration: LedgerSessionConfiguration): Try[LedgerSession] =
    configuration match {
      case LedgerSessionConfiguration.Managed(config) if config.jdbcUrl.isDefined =>
        Failure(new IllegalArgumentException("The Postgres-backed sandbox is not yet supported"))
      case LedgerSessionConfiguration.Managed(config) if config.port != 0 =>
        Failure(new IllegalArgumentException("The sandbox port must be 0"))
      case LedgerSessionConfiguration.Managed(config) =>
        Try(spinUpManagedSandbox(config))
      case _ =>
        Failure(UnsupportedConfiguration(configuration))
    }

  def getOrCreate(configuration: LedgerSessionConfiguration): Try[LedgerSession] =
    Try(channels.getOrElseUpdate(configuration, create(configuration).get))

  def closeAll(): Unit =
    for ((_, session) <- channels) {
      session.close()
    }

  private def spinUpManagedSandbox(config: SandboxConfig): LedgerSession = {
    logger.debug("Starting a new managed ledger session...")
    val threadFactoryPoolName = "grpc-event-loop"
    val daemonThreads = true
    val threadFactory: DefaultThreadFactory =
      new DefaultThreadFactory(threadFactoryPoolName, daemonThreads)
    logger.trace(
      s"gRPC thread factory instantiated with pool '$threadFactoryPoolName' (daemon threads: $daemonThreads)")
    val threadCount = Runtime.getRuntime.availableProcessors
    val eventLoopGroup: NioEventLoopGroup =
      new NioEventLoopGroup(threadCount, threadFactory)
    logger.trace(
      s"gRPC event loop thread group instantiated with $threadCount threads using pool '$threadFactoryPoolName'")
    val sandbox = SandboxServer(config)
    logger.trace(s"Sandbox started on port ${sandbox.port}!")
    val managedChannel = NettyChannelBuilder
      .forAddress("127.0.0.1", sandbox.port)
      .eventLoopGroup(eventLoopGroup)
      .usePlaintext()
      .directExecutor()
      .build()
    logger.trace("Sandbox communication channel instantiated")
    new LedgerSession(LedgerSessionConfiguration.Managed(config.copy(port = sandbox.port))) {
      private[this] val logger = LoggerFactory.getLogger(classOf[LedgerSession])
      override final val channel: ManagedChannel = managedChannel
      override final def close(): Unit = {
        logger.trace("Closing managed ledger session...")
        channel.shutdownNow()
        if (!channel.awaitTermination(5L, TimeUnit.SECONDS)) {
          sys.error(
            "Unable to shutdown channel to a remote API under tests. Unable to recover. Terminating.")
        }
        logger.trace("Sandbox communication channel shut down.")
        if (!eventLoopGroup
            .shutdownGracefully(0, 0, TimeUnit.SECONDS)
            .await(10L, TimeUnit.SECONDS)) {
          sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
        }
        logger.trace("Sandbox event loop group shut down.")
        sandbox.close()
        logger.trace("Sandbox fully shut down.")
      }
    }
  }

}
