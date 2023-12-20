// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands.DeduplicationPeriod
import com.daml.ledger.api.v1.commands.{Command, Commands}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.package_service.GetPackageStatusResponse
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TransactionFilter}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{ClientConfig, ProcessingTimeout}
import com.digitalasset.canton.ledger.api.refinements.ApiTypes.{ApplicationId, WorkflowId}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.{Done, NotUsed}
import org.slf4j.event.Level
import scalaz.syntax.tag.*

import java.time.Duration
import java.util.UUID
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success}

/** Extract from connection for only submitting functionality */
trait LedgerSubmit extends FlagCloseableAsync {
  def submitCommand(
      command: Seq[Command],
      commandId: Option[String] = None,
      workflowId: Option[WorkflowId] = None,
      deduplicationTime: Option[NonNegativeFiniteDuration] = None,
      timeout: Option[Duration] = None,
  )(implicit traceContext: TraceContext): Future[CommandResult]

  def submitAsync(
      commands: Seq[Command],
      commandId: Option[String] = None,
      workflowId: Option[WorkflowId] = None,
      deduplicationTime: Option[NonNegativeFiniteDuration] = None,
  )(implicit traceContext: TraceContext): Future[Unit]
}

/** Subscription for reading the ledger */
trait LedgerSubscription extends FlagCloseableAsync with NamedLogging {
  val completed: Future[Done]
}

trait LedgerAcs extends LedgerSubmit {
  def sender: Party
  def activeContracts(
      filter: TransactionFilter = LedgerConnection.transactionFilter(sender)
  ): Future[(Seq[CreatedEvent], LedgerOffset)]
  def getPackageStatus(packageId: String): Future[GetPackageStatusResponse]
}

trait LedgerConnection extends LedgerSubmit with LedgerAcs {
  def ledgerEnd: Future[LedgerOffset]
  def subscribe(
      subscriptionName: String,
      offset: LedgerOffset,
      filter: TransactionFilter = LedgerConnection.transactionFilter(sender),
  )(f: Transaction => Unit): LedgerSubscription
  def subscribeAsync(
      subscriptionName: String,
      offset: LedgerOffset,
      filter: TransactionFilter = LedgerConnection.transactionFilter(sender),
  )(f: Transaction => Future[Unit]): LedgerSubscription
  def subscription[_T](
      subscriptionName: String,
      offset: LedgerOffset,
      filter: TransactionFilter = LedgerConnection.transactionFilter(sender),
  )(mapOperator: Flow[Transaction, Any, ?]): LedgerSubscription

  def subscribeTree(
      subscriptionName: String,
      offset: LedgerOffset,
      filter: Seq[Any] = Seq(sender),
  )(f: TransactionTree => Unit): LedgerSubscription
  def subscribeAsyncTree(
      subscriptionName: String,
      offset: LedgerOffset,
      filter: Seq[Any] = Seq(sender),
  )(f: TransactionTree => Future[Unit]): LedgerSubscription
  def subscriptionTree[_T](
      subscriptionName: String,
      offset: LedgerOffset,
      filter: Seq[Any] = Seq(sender),
  )(mapOperator: Flow[TransactionTree, Any, ?]): LedgerSubscription

  def transactionById(id: String): Future[Option[Transaction]]

  def allocatePartyViaLedgerApi(hint: Option[String], displayName: Option[String]): Future[PartyId]

}

object LedgerConnection {
  def createLedgerClient(
      applicationId: ApplicationId,
      config: ClientConfig,
      commandClientConfiguration: CommandClientConfiguration,
      tracerProvider: TracerProvider,
      loggerFactory: NamedLoggerFactory,
      token: Option[String] = None,
  )(implicit
      ec: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
  ): Future[LedgerClient] = {
    val clientConfig = LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement(None),
      commandClient = commandClientConfiguration,
      token = token,
    )
    val clientChannelConfig = LedgerClientChannelConfiguration(
      sslContext = config.tls.map(x => ClientChannelBuilder.sslContext(x)),
      // Hard-coding the maximum value (= 2GB).
      // If a limit is needed, because an application can't handle transactions at that size,
      // the participants should agree on a lower limit and enforce that through domain parameters.
      maxInboundMessageSize = Int.MaxValue,
    )

    val builder = clientChannelConfig
      .builderFor(config.address, config.port.unwrap)
      .executor(ec)
      .intercept(
        GrpcTelemetry.builder(tracerProvider.openTelemetry).build().newClientInterceptor()
      )
    LedgerClient(builder.build(), clientConfig, loggerFactory)
  }

  def apply(
      clientConfig: ClientConfig,
      applicationId: ApplicationId,
      maxRetries: Int,
      senderParty: Party,
      defaultWorkflowId: WorkflowId,
      commandClientConfiguration: CommandClientConfiguration,
      token: Option[String],
      processingTimeouts: ProcessingTimeout,
      loggerFactoryForLedgerConnectionOverride: NamedLoggerFactory,
      tracerProvider: TracerProvider,
      futureSupervisor: FutureSupervisor,
      overrideRetryable: PartialFunction[Status, Boolean] = PartialFunction.empty,
  )(implicit
      ec: ExecutionContextExecutor,
      as: ActorSystem,
      sequencerPool: ExecutionSequencerFactory,
  ): LedgerConnection & NamedLogging =
    new LedgerConnection with NamedLogging {
      protected val loggerFactory: NamedLoggerFactory = loggerFactoryForLedgerConnectionOverride

      override protected def timeouts: ProcessingTimeout = processingTimeouts

      private val client = {
        import TraceContext.Implicits.Empty.*
        processingTimeouts.unbounded.await(
          s"Creation of the ledger client",
          logFailing = Some(Level.WARN),
        )(
          createLedgerClient(
            applicationId,
            clientConfig,
            commandClientConfiguration,
            tracerProvider,
            loggerFactory,
            token,
          )
        )
      }
      private val ledgerId = client.ledgerId
      private val commandClient = client.commandClient

      private val transactionClient = client.transactionClient

      private val commandSubmitterWithRetry =
        new CommandSubmitterWithRetry(
          maxRetries,
          client.commandServiceClient,
          futureSupervisor,
          processingTimeouts,
          loggerFactoryForLedgerConnectionOverride,
          overrideRetryable,
        )

      override def sender: Party = senderParty

      override def ledgerEnd: Future[LedgerOffset] =
        transactionClient.getLedgerEnd().flatMap(response => toFuture(response.offset))

      override def activeContracts(
          filter: TransactionFilter
      ): Future[(Seq[CreatedEvent], LedgerOffset)] = {
        val activeContractsRequest = client.activeContractSetClient.getActiveContracts(filter)
        activeContractsRequest.toMat(Sink.seq)(Keep.right).run().map { responseSequence =>
          val offset = responseSequence
            .map(_.offset)
            .lastOption
            // according to spec, should always be defined in last message of stream
            .getOrElse(
              throw new RuntimeException(
                "Expected to have offset in the last message of the acs stream but didn't have one!"
              )
            )
          val active = responseSequence.flatMap(_.activeContracts)
          (active, LedgerOffset(value = LedgerOffset.Value.Absolute(offset)))
        }
      }

      override def submitCommand(
          command: Seq[Command],
          commandId: Option[String] = None,
          workflowId: Option[WorkflowId] = None,
          deduplicationTime: Option[NonNegativeFiniteDuration] = None,
          timeout: Option[Duration] = None,
      )(implicit traceContext: TraceContext): Future[CommandResult] = {
        val fullCommand =
          commandsOf(
            sender,
            commandId,
            workflowId.getOrElse(defaultWorkflowId),
            deduplicationTime,
            command,
          )
        val commandIdA = fullCommand.commandId
        logger.debug(s"Submitting command [$commandIdA]")
        val defaultTimeout = commandClientConfiguration.defaultDeduplicationTime
        val commandTimeout = timeout
          .map(Ordering[java.time.Duration].min(_, defaultTimeout))
          .getOrElse(defaultTimeout)
          .toScala

        commandSubmitterWithRetry
          .submitCommands(fullCommand, commandTimeout)
          .thereafter { outcome =>
            outcome.fold(
              e => logger.error(s"Command failed [$commandIdA] badly due to an exception", e),
              {
                case CommandResult.Success(transactionId) =>
                  logger.debug(
                    s"Command [$commandIdA] succeeded with transactionId=$transactionId"
                  )
                case CommandResult.Failed(commandId, errorStatus) =>
                  logger.info(show"Command [$commandId] failed with status=$errorStatus")
                case CommandResult.MaxRetriesReached(commandId, lastErrorStatus) =>
                  logger.info(
                    show"Command [$commandId] reached max-retries. Last error status=$lastErrorStatus"
                  )
                case CommandResult.AbortedDueToShutdown =>
                  logger.info(s"Command [$commandIdA] was aborted due to service shutdown")
              },
            )
          }
      }

      override def submitAsync(
          commands: Seq[Command],
          commandId: Option[String] = None,
          workflowId: Option[WorkflowId] = None,
          deduplicationTime: Option[NonNegativeFiniteDuration],
      )(implicit traceContext: TraceContext): Future[Unit] = {
        val fullCommand =
          commandsOf(
            sender,
            commandId,
            workflowId.getOrElse(defaultWorkflowId),
            deduplicationTime,
            commands,
          )
        val commandIdA = fullCommand.commandId
        logger.debug(s"Submitting command [$commandIdA] asynchronously")
        commandClient.submitSingleCommand(SubmitRequest(Some(fullCommand))).map(_ => ())
      }

      def commandsOf(
          party: Party,
          commandId: Option[String],
          workflowId: WorkflowId,
          deduplicationTime: Option[NonNegativeFiniteDuration],
          seq: Seq[Command],
      ): Commands =
        Commands(
          ledgerId = ledgerId.unwrap,
          workflowId = WorkflowId.unwrap(workflowId),
          applicationId = ApplicationId.unwrap(applicationId),
          commandId = commandId.getOrElse(uniqueId),
          party = party.getValue,
          deduplicationPeriod = deduplicationTime
            .map(dt => DeduplicationPeriod.DeduplicationDuration(dt.toProtoPrimitive))
            .getOrElse(DeduplicationPeriod.Empty),
          commands = seq,
        )

      override def subscribe(
          subscriptionName: String,
          offset: LedgerOffset,
          filter: TransactionFilter = transactionFilter(sender),
      )(f: Transaction => Unit): LedgerSubscription =
        subscription(subscriptionName, offset, filter)({
          Flow[Transaction].map(f)
        })

      override def subscribeAsync(
          subscriptionName: String,
          offset: LedgerOffset,
          filter: TransactionFilter = transactionFilter(sender),
      )(f: Transaction => Future[Unit]): LedgerSubscription =
        subscription(subscriptionName, offset, filter)({
          Flow[Transaction].mapAsync(1)(f)
        })

      override def transactionById(id: String): Future[Option[Transaction]] =
        client.transactionClient.getFlatTransactionById(id, Seq(sender.getValue), token).map {
          resp =>
            resp.transaction
        }

      override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = List[AsyncOrSyncCloseable](
        SyncCloseable("commandSubmitterWithRetry", commandSubmitterWithRetry.close()),
        SyncCloseable("ledgerClient", client.close()),
      )

      override def subscription[_T](
          subscriptionName: String,
          offset: LedgerOffset,
          filter: TransactionFilter = transactionFilter(sender),
      )(mapOperator: Flow[Transaction, Any, ?]): LedgerSubscription =
        makeSubscription(
          transactionClient.getTransactions(offset, None, filter),
          mapOperator,
          subscriptionName,
        )

      override def subscribeTree(
          subscriptionName: String,
          offset: LedgerOffset,
          filterParty: Seq[Any] = Seq(sender),
      )(f: TransactionTree => Unit): LedgerSubscription =
        subscriptionTree(subscriptionName, offset, filterParty)({
          Flow[TransactionTree].map(f)
        })

      override def subscribeAsyncTree(
          subscriptionName: String,
          offset: LedgerOffset,
          filterParty: Seq[Any] = Seq(sender),
      )(f: TransactionTree => Future[Unit]): LedgerSubscription =
        subscriptionTree(subscriptionName, offset, filterParty)({
          Flow[TransactionTree].mapAsync(1)(f)
        })

      override def subscriptionTree[_T](
          subscriptionName: String,
          offset: LedgerOffset,
          filterParty: Seq[Any] = Seq(sender),
      )(mapOperator: Flow[TransactionTree, Any, ?]): LedgerSubscription =
        makeSubscription(
          transactionClient.getTransactionTrees(offset, None, transactionFilter(sender)),
          mapOperator,
          subscriptionName,
        )

      private def makeSubscription[S, T](
          source: Source[S, NotUsed],
          mapOperator: Flow[S, T, ?],
          subscriptionName: String,
      ): LedgerSubscription =
        new LedgerSubscription {
          override protected def timeouts: ProcessingTimeout = processingTimeouts
          import TraceContext.Implicits.Empty.*
          val (killSwitch, completed) = PekkoUtil.runSupervised(
            logger.error("Fatally failed to handle transaction", _),
            source
              // we place the kill switch before the map operator, such that
              // we can shut down the operator quickly and signal upstream to cancel further sending
              .viaMat(KillSwitches.single)(Keep.right)
              .viaMat(mapOperator)(Keep.left)
              // and we get the Future[Done] as completed from the sink so we know when the last message
              // was processed
              .toMat(Sink.ignore)(Keep.both),
          )
          override val loggerFactory: NamedLoggerFactory =
            if (subscriptionName.isEmpty)
              loggerFactoryForLedgerConnectionOverride
            else
              loggerFactoryForLedgerConnectionOverride.appendUnnamedKey(
                "subscription",
                subscriptionName,
              )

          override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
            import TraceContext.Implicits.Empty.*
            List[AsyncOrSyncCloseable](
              SyncCloseable(s"killSwitch.shutdown $subscriptionName", killSwitch.shutdown()),
              AsyncCloseable(
                s"graph.completed $subscriptionName",
                completed.transform {
                  case Success(v) => Success(v)
                  case Failure(_: StatusRuntimeException) =>
                    // don't fail to close if there was a grpc status runtime exception
                    // this can happen (i.e. server not available etc.)
                    Success(Done)
                  case Failure(ex) => Failure(ex)
                },
                processingTimeouts.shutdownShort,
              ),
            )
          }
        }

      override def allocatePartyViaLedgerApi(
          hint: Option[String],
          displayName: Option[String],
      ): Future[PartyId] =
        client.partyManagementClient.allocateParty(hint, displayName, token).map { details =>
          PartyId.tryFromLfParty(details.party)
        }

      override def getPackageStatus(packageId: String): Future[GetPackageStatusResponse] =
        client.packageClient.getPackageStatus(packageId, token)

    }

  def transactionFilter(ps: Party*): TransactionFilter =
    TransactionFilter((ps: Seq[Party]).map(p => (p.getValue, Filters.defaultInstance)).toMap)

  def transactionFilterByParty(filter: Map[PartyId, Seq[Identifier]]): TransactionFilter =
    TransactionFilter(filter.map {
      case (p, Nil) => p.toParty.getValue -> Filters.defaultInstance
      case (p, ts) => p.toParty.getValue -> Filters(Some(InclusiveFilters(ts)))
    })

  def mapTemplateIds(id: javaapi.data.Identifier): Identifier =
    Identifier(
      packageId = id.getPackageId,
      moduleName = id.getModuleName,
      entityName = id.getEntityName,
    )

  val ledgerBegin: LedgerOffset = LedgerOffset(
    LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
  )

  def uniqueId: String = UUID.randomUUID.toString

  def toFuture[A](o: Option[A]): Future[A] =
    o.fold(Future.failed[A](new IllegalStateException(s"Empty option: $o")))(a =>
      Future.successful(a)
    )
}
