// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.commands.{Command, DisclosedContract}
import com.daml.ledger.api.v1.event_query_service.GetEventsByContractIdRequest
import com.daml.ledger.api.v1.transaction_filter.{Filters, InclusiveFilters, TemplateFilter}
import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionServiceStub
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
}
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandServiceStub
import com.daml.ledger.api.v2.command_service.{
  CommandServiceGrpc,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
  SubmitAndWaitRequest,
}
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v2.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitReassignmentRequest,
  SubmitReassignmentResponse,
  SubmitRequest,
  SubmitResponse,
}
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.api.v2.event_query_service.EventQueryServiceGrpc.EventQueryServiceStub
import com.daml.ledger.api.v2.event_query_service.{
  EventQueryServiceGrpc,
  GetEventsByContractIdResponse,
}
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.reassignment.{AssignedEvent, Reassignment, UnassignedEvent}
import com.daml.ledger.api.v2.reassignment_command.{
  AssignCommand,
  ReassignmentCommand,
  UnassignCommand,
}
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateServiceStub
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  GetConnectedDomainsRequest,
  GetConnectedDomainsResponse,
  GetLedgerEndRequest,
  GetLedgerEndResponse,
  StateServiceGrpc,
}
import com.daml.ledger.api.v2.testing.time_service.TimeServiceGrpc.TimeServiceStub
import com.daml.ledger.api.v2.testing.time_service.{
  GetTimeRequest,
  GetTimeResponse,
  SetTimeRequest,
  TimeServiceGrpc,
}
import com.daml.ledger.api.v2.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateServiceStub
import com.daml.ledger.api.v2.update_service.{
  GetTransactionByIdRequest,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.DeduplicationPeriod
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.ForwardingStreamObserver
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.DomainId
import com.google.protobuf.empty.Empty
import io.grpc.*
import io.grpc.stub.StreamObserver

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

// TODO(#15280) delete LedgerApiCommands, and rename this to LedgerApiCommands
object LedgerApiV2Commands {

  object UpdateService {

    sealed trait UpdateTreeWrapper
    sealed trait UpdateWrapper
    final case class TransactionTreeWrapper(transactionTree: TransactionTree)
        extends UpdateTreeWrapper
    final case class TransactionWrapper(transaction: Transaction) extends UpdateWrapper
    sealed trait ReassignmentWrapper extends UpdateTreeWrapper with UpdateWrapper {
      def reassignment: Reassignment
    }
    object ReassignmentWrapper {
      def apply(reassignment: Reassignment): ReassignmentWrapper = {
        val event = reassignment.event
        event.assignedEvent
          .map[ReassignmentWrapper](AssignedWrapper(reassignment, _))
          .orElse(
            event.unassignedEvent.map[ReassignmentWrapper](UnassignedWrapper(reassignment, _))
          )
          .getOrElse(
            throw new IllegalStateException(
              s"Invalid reassignment event (only supported UnassignedEvent and AssignedEvent): ${reassignment.event}"
            )
          )
      }
    }
    final case class AssignedWrapper(reassignment: Reassignment, assignedEvent: AssignedEvent)
        extends ReassignmentWrapper
    final case class UnassignedWrapper(reassignment: Reassignment, unassignedEvent: UnassignedEvent)
        extends ReassignmentWrapper

    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = UpdateServiceStub

      override def createService(channel: ManagedChannel): UpdateServiceStub =
        UpdateServiceGrpc.stub(channel)
    }

    trait SubscribeBase[Resp, Res]
        extends BaseCommand[GetUpdatesRequest, AutoCloseable, AutoCloseable] {
      // The subscription should never be cut short because of a gRPC timeout
      override def timeoutType: TimeoutType = ServerEnforcedTimeout

      def observer: StreamObserver[Res]

      def beginExclusive: ParticipantOffset

      def endInclusive: Option[ParticipantOffset]

      def filter: TransactionFilter

      def verbose: Boolean

      def doRequest(
          service: UpdateServiceStub,
          request: GetUpdatesRequest,
          rawObserver: StreamObserver[Resp],
      ): Unit

      def extractResults(response: Resp): IterableOnce[Res]

      implicit def loggingContext: ErrorLoggingContext

      override def createRequest(): Either[String, GetUpdatesRequest] = Right {
        GetUpdatesRequest(
          beginExclusive = Some(beginExclusive),
          endInclusive = endInclusive,
          verbose = verbose,
          filter = Some(filter),
        )
      }

      override def submitRequest(
          service: UpdateServiceStub,
          request: GetUpdatesRequest,
      ): Future[AutoCloseable] = {
        val rawObserver = new ForwardingStreamObserver[Resp, Res](observer, extractResults)
        val context = Context.current().withCancellation()
        context.run(() => doRequest(service, request, rawObserver))
        Future.successful(context)
      }

      override def handleResponse(response: AutoCloseable): Either[String, AutoCloseable] = Right(
        response
      )
    }

    final case class SubscribeTrees(
        override val observer: StreamObserver[UpdateTreeWrapper],
        override val beginExclusive: ParticipantOffset,
        override val endInclusive: Option[ParticipantOffset],
        override val filter: TransactionFilter,
        override val verbose: Boolean,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends SubscribeBase[GetUpdateTreesResponse, UpdateTreeWrapper] {
      override def doRequest(
          service: UpdateServiceStub,
          request: GetUpdatesRequest,
          rawObserver: StreamObserver[GetUpdateTreesResponse],
      ): Unit =
        service.getUpdateTrees(request, rawObserver)

      override def extractResults(
          response: GetUpdateTreesResponse
      ): IterableOnce[UpdateTreeWrapper] =
        response.update.transactionTree
          .map[UpdateTreeWrapper](TransactionTreeWrapper)
          .orElse(response.update.reassignment.map(ReassignmentWrapper(_)))
    }

    final case class SubscribeFlat(
        override val observer: StreamObserver[UpdateWrapper],
        override val beginExclusive: ParticipantOffset,
        override val endInclusive: Option[ParticipantOffset],
        override val filter: TransactionFilter,
        override val verbose: Boolean,
    )(override implicit val loggingContext: ErrorLoggingContext)
        extends SubscribeBase[GetUpdatesResponse, UpdateWrapper] {
      override def doRequest(
          service: UpdateServiceStub,
          request: GetUpdatesRequest,
          rawObserver: StreamObserver[GetUpdatesResponse],
      ): Unit =
        service.getUpdates(request, rawObserver)

      override def extractResults(response: GetUpdatesResponse): IterableOnce[UpdateWrapper] =
        response.update.transaction
          .map[UpdateWrapper](TransactionWrapper)
          .orElse(response.update.reassignment.map(ReassignmentWrapper(_)))
    }

    final case class GetTransactionById(parties: Set[LfPartyId], id: String)(implicit
        ec: ExecutionContext
    ) extends BaseCommand[GetTransactionByIdRequest, GetTransactionTreeResponse, Option[
          TransactionTree
        ]]
        with PrettyPrinting {
      override def createRequest(): Either[String, GetTransactionByIdRequest] = Right {
        GetTransactionByIdRequest(
          updateId = id,
          requestingParties = parties.toSeq,
        )
      }

      override def submitRequest(
          service: UpdateServiceStub,
          request: GetTransactionByIdRequest,
      ): Future[GetTransactionTreeResponse] = {
        // The Ledger API will throw an error if it can't find a transaction by ID.
        // However, as Canton is distributed, a transaction ID might show up later, so we don't treat this as
        // an error and change it to a None
        service.getTransactionTreeById(request).recover {
          case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.NOT_FOUND =>
            GetTransactionTreeResponse(None)
        }
      }

      override def handleResponse(
          response: GetTransactionTreeResponse
      ): Either[String, Option[TransactionTree]] =
        Right(response.transaction)

      override def pretty: Pretty[GetTransactionById] =
        prettyOfClass(
          param("id", _.id.unquoted),
          param("parties", _.parties),
        )
    }

  }

  private[commands] trait SubmitCommand extends PrettyPrinting {
    def actAs: Seq[LfPartyId]
    def readAs: Seq[LfPartyId]
    def commands: Seq[Command]
    def workflowId: String
    def commandId: String
    def deduplicationPeriod: Option[DeduplicationPeriod]
    def submissionId: String
    def minLedgerTimeAbs: Option[Instant]
    def disclosedContracts: Seq[DisclosedContract]
    def domainId: DomainId
    def applicationId: String

    protected def mkCommand: Commands = Commands(
      workflowId = workflowId,
      applicationId = applicationId,
      commandId = if (commandId.isEmpty) UUID.randomUUID().toString else commandId,
      actAs = actAs,
      readAs = readAs,
      commands = commands,
      deduplicationPeriod = deduplicationPeriod.fold(
        Commands.DeduplicationPeriod.Empty: Commands.DeduplicationPeriod
      ) {
        case DeduplicationPeriod.DeduplicationDuration(duration) =>
          Commands.DeduplicationPeriod.DeduplicationDuration(
            ProtoConverter.DurationConverter.toProtoPrimitive(duration)
          )
        case DeduplicationPeriod.DeduplicationOffset(offset) =>
          Commands.DeduplicationPeriod.DeduplicationOffset(
            offset.toHexString
          )
      },
      minLedgerTimeAbs = minLedgerTimeAbs.map(ProtoConverter.InstantConverter.toProtoPrimitive),
      submissionId = submissionId,
      disclosedContracts = disclosedContracts,
      domainId = domainId.toProtoPrimitive,
    )

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("actAs", _.actAs),
        param("readAs", _.readAs),
        param("commandId", _.commandId.singleQuoted),
        param("workflowId", _.workflowId.singleQuoted),
        param("submissionId", _.submissionId.singleQuoted),
        param("deduplicationPeriod", _.deduplicationPeriod),
        paramIfDefined("minLedgerTimeAbs", _.minLedgerTimeAbs),
        paramWithoutValue("commands"),
      )
  }

  object CommandSubmissionService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandSubmissionServiceStub
      override def createService(channel: ManagedChannel): CommandSubmissionServiceStub =
        CommandSubmissionServiceGrpc.stub(channel)
    }

    final case class Submit(
        override val actAs: Seq[LfPartyId],
        override val readAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
        override val disclosedContracts: Seq[DisclosedContract],
        override val domainId: DomainId,
        override val applicationId: String,
    ) extends SubmitCommand
        with BaseCommand[SubmitRequest, SubmitResponse, Unit] {
      override def createRequest(): Either[String, SubmitRequest] = Right(
        SubmitRequest(commands = Some(mkCommand))
      )

      override def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitRequest,
      ): Future[SubmitResponse] = {
        service.submit(request)
      }

      override def handleResponse(response: SubmitResponse): Either[String, Unit] = Right(())
    }

    final case class SubmitAssignCommand(
        workflowId: String,
        applicationId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        unassignId: String,
        source: DomainId,
        target: DomainId,
    ) extends BaseCommand[SubmitReassignmentRequest, SubmitReassignmentResponse, Unit] {
      override def createRequest(): Either[String, SubmitReassignmentRequest] = Right(
        SubmitReassignmentRequest(
          Some(
            ReassignmentCommand(
              workflowId = workflowId,
              applicationId = applicationId,
              commandId = commandId,
              submitter = submitter.toString,
              command = ReassignmentCommand.Command.AssignCommand(
                AssignCommand(
                  unassignId = unassignId,
                  source = source.toProtoPrimitive,
                  target = target.toProtoPrimitive,
                )
              ),
              submissionId = submissionId,
            )
          )
        )
      )

      override def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitReassignmentRequest,
      ): Future[SubmitReassignmentResponse] = {
        service.submitReassignment(request)
      }

      override def handleResponse(response: SubmitReassignmentResponse): Either[String, Unit] =
        Right(())
    }

    final case class SubmitUnassignCommand(
        workflowId: String,
        applicationId: String,
        commandId: String,
        submitter: LfPartyId,
        submissionId: String,
        contractId: LfContractId,
        source: DomainId,
        target: DomainId,
    ) extends BaseCommand[SubmitReassignmentRequest, SubmitReassignmentResponse, Unit] {
      override def createRequest(): Either[String, SubmitReassignmentRequest] = Right(
        SubmitReassignmentRequest(
          Some(
            ReassignmentCommand(
              workflowId = workflowId,
              applicationId = applicationId,
              commandId = commandId,
              submitter = submitter.toString,
              command = ReassignmentCommand.Command.UnassignCommand(
                UnassignCommand(
                  contractId = contractId.coid.toString,
                  source = source.toProtoPrimitive,
                  target = target.toProtoPrimitive,
                )
              ),
              submissionId = submissionId,
            )
          )
        )
      )

      override def submitRequest(
          service: CommandSubmissionServiceStub,
          request: SubmitReassignmentRequest,
      ): Future[SubmitReassignmentResponse] = {
        service.submitReassignment(request)
      }

      override def handleResponse(response: SubmitReassignmentResponse): Either[String, Unit] =
        Right(())
    }
  }

  object CommandService {
    trait BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandServiceStub
      override def createService(channel: ManagedChannel): CommandServiceStub =
        CommandServiceGrpc.stub(channel)
    }

    final case class SubmitAndWaitTransactionTree(
        override val actAs: Seq[LfPartyId],
        override val readAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
        override val disclosedContracts: Seq[DisclosedContract],
        override val domainId: DomainId,
        override val applicationId: String,
    ) extends SubmitCommand
        with BaseCommand[
          SubmitAndWaitRequest,
          SubmitAndWaitForTransactionTreeResponse,
          TransactionTree,
        ] {

      override def createRequest(): Either[String, SubmitAndWaitRequest] =
        Right(SubmitAndWaitRequest(commands = Some(mkCommand)))

      override def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitRequest,
      ): Future[SubmitAndWaitForTransactionTreeResponse] =
        service.submitAndWaitForTransactionTree(request)

      override def handleResponse(
          response: SubmitAndWaitForTransactionTreeResponse
      ): Either[String, TransactionTree] =
        response.transaction.toRight("Received response without any transaction tree")

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }

    final case class SubmitAndWaitTransaction(
        override val actAs: Seq[LfPartyId],
        override val readAs: Seq[LfPartyId],
        override val commands: Seq[Command],
        override val workflowId: String,
        override val commandId: String,
        override val deduplicationPeriod: Option[DeduplicationPeriod],
        override val submissionId: String,
        override val minLedgerTimeAbs: Option[Instant],
        override val disclosedContracts: Seq[DisclosedContract],
        override val domainId: DomainId,
        override val applicationId: String,
    ) extends SubmitCommand
        with BaseCommand[SubmitAndWaitRequest, SubmitAndWaitForTransactionResponse, Transaction] {

      override def createRequest(): Either[String, SubmitAndWaitRequest] =
        Right(SubmitAndWaitRequest(commands = Some(mkCommand)))

      override def submitRequest(
          service: CommandServiceStub,
          request: SubmitAndWaitRequest,
      ): Future[SubmitAndWaitForTransactionResponse] =
        service.submitAndWaitForTransaction(request)

      override def handleResponse(
          response: SubmitAndWaitForTransactionResponse
      ): Either[String, Transaction] =
        response.transaction.toRight("Received response without any transaction")

      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  object StateService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = StateServiceStub

      override def createService(channel: ManagedChannel): StateServiceStub =
        StateServiceGrpc.stub(channel)
    }

    final case class LedgerEnd()
        extends BaseCommand[GetLedgerEndRequest, GetLedgerEndResponse, ParticipantOffset] {

      override def createRequest(): Either[String, GetLedgerEndRequest] =
        Right(GetLedgerEndRequest())

      override def submitRequest(
          service: StateServiceStub,
          request: GetLedgerEndRequest,
      ): Future[GetLedgerEndResponse] =
        service.getLedgerEnd(request)

      override def handleResponse(
          response: GetLedgerEndResponse
      ): Either[String, ParticipantOffset] =
        response.offset.toRight("Empty LedgerEndResponse received without offset")
    }

    final case class GetConnectedDomains(partyId: LfPartyId)
        extends BaseCommand[
          GetConnectedDomainsRequest,
          GetConnectedDomainsResponse,
          GetConnectedDomainsResponse,
        ] {

      override def createRequest(): Either[String, GetConnectedDomainsRequest] =
        Right(GetConnectedDomainsRequest(partyId.toString))

      override def submitRequest(
          service: StateServiceStub,
          request: GetConnectedDomainsRequest,
      ): Future[GetConnectedDomainsResponse] =
        service.getConnectedDomains(request)

      override def handleResponse(
          response: GetConnectedDomainsResponse
      ): Either[String, GetConnectedDomainsResponse] =
        Right(response)
    }

    final case class GetActiveContracts(
        parties: Set[LfPartyId],
        limit: PositiveInt,
        templateFilter: Seq[TemplateId] = Seq.empty,
        verbose: Boolean = true,
        timeout: FiniteDuration,
        includeCreatedEventBlob: Boolean = false,
    )(scheduler: ScheduledExecutorService)
        extends BaseCommand[GetActiveContractsRequest, Seq[GetActiveContractsResponse], Seq[
          GetActiveContractsResponse
        ]] {

      override def createRequest(): Either[String, GetActiveContractsRequest] = {
        val filter =
          if (templateFilter.nonEmpty) {
            Filters(
              Some(
                InclusiveFilters(templateFilters =
                  templateFilter.map(tId =>
                    TemplateFilter(Some(tId.toIdentifier), includeCreatedEventBlob)
                  )
                )
              )
            )
          } else Filters.defaultInstance
        Right(
          GetActiveContractsRequest(
            filter = Some(TransactionFilter(parties.map((_, filter)).toMap)),
            verbose = verbose,
          )
        )
      }

      override def submitRequest(
          service: StateServiceStub,
          request: GetActiveContractsRequest,
      ): Future[Seq[GetActiveContractsResponse]] = {
        GrpcAdminCommand.streamedResponse[
          GetActiveContractsRequest,
          GetActiveContractsResponse,
          GetActiveContractsResponse,
        ](
          service.getActiveContracts,
          List(_),
          request,
          limit.value,
          timeout,
          scheduler,
        )
      }

      override def handleResponse(
          response: Seq[GetActiveContractsResponse]
      ): Either[String, Seq[GetActiveContractsResponse]] = {
        Right(response)
      }

      // fetching ACS might take long if we fetch a lot of data
      override def timeoutType: TimeoutType = DefaultUnboundedTimeout

    }
  }

  final case class CompletionWrapper(
      completion: Completion,
      checkpoint: Checkpoint,
      domainId: DomainId,
  )

  object CommandCompletionService {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = CommandCompletionServiceStub

      override def createService(channel: ManagedChannel): CommandCompletionServiceStub =
        CommandCompletionServiceGrpc.stub(channel)
    }

    final case class CompletionRequest(
        partyId: LfPartyId,
        beginOffset: ParticipantOffset,
        expectedCompletions: Int,
        timeout: java.time.Duration,
        applicationId: String,
    )(filter: CompletionWrapper => Boolean, scheduler: ScheduledExecutorService)
        extends BaseCommand[
          CompletionStreamRequest,
          Seq[CompletionWrapper],
          Seq[CompletionWrapper],
        ] {

      override def createRequest(): Either[String, CompletionStreamRequest] =
        Right(
          CompletionStreamRequest(
            applicationId = applicationId,
            parties = Seq(partyId),
            beginExclusive = Some(beginOffset),
          )
        )

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[Seq[CompletionWrapper]] = {
        import scala.jdk.DurationConverters.*
        GrpcAdminCommand
          .streamedResponse[CompletionStreamRequest, CompletionStreamResponse, CompletionWrapper](
            service.completionStream,
            response =>
              List(
                CompletionWrapper(
                  completion = response.completion.getOrElse(
                    throw new IllegalStateException("Completion should be present.")
                  ),
                  checkpoint = response.checkpoint.getOrElse(
                    throw new IllegalStateException("Checkpoint should be present.")
                  ),
                  domainId = DomainId.tryFromString(response.domainId),
                )
              ).filter(filter),
            request,
            expectedCompletions,
            timeout.toScala,
            scheduler,
          )
      }

      override def handleResponse(
          response: Seq[CompletionWrapper]
      ): Either[String, Seq[CompletionWrapper]] =
        Right(response)

      override def timeoutType: TimeoutType = ServerEnforcedTimeout
    }

    final case class Subscribe(
        observer: StreamObserver[CompletionWrapper],
        parties: Seq[String],
        offset: Option[ParticipantOffset],
        applicationId: String,
    )(implicit loggingContext: ErrorLoggingContext)
        extends BaseCommand[CompletionStreamRequest, AutoCloseable, AutoCloseable] {
      // The subscription should never be cut short because of a gRPC timeout
      override def timeoutType: TimeoutType = ServerEnforcedTimeout

      override def createRequest(): Either[String, CompletionStreamRequest] = Right {
        CompletionStreamRequest(
          applicationId = applicationId,
          parties = parties,
          beginExclusive = offset,
        )
      }

      override def submitRequest(
          service: CommandCompletionServiceStub,
          request: CompletionStreamRequest,
      ): Future[AutoCloseable] = {
        val rawObserver = new ForwardingStreamObserver[CompletionStreamResponse, CompletionWrapper](
          observer,
          response =>
            List(
              CompletionWrapper(
                completion = response.completion.getOrElse(
                  throw new IllegalStateException("Completion should be present.")
                ),
                checkpoint = response.checkpoint.getOrElse(
                  throw new IllegalStateException("Checkpoint should be present.")
                ),
                domainId = DomainId.tryFromString(response.domainId),
              )
            ),
        )
        val context = Context.current().withCancellation()
        context.run(() => service.completionStream(request, rawObserver))
        Future.successful(context)
      }

      override def handleResponse(response: AutoCloseable): Either[String, AutoCloseable] = Right(
        response
      )
    }
  }

  object Time {
    abstract class BaseCommand[Req, Resp, Res] extends GrpcAdminCommand[Req, Resp, Res] {
      override type Svc = TimeServiceStub

      override def createService(channel: ManagedChannel): TimeServiceStub =
        TimeServiceGrpc.stub(channel)
    }

    final object Get
        extends BaseCommand[
          GetTimeRequest,
          GetTimeResponse,
          CantonTimestamp,
        ] {

      override def submitRequest(
          service: TimeServiceStub,
          request: GetTimeRequest,
      ): Future[GetTimeResponse] = {
        service.getTime(request)
      }

      /** Create the request from configured options
        */
      override def createRequest(): Either[String, GetTimeRequest] = Right(GetTimeRequest())

      /** Handle the response the service has provided
        */
      override def handleResponse(
          response: GetTimeResponse
      ): Either[String, CantonTimestamp] =
        for {
          prototTimestamp <- response.currentTime.map(Right(_)).getOrElse(Left("currentTime empty"))
          result <- CantonTimestamp.fromProtoPrimitive(prototTimestamp).left.map(_.message)
        } yield result
    }

    final case class Set(currentTime: CantonTimestamp, newTime: CantonTimestamp)
        extends BaseCommand[
          SetTimeRequest,
          Empty,
          Unit,
        ] {

      override def submitRequest(service: TimeServiceStub, request: SetTimeRequest): Future[Empty] =
        service.setTime(request)

      override def createRequest(): Either[String, SetTimeRequest] =
        Right(
          SetTimeRequest(
            currentTime = Some(currentTime.toProtoPrimitive),
            newTime = Some(newTime.toProtoPrimitive),
          )
        )

      /** Handle the response the service has provided
        */
      override def handleResponse(response: Empty): Either[String, Unit] = Right(())

    }

  }

  object QueryService {

    abstract class BaseCommand[Req, Res] extends GrpcAdminCommand[Req, Res, Res] {
      override type Svc = EventQueryServiceStub

      override def createService(channel: ManagedChannel): EventQueryServiceStub =
        EventQueryServiceGrpc.stub(channel)

      override def handleResponse(response: Res): Either[String, Res] = Right(response)
    }

    final case class GetEventsByContractId(
        contractId: String,
        requestingParties: Seq[String],
    ) extends BaseCommand[
          GetEventsByContractIdRequest,
          GetEventsByContractIdResponse,
        ] {

      override def createRequest(): Either[String, GetEventsByContractIdRequest] = Right(
        GetEventsByContractIdRequest(
          contractId = contractId,
          requestingParties = requestingParties,
        )
      )

      override def submitRequest(
          service: EventQueryServiceStub,
          request: GetEventsByContractIdRequest,
      ): Future[GetEventsByContractIdResponse] = service.getEventsByContractId(request)

    }
  }
}
