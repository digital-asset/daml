// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.transaction_filter.EventFormat
import com.daml.ledger.api.v2.{reassignment, state_service}
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsContractEntry.{
  JsActiveContract,
  JsContractEntry,
  JsIncompleteAssigned,
  JsIncompleteUnassigned,
}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{JsCantonError, JsEvent}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.grpc.stub.StreamObserver
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, CodecFormat, Schema, query, webSocketBody}

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class JsStateService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    val esf: ExecutionSequencerFactory,
    materializer: Materializer,
    wsConfig: WebsocketConfig,
    val authInterceptor: AuthInterceptor,
) extends Endpoints
    with NamedLogging {

  import JsStateService.*

  private def stateServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): state_service.StateServiceGrpc.StateServiceStub =
    ledgerClient.serviceClient(state_service.StateServiceGrpc.stub, token)

  def endpoints() = List(
    websocket(
      activeContractsEndpoint,
      getActiveContractsStream,
    ),
    asList(
      activeContractsListEndpoint,
      getActiveContractsStream,
    ),
    withServerLogic(JsStateService.getConnectedSynchronizersEndpoint, getConnectedSynchronizers),
    withServerLogic(
      JsStateService.getLedgerEndEndpoint,
      getLedgerEnd,
    ),
    withServerLogic(
      JsStateService.getLastPrunedOffsetsEndpoint,
      getLatestPrunedOffsets,
    ),
  )

  private def getConnectedSynchronizers(
      callerContext: CallerContext
  ): TracedInput[(Option[String], Option[String], Option[String])] => Future[
    Either[JsCantonError, state_service.GetConnectedSynchronizersResponse]
  ] = {
    implicit val traceContext: TraceContext = callerContext.traceContext()

    req =>
      stateServiceClient(callerContext.token())
        .getConnectedSynchronizers(
          state_service
            .GetConnectedSynchronizersRequest(
              party = req.in._1.getOrElse(""),
              participantId = req.in._2.getOrElse(""),
              identityProviderId = req.in._3.getOrElse(""),
            )
        )
        .resultToRight
  }

  private def getLedgerEnd(
      callerContext: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, state_service.GetLedgerEndResponse]
  ] = {
    implicit val traceContext: TraceContext = callerContext.traceContext()

    _ =>
      stateServiceClient(callerContext.token())
        .getLedgerEnd(state_service.GetLedgerEndRequest())
        .resultToRight
  }

  private def getLatestPrunedOffsets(
      callerContext: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, state_service.GetLatestPrunedOffsetsResponse]
  ] = {
    implicit val traceContext: TraceContext = callerContext.traceContext()

    _ =>
      stateServiceClient(callerContext.token())
        .getLatestPrunedOffsets(state_service.GetLatestPrunedOffsetsRequest())
        .resultToRight
  }

  private def getActiveContractsStream(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[
    LegacyDTOs.GetActiveContractsRequest,
    JsGetActiveContractsResponse,
    NotUsed,
  ] =
    _ => {
      implicit val tc: TraceContext = caller.traceContext()
      Flow[LegacyDTOs.GetActiveContractsRequest].map {
        toGetActiveContractsRequest
      } via
        prepareSingleWsStream(
          (
              req: state_service.GetActiveContractsRequest,
              obs: StreamObserver[state_service.GetActiveContractsResponse],
          ) => stateServiceClient(caller.token()).getActiveContracts(req, obs),
          (r: state_service.GetActiveContractsResponse) =>
            protocolConverters.GetActiveContractsResponse.toJson(r),
        )
    }

  private def toGetActiveContractsRequest(
      req: LegacyDTOs.GetActiveContractsRequest
  )(implicit traceContext: TraceContext): state_service.GetActiveContractsRequest =
    (req.eventFormat, req.filter, req.verbose) match {
      case (Some(_), Some(_), _) =>
        throw RequestValidationErrors.InvalidArgument
          .Reject(
            "Both event_format and filter are set. Please use either backwards compatible arguments (filter and verbose) or event_format, but not both."
          )
          .asGrpcError
      case (Some(_), _, true) =>
        throw RequestValidationErrors.InvalidArgument
          .Reject(
            "Both event_format and verbose are set. Please use either backwards compatible arguments (filter and verbose) or event_format, but not both."
          )
          .asGrpcError
      case (Some(_), None, false) =>
        state_service.GetActiveContractsRequest(
          activeAtOffset = req.activeAtOffset,
          eventFormat = req.eventFormat,
        )
      case (None, None, _) =>
        throw RequestValidationErrors.InvalidArgument
          .Reject(
            "Either filter/verbose or event_format is required. Please use either backwards compatible arguments (filter and verbose) or event_format."
          )
          .asGrpcError
      case (None, Some(filter), verbose) =>
        state_service.GetActiveContractsRequest(
          activeAtOffset = req.activeAtOffset,
          eventFormat = Some(
            EventFormat(
              filtersByParty = filter.filtersByParty,
              filtersForAnyParty = filter.filtersForAnyParty,
              verbose = verbose,
            )
          ),
        )
    }

}

object JsStateService extends DocumentationEndpoints {
  import Endpoints.*
  import JsStateServiceCodecs.*

  private lazy val state = v2Endpoint.in(sttp.tapir.stringToPath("state"))

  val activeContractsEndpoint = state.get
    .in(sttp.tapir.stringToPath("active-contracts"))
    .out(
      webSocketBody[
        LegacyDTOs.GetActiveContractsRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetActiveContractsResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .protoRef(state_service.StateServiceGrpc.METHOD_GET_ACTIVE_CONTRACTS)

  val activeContractsListEndpoint = state.post
    .in(sttp.tapir.stringToPath("active-contracts"))
    .in(jsonBody[LegacyDTOs.GetActiveContractsRequest])
    .out(jsonBody[Seq[JsGetActiveContractsResponse]])
    .description(
      s"""Query active contracts list (blocking call).
        |Querying active contracts is an expensive operation and if possible should not be repeated often.
        |Consider querying active contracts initially (for a given offset)
        |and then repeatedly call one of `/v2/updates/...`endpoints  to get subsequent modifications.
        |You can also use websockets to get updates with better performance.
        |
        |${createProtoRef(state_service.StateServiceGrpc.METHOD_GET_ACTIVE_CONTRACTS)}
        |""".stripMargin
    )
    .inStreamListParamsAndDescription()

  val getConnectedSynchronizersEndpoint = state.get
    .in(sttp.tapir.stringToPath("connected-synchronizers"))
    .in(query[Option[String]]("party"))
    .in(query[Option[String]]("participantId"))
    .in(query[Option[String]]("identityProviderId"))
    .out(jsonBody[state_service.GetConnectedSynchronizersResponse])
    .protoRef(state_service.StateServiceGrpc.METHOD_GET_CONNECTED_SYNCHRONIZERS)

  val getLedgerEndEndpoint = state.get
    .in(sttp.tapir.stringToPath("ledger-end"))
    .out(jsonBody[state_service.GetLedgerEndResponse])
    .protoRef(state_service.StateServiceGrpc.METHOD_GET_LEDGER_END)

  val getLastPrunedOffsetsEndpoint = state.get
    .in(sttp.tapir.stringToPath("latest-pruned-offsets"))
    .out(jsonBody[state_service.GetLatestPrunedOffsetsResponse])
    .protoRef(state_service.StateServiceGrpc.METHOD_GET_LATEST_PRUNED_OFFSETS)

  override def documentation: Seq[AnyEndpoint] = Seq(
    activeContractsEndpoint,
    activeContractsListEndpoint,
    getConnectedSynchronizersEndpoint,
    getLedgerEndEndpoint,
    getLastPrunedOffsetsEndpoint,
  )
}

object JsContractEntry {
  sealed trait JsContractEntry

  case object JsEmpty extends JsContractEntry
  final case class JsIncompleteAssigned(assignedEvent: JsAssignedEvent) extends JsContractEntry
  final case class JsIncompleteUnassigned(
      createdEvent: JsEvent.CreatedEvent,
      unassignedEvent: reassignment.UnassignedEvent,
  ) extends JsContractEntry

  final case class JsActiveContract(
      createdEvent: JsEvent.CreatedEvent,
      synchronizerId: String,
      reassignmentCounter: Long,
  ) extends JsContractEntry
}

final case class JsAssignedEvent(
    source: String,
    target: String,
    reassignmentId: String,
    submitter: String,
    reassignmentCounter: Long,
    createdEvent: JsEvent.CreatedEvent,
)

final case class JsGetActiveContractsResponse(
    workflowId: String,
    contractEntry: JsContractEntry,
)

object JsStateServiceCodecs {

  import JsSchema.*
  import JsSchema.JsServicesCommonCodecs.*

  implicit val getActiveContractsRequestRW: Codec[state_service.GetActiveContractsRequest] =
    deriveRelaxedCodec

  implicit val getActiveContractsRequestLegacyRW: Codec[LegacyDTOs.GetActiveContractsRequest] =
    deriveRelaxedCodec

  implicit val jsGetActiveContractsResponseRW: Codec[JsGetActiveContractsResponse] =
    deriveConfiguredCodec

  implicit val jsContractEntryRW: Codec[JsContractEntry] = deriveConfiguredCodec

  implicit val jsIncompleteUnassignedRW: Codec[JsIncompleteUnassigned] = deriveConfiguredCodec
  implicit val jsIncompleteAssignedRW: Codec[JsIncompleteAssigned] = deriveConfiguredCodec
  implicit val jsActiveContractRW: Codec[JsActiveContract] = deriveConfiguredCodec
  implicit val jsAssignedEventRW: Codec[JsAssignedEvent] = deriveConfiguredCodec

  implicit val getConnectedSynchronizersRequestRW
      : Codec[state_service.GetConnectedSynchronizersRequest] =
    deriveRelaxedCodec
  implicit val getConnectedSynchronizersResponseRW
      : Codec[state_service.GetConnectedSynchronizersResponse] =
    deriveRelaxedCodec

  implicit val connectedSynchronizerRW
      : Codec[state_service.GetConnectedSynchronizersResponse.ConnectedSynchronizer] =
    deriveRelaxedCodec

  implicit val getLedgerEndRequestRW: Codec[state_service.GetLedgerEndRequest] = deriveRelaxedCodec

  implicit val getLedgerEndResponseRW: Codec[state_service.GetLedgerEndResponse] =
    deriveRelaxedCodec
  implicit val getLatestPrunedOffsetsResponseRW
      : Codec[state_service.GetLatestPrunedOffsetsResponse] =
    deriveRelaxedCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsContractEntrySchema: Schema[JsContractEntry] = Schema.oneOfWrapped

  implicit val connectedSynchronizerSchema
      : Schema[state_service.GetConnectedSynchronizersResponse.ConnectedSynchronizer] =
    Schema.derived

  implicit val getConnectedSynchronizersRequestSchema
      : Schema[state_service.GetConnectedSynchronizersRequest] = Schema.derived
}
