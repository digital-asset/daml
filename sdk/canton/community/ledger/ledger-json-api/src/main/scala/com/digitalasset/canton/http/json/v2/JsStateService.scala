// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.{reassignment, state_service}
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsContractEntry.{
  JsActiveContract,
  JsContractEntry,
  JsIncompleteAssigned,
  JsIncompleteUnassigned,
}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{JsCantonError, JsEvent}
import com.digitalasset.canton.http.json.v2.Protocol.Protocol
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, CodecFormat, Schema, query, webSocketBody}

import scala.concurrent.{ExecutionContext, Future}

class JsStateService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    val esf: ExecutionSequencerFactory,
    materializer: Materializer,
    wsConfig: WebsocketConfig,
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
  ): TracedInput[(String, Option[String])] => Future[
    Either[JsCantonError, state_service.GetConnectedSynchronizersResponse]
  ] = req =>
    stateServiceClient(callerContext.token())(req.traceContext)
      .getConnectedSynchronizers(
        state_service
          .GetConnectedSynchronizersRequest(
            party = req.in._1,
            participantId = req.in._2.getOrElse(""),
          )
      )
      .resultToRight

  private def getLedgerEnd(
      callerContext: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, state_service.GetLedgerEndResponse]
  ] = req =>
    stateServiceClient(callerContext.token())(req.traceContext)
      .getLedgerEnd(state_service.GetLedgerEndRequest())
      .resultToRight

  private def getLatestPrunedOffsets(
      callerContext: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, state_service.GetLatestPrunedOffsetsResponse]
  ] = req =>
    stateServiceClient(callerContext.token())(req.traceContext)
      .getLatestPrunedOffsets(state_service.GetLatestPrunedOffsetsRequest())
      .resultToRight

  private def getActiveContractsStream(
      caller: CallerContext,
      protocol: Protocol,
  ): TracedInput[Unit] => Flow[
    state_service.GetActiveContractsRequest,
    JsGetActiveContractsResponse,
    NotUsed,
  ] =
    req => {
      implicit val token = caller.token()
      implicit val tc = req.traceContext
      prepareSingleWsStream(
        stateServiceClient(caller.token())(TraceContext.empty).getActiveContracts,
        (r: state_service.GetActiveContractsResponse) =>
          protocolConverters.GetActiveContractsResponse.toJson(r),
        protocol = protocol,
        withCloseDelay = true,
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
        state_service.GetActiveContractsRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetActiveContractsResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .description("Get active contracts stream")

  val activeContractsListEndpoint = state.post
    .in(sttp.tapir.stringToPath("active-contracts"))
    .in(jsonBody[state_service.GetActiveContractsRequest])
    .out(jsonBody[Seq[JsGetActiveContractsResponse]])
    .inStreamListParams()
    .description("Query active contracts list (blocking call)")

  val getConnectedSynchronizersEndpoint = state.get
    .in(sttp.tapir.stringToPath("connected-synchronizers"))
    .in(query[String]("party"))
    .in(query[Option[String]]("participantId"))
    .out(jsonBody[state_service.GetConnectedSynchronizersResponse])
    .description("Get connected synchronizers")

  val getLedgerEndEndpoint = state.get
    .in(sttp.tapir.stringToPath("ledger-end"))
    .out(jsonBody[state_service.GetLedgerEndResponse])
    .description("Get ledger end")

  val getLastPrunedOffsetsEndpoint = state.get
    .in(sttp.tapir.stringToPath("latest-pruned-offsets"))
    .out(jsonBody[state_service.GetLatestPrunedOffsetsResponse])
    .description("Get latest pruned offsets")

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
    unassignId: String,
    submitter: String,
    reassignmentCounter: Long,
    createdEvent: JsEvent.CreatedEvent,
)

final case class JsUnassignedEvent(
    unassignId: String,
    contractId: String,
    templateId: String,
    source: String,
    target: String,
    submitter: String,
    reassignmentCounter: Long,
    assignmentExclusivity: Option[com.google.protobuf.timestamp.Timestamp],
    witnessParties: Seq[String],
    packageName: String,
)

final case class JsGetActiveContractsResponse(
    workflowId: String,
    contractEntry: JsContractEntry,
)

object JsStateServiceCodecs {

  import JsSchema.JsServicesCommonCodecs.*

  implicit val getActiveContractsRequestRW: Codec[state_service.GetActiveContractsRequest] =
    deriveCodec

  implicit val jsGetActiveContractsResponseRW: Codec[JsGetActiveContractsResponse] = deriveCodec

  implicit val jsContractEntryRW: Codec[JsContractEntry] = deriveCodec
  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsContractEntrySchema: Schema[JsContractEntry] = Schema.oneOfWrapped

  implicit val jsIncompleteUnassignedRW: Codec[JsIncompleteUnassigned] = deriveCodec
  implicit val jsIncompleteAssignedRW: Codec[JsIncompleteAssigned] = deriveCodec
  implicit val jsActiveContractRW: Codec[JsActiveContract] = deriveCodec
  implicit val jsUnassignedEventRW: Codec[JsUnassignedEvent] = deriveCodec
  implicit val jsAssignedEventRW: Codec[JsAssignedEvent] = deriveCodec

  implicit val getConnectedSynchronizersRequestRW
      : Codec[state_service.GetConnectedSynchronizersRequest] =
    deriveCodec
  implicit val getConnectedSynchronizersResponseRW
      : Codec[state_service.GetConnectedSynchronizersResponse] =
    deriveCodec
  implicit val connectedSynchronizerRW
      : Codec[state_service.GetConnectedSynchronizersResponse.ConnectedSynchronizer] =
    deriveCodec
  implicit val participantPermissionRW: Codec[state_service.ParticipantPermission] = deriveCodec

  implicit val getLedgerEndRequestRW: Codec[state_service.GetLedgerEndRequest] = deriveCodec

  implicit val getLedgerEndResponseRW: Codec[state_service.GetLedgerEndResponse] = deriveCodec
  implicit val getLatestPrunedOffsetsResponseRW
      : Codec[state_service.GetLatestPrunedOffsetsResponse] =
    deriveCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val participantPermissionRecognizedSchema
      : Schema[state_service.ParticipantPermission.Recognized] =
    Schema.oneOfWrapped

  implicit val participantPermissionSchema: Schema[state_service.ParticipantPermission] =
    Schema.oneOfWrapped
}
