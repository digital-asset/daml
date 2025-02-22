// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.transaction_filter.ParticipantAuthorizationTopologyFormat
import com.daml.ledger.api.v2.{offset_checkpoint, reassignment, transaction_filter, update_service}
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsEvent.CreatedEvent
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  JsTopologyTransaction,
  JsTransaction,
  JsTransactionTree,
}
import com.digitalasset.canton.http.json.v2.Protocol.Protocol
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.*
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
import sttp.tapir.{AnyEndpoint, CodecFormat, Endpoint, Schema, path, query, webSocketBody}

import scala.concurrent.{ExecutionContext, Future}

class JsUpdateService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
    wsConfig: WebsocketConfig,
    materializer: Materializer,
) extends Endpoints
    with NamedLogging {

  private def updateServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): update_service.UpdateServiceGrpc.UpdateServiceStub =
    ledgerClient.serviceClient(update_service.UpdateServiceGrpc.stub, token)

  def endpoints() = List(
    websocket(
      JsUpdateService.getUpdatesFlatEndpoint,
      getFlats,
    ),
    asList(
      JsUpdateService.getUpdatesFlatListEndpoint,
      getFlats,
      timeoutOpenEndedStream = true,
    ),
    websocket(
      JsUpdateService.getUpdatesTreeEndpoint,
      getTrees,
    ),
    asList(
      JsUpdateService.getUpdatesTreeListEndpoint,
      getTrees,
      timeoutOpenEndedStream = true,
    ),
    withServerLogic(
      JsUpdateService.getTransactionTreeByOffsetEndpoint,
      getTreeByOffset,
    ),
    withServerLogic(
      JsUpdateService.getTransactionByOffsetEndpoint,
      getTransactionByOffset,
    ),
    withServerLogic(
      JsUpdateService.getTransactionByIdEndpoint,
      getTransactionById,
    ),
    withServerLogic(
      JsUpdateService.getTransactionTreeByIdEndpoint,
      getTransactionTreeById,
    ),
  )

  private def getTreeByOffset(
      caller: CallerContext
  ): TracedInput[(Long, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionTreeResponse]
  ] = { req =>
    implicit val token: Option[String] = caller.token()
    implicit val tc: TraceContext = req.traceContext
    updateServiceClient(caller.token())(req.traceContext)
      .getTransactionTreeByOffset(
        update_service.GetTransactionByOffsetRequest(
          offset = req.in._1,
          requestingParties = req.in._2,
        )
      )
      .flatMap(protocolConverters.GetTransactionTreeResponse.toJson(_))
      .resultToRight
  }

  private def getTransactionByOffset(
      caller: CallerContext
  ): TracedInput[(Long, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionResponse]
  ] =
    req => {
      implicit val token = caller.token()
      implicit val tc = req.traceContext
      updateServiceClient(caller.token())(req.traceContext)
        .getTransactionByOffset(
          update_service.GetTransactionByOffsetRequest(
            offset = req.in._1,
            requestingParties = req.in._2,
          )
        )
        .flatMap(protocolConverters.GetTransactionResponse.toJson(_))
        .resultToRight
    }

  private def getTransactionById(
      caller: CallerContext
  ): TracedInput[(String, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionResponse]
  ] = { req =>
    implicit val token = caller.token()
    implicit val tc = req.traceContext
    updateServiceClient(caller.token())(req.traceContext)
      .getTransactionById(
        update_service.GetTransactionByIdRequest(
          updateId = req.in._1,
          requestingParties = req.in._2,
        )
      )
      .flatMap(protocolConverters.GetTransactionResponse.toJson(_))
      .resultToRight
  }

  private def getTransactionTreeById(
      caller: CallerContext
  ): TracedInput[(String, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionTreeResponse]
  ] =
    req => {
      implicit val token = caller.token()
      implicit val tc = req.traceContext
      updateServiceClient(caller.token())(req.traceContext)
        .getTransactionTreeById(
          update_service.GetTransactionByIdRequest(
            updateId = req.in._1,
            requestingParties = req.in._2,
          )
        )
        .flatMap { tr =>
          protocolConverters.GetTransactionTreeResponse.toJson(tr)
        }
        .resultToRight
    }

  private def getFlats(
      caller: CallerContext,
      protocol: Protocol,
  ): TracedInput[Unit] => Flow[update_service.GetUpdatesRequest, JsGetUpdatesResponse, NotUsed] =
    req => {
      implicit val token = caller.token()
      implicit val tc = req.traceContext
      prepareSingleWsStream(
        updateServiceClient(caller.token())(TraceContext.empty).getUpdates,
        (r: update_service.GetUpdatesResponse) => protocolConverters.GetUpdatesResponse.toJson(r),
        protocol = protocol,
        withCloseDelay = true,
      )
    }

  private def getTrees(
      caller: CallerContext,
      protocol: Protocol,
  ): TracedInput[Unit] => Flow[
    update_service.GetUpdatesRequest,
    JsGetUpdateTreesResponse,
    NotUsed,
  ] =
    wsReq => {
      implicit val token: Option[String] = caller.token()
      implicit val tc: TraceContext = wsReq.traceContext
      prepareSingleWsStream(
        updateServiceClient(caller.token()).getUpdateTrees,
        (r: update_service.GetUpdateTreesResponse) =>
          protocolConverters.GetUpdateTreesResponse.toJson(r),
        protocol = protocol,
        withCloseDelay = true,
      )
    }

}

object JsUpdateService extends DocumentationEndpoints {
  import Endpoints.*
  import JsUpdateServiceCodecs.*

  private lazy val updates = v2Endpoint.in(sttp.tapir.stringToPath("updates"))
  val getUpdatesFlatEndpoint = updates.get
    .in(sttp.tapir.stringToPath("flats"))
    .out(
      webSocketBody[
        update_service.GetUpdatesRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetUpdatesResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .description("Get flat transactions update stream")

  val getUpdatesFlatListEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("flats"))
      .in(jsonBody[update_service.GetUpdatesRequest])
      .out(jsonBody[Seq[JsGetUpdatesResponse]])
      .inStreamListParams()
      .description("Query flat transactions update list (blocking call)")

  val getUpdatesTreeEndpoint = updates.get
    .in(sttp.tapir.stringToPath("trees"))
    .out(
      webSocketBody[
        update_service.GetUpdatesRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetUpdateTreesResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .description("Get update transactions tree stream")

  val getUpdatesTreeListEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("trees"))
      .in(jsonBody[update_service.GetUpdatesRequest])
      .out(jsonBody[Seq[JsGetUpdateTreesResponse]])
      .inStreamListParams()
      .description("Query update transactions tree list (blocking call)")

  val getTransactionTreeByOffsetEndpoint = updates.get
    .in(sttp.tapir.stringToPath("transaction-tree-by-offset"))
    .in(path[Long]("offset"))
    .in(query[List[String]]("parties"))
    .out(jsonBody[JsGetTransactionTreeResponse])
    .description("Get transaction tree by offset")

  val getTransactionTreeByIdEndpoint: Endpoint[
    CallerContext,
    (String, List[String]),
    JsCantonError,
    JsGetTransactionTreeResponse,
    Any,
  ] = updates.get
    .in(sttp.tapir.stringToPath("transaction-tree-by-id"))
    .in(path[String]("update-id"))
    .in(query[List[String]]("parties"))
    .out(jsonBody[JsGetTransactionTreeResponse])
    .description("Get transaction tree by  id")

  val getTransactionByIdEndpoint =
    updates.get
      .in(sttp.tapir.stringToPath("transaction-by-id"))
      .in(path[String]("update-id"))
      .in(query[List[String]]("parties"))
      .out(jsonBody[JsGetTransactionResponse])
      .description("Get transaction by id")

  val getTransactionByOffsetEndpoint =
    updates.get
      .in(sttp.tapir.stringToPath("transaction-by-offset"))
      .in(path[Long]("offset"))
      .in(query[List[String]]("parties"))
      .out(jsonBody[JsGetTransactionResponse])
      .description("Get transaction by offset")

  override def documentation: Seq[AnyEndpoint] = List(
    getUpdatesFlatEndpoint,
    getUpdatesFlatListEndpoint,
    getUpdatesTreeEndpoint,
    getUpdatesTreeListEndpoint,
    getTransactionTreeByOffsetEndpoint,
    getTransactionByOffsetEndpoint,
    getTransactionByIdEndpoint,
    getTransactionTreeByIdEndpoint,
  )
}

object JsReassignmentEvent {
  sealed trait JsReassignmentEvent

  final case class JsAssignmentEvent(
      source: String,
      target: String,
      unassignId: String,
      submitter: String,
      reassignmentCounter: Long,
      createdEvent: CreatedEvent,
  ) extends JsReassignmentEvent

  final case class JsUnassignedEvent(value: reassignment.UnassignedEvent)
      extends JsReassignmentEvent

}

final case class JsReassignment(
    updateId: String,
    commandId: String,
    workflowId: String,
    offset: Long,
    event: JsReassignmentEvent.JsReassignmentEvent,
    traceContext: Option[com.daml.ledger.api.v2.trace_context.TraceContext],
    recordTime: com.google.protobuf.timestamp.Timestamp,
)

object JsUpdate {
  sealed trait Update
  final case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
  final case class Reassignment(value: JsReassignment) extends Update
  final case class Transaction(value: JsTransaction) extends Update
  final case class TopologyTransaction(value: JsTopologyTransaction) extends Update
}

final case class JsGetTransactionTreeResponse(transaction: JsTransactionTree)

final case class JsGetTransactionResponse(transaction: JsTransaction)

final case class JsGetUpdatesResponse(
    update: JsUpdate.Update
)

object JsUpdateTree {
  sealed trait Update
  final case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
  final case class Reassignment(value: JsReassignment) extends Update
  final case class TransactionTree(value: JsTransactionTree) extends Update
  final case class TopologyTransaction(value: JsTopologyTransaction) extends Update
}

final case class JsGetUpdateTreesResponse(
    update: JsUpdateTree.Update
)

object JsUpdateServiceCodecs {
  import JsCommandServiceCodecs.*
  import JsStateServiceCodecs.*

  implicit val participantAuthorizationTopologyFormatRW
      : Codec[ParticipantAuthorizationTopologyFormat] = deriveCodec
  implicit val transactionShapeFormatRW: Codec[transaction_filter.TransactionShape] = deriveCodec
  implicit val transactionFormatRW: Codec[transaction_filter.TransactionFormat] = deriveCodec
  implicit val topologyFormatRW: Codec[transaction_filter.TopologyFormat] = deriveCodec
  implicit val updateFormatRW: Codec[transaction_filter.UpdateFormat] = deriveCodec
  implicit val getUpdatesRequest: Codec[update_service.GetUpdatesRequest] = deriveCodec

  implicit val jsGetUpdatesResponse: Codec[JsGetUpdatesResponse] = deriveCodec

  implicit val jsUpdateRW: Codec[JsUpdate.Update] = deriveCodec

  implicit val jsUpdateOffsetCheckpoint: Codec[JsUpdate.OffsetCheckpoint] = deriveCodec
  implicit val jsUpdateReassignment: Codec[JsUpdate.Reassignment] = deriveCodec
  implicit val jsUpdateTransaction: Codec[JsUpdate.Transaction] = deriveCodec
  implicit val jsUpdateTopologyTransaction: Codec[JsUpdate.TopologyTransaction] = deriveCodec
  implicit val jsReassignment: Codec[JsReassignment] = deriveCodec

  implicit val jsReassignmentEventRW: Codec[JsReassignmentEvent.JsReassignmentEvent] = deriveCodec

  implicit val jsReassignmentEventJsUnassignedEventRW
      : Codec[JsReassignmentEvent.JsUnassignedEvent] =
    deriveCodec

  implicit val jsReassignmentEventJsAssignedEventRW: Codec[JsReassignmentEvent.JsAssignmentEvent] =
    deriveCodec

  implicit val jsGetUpdateTreesResponse: Codec[JsGetUpdateTreesResponse] = deriveCodec

  implicit val jsGetTransactionTreeResponse: Codec[JsGetTransactionTreeResponse] = deriveCodec
  implicit val jsGetTransactionResponse: Codec[JsGetTransactionResponse] = deriveCodec

  implicit val jsUpdateTree: Codec[JsUpdateTree.Update] = deriveCodec
  implicit val jsUpdateTreeReassignment: Codec[JsUpdateTree.Reassignment] = deriveCodec
  implicit val jsUpdateTreeTransaction: Codec[JsUpdateTree.TransactionTree] = deriveCodec
  implicit val jsUpdateTreeTopologyTransaction: Codec[JsUpdateTree.TopologyTransaction] =
    deriveCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsReassignmentEventSchema: Schema[JsReassignmentEvent.JsReassignmentEvent] =
    Schema.oneOfWrapped

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsUpdateSchema: Schema[JsUpdate.Update] = Schema.oneOfWrapped

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsUpdateTreeSchema: Schema[JsUpdateTree.Update] = Schema.oneOfWrapped
}
