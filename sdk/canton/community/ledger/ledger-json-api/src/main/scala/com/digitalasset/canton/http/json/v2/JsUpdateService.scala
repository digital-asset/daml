// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.transaction_filter.ParticipantAuthorizationTopologyFormat
import com.daml.ledger.api.v2.{offset_checkpoint, transaction_filter, update_service}
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  JsReassignment,
  JsReassignmentEvent,
  JsTopologyTransaction,
  JsTransaction,
  JsTransactionTree,
}
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, CodecFormat, Schema, path, query, webSocketBody}

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
      JsUpdateService.getUpdateByOffsetEndpoint,
      getUpdateByOffset,
    ),
    withServerLogic(
      JsUpdateService.getTransactionByIdEndpoint,
      getTransactionById,
    ),
    withServerLogic(
      JsUpdateService.getUpdateByIdEndpoint,
      getUpdateById,
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
    implicit val tc: TraceContext = req.traceContext
    updateServiceClient(caller.token())(req.traceContext)
      .getTransactionTreeByOffset(
        update_service.GetTransactionByOffsetRequest(
          offset = req.in._1,
          requestingParties = req.in._2,
          transactionFormat = None,
        )
      )
      .flatMap(protocolConverters.GetTransactionTreeResponse.toJson(_))
      .resultToRight
  }

  private def getTransactionByOffset(
      caller: CallerContext
  ): TracedInput[update_service.GetTransactionByOffsetRequest] => Future[
    Either[JsCantonError, JsGetTransactionResponse]
  ] =
    req => {
      implicit val tc = req.traceContext
      updateServiceClient(caller.token())(req.traceContext)
        .getTransactionByOffset(req.in)
        .flatMap(protocolConverters.GetTransactionResponse.toJson(_))
        .resultToRight
    }

  private def getUpdateByOffset(
      caller: CallerContext
  ): TracedInput[update_service.GetUpdateByOffsetRequest] => Future[
    Either[JsCantonError, JsGetUpdateResponse]
  ] =
    req => {
      implicit val tc = req.traceContext
      updateServiceClient(caller.token())(req.traceContext)
        .getUpdateByOffset(req.in)
        .flatMap(protocolConverters.GetUpdateResponse.toJson(_))
        .resultToRight
    }

  private def getUpdateById(
      caller: CallerContext
  ): TracedInput[update_service.GetUpdateByIdRequest] => Future[
    Either[JsCantonError, JsGetUpdateResponse]
  ] =
    req => {
      implicit val tc = req.traceContext
      updateServiceClient(caller.token())(req.traceContext)
        .getUpdateById(req.in)
        .flatMap(protocolConverters.GetUpdateResponse.toJson(_))
        .resultToRight
    }

  private def getTransactionById(
      caller: CallerContext
  ): TracedInput[update_service.GetTransactionByIdRequest] => Future[
    Either[JsCantonError, JsGetTransactionResponse]
  ] = { req =>
    implicit val tc = req.traceContext
    updateServiceClient(caller.token())(req.traceContext)
      .getTransactionById(req.in)
      .flatMap(protocolConverters.GetTransactionResponse.toJson(_))
      .resultToRight
  }

  private def getTransactionTreeById(
      caller: CallerContext
  ): TracedInput[(String, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionTreeResponse]
  ] =
    req => {
      implicit val tc = req.traceContext
      updateServiceClient(caller.token())(req.traceContext)
        .getTransactionTreeById(
          update_service.GetTransactionByIdRequest(
            updateId = req.in._1,
            requestingParties = req.in._2,
            transactionFormat = None,
          )
        )
        .flatMap { tr =>
          protocolConverters.GetTransactionTreeResponse.toJson(tr)
        }
        .resultToRight
    }

  private def getFlats(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[update_service.GetUpdatesRequest, JsGetUpdatesResponse, NotUsed] =
    req => {
      implicit val tc = req.traceContext
      prepareSingleWsStream(
        updateServiceClient(caller.token())(TraceContext.empty).getUpdates,
        (r: update_service.GetUpdatesResponse) => protocolConverters.GetUpdatesResponse.toJson(r),
      )
    }

  private def getTrees(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[
    update_service.GetUpdatesRequest,
    JsGetUpdateTreesResponse,
    NotUsed,
  ] =
    wsReq => {
      implicit val tc: TraceContext = wsReq.traceContext
      prepareSingleWsStream(
        updateServiceClient(caller.token()).getUpdateTrees,
        (r: update_service.GetUpdateTreesResponse) =>
          protocolConverters.GetUpdateTreesResponse.toJson(r),
      )
    }

}

object JsUpdateService extends DocumentationEndpoints {
  import Endpoints.*
  import JsUpdateServiceCodecs.*
  import JsSchema.JsServicesCommonCodecs.*

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

  val getTransactionTreeByIdEndpoint = updates.get
    .in(sttp.tapir.stringToPath("transaction-tree-by-id"))
    .in(path[String]("update-id"))
    .in(query[List[String]]("parties"))
    .out(jsonBody[JsGetTransactionTreeResponse])
    .description("Get transaction tree by  id")

  val getTransactionByIdEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("transaction-by-id"))
      .in(jsonBody[update_service.GetTransactionByIdRequest])
      .out(jsonBody[JsGetTransactionResponse])
      .description("Get transaction by id")

  val getTransactionByOffsetEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("transaction-by-offset"))
      .in(jsonBody[update_service.GetTransactionByOffsetRequest])
      .out(jsonBody[JsGetTransactionResponse])
      .description("Get transaction by offset")

  val getUpdateByOffsetEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("update-by-offset"))
      .in(jsonBody[update_service.GetUpdateByOffsetRequest])
      .out(jsonBody[JsGetUpdateResponse])
      .description("Get update by offset")

  val getUpdateByIdEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("update-by-id"))
      .in(jsonBody[update_service.GetUpdateByIdRequest])
      .out(jsonBody[JsGetUpdateResponse])
      .description("Get update by id")

  override def documentation: Seq[AnyEndpoint] = List(
    getUpdatesFlatEndpoint,
    getUpdatesFlatListEndpoint,
    getUpdatesTreeEndpoint,
    getUpdatesTreeListEndpoint,
    getTransactionTreeByOffsetEndpoint,
    getTransactionByOffsetEndpoint,
    getUpdateByOffsetEndpoint,
    getTransactionByIdEndpoint,
    getUpdateByIdEndpoint,
    getTransactionTreeByIdEndpoint,
  )
}

object JsUpdate {
  sealed trait Update
  final case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
  final case class Reassignment(value: JsReassignment) extends Update
  final case class Transaction(value: JsTransaction) extends Update
  final case class TopologyTransaction(value: JsTopologyTransaction) extends Update
}

final case class JsGetTransactionTreeResponse(transaction: JsTransactionTree)

final case class JsGetTransactionResponse(transaction: JsTransaction)

final case class JsGetUpdateResponse(update: JsUpdate.Update)

final case class JsGetUpdatesResponse(
    update: JsUpdate.Update
)

object JsUpdateTree {
  sealed trait Update
  final case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
  final case class Reassignment(value: JsReassignment) extends Update
  final case class TransactionTree(value: JsTransactionTree) extends Update
}

final case class JsGetUpdateTreesResponse(
    update: JsUpdateTree.Update
)

object JsUpdateServiceCodecs {
  import JsSchema.config
  import JsSchema.JsServicesCommonCodecs.*

  implicit val participantAuthorizationTopologyFormatRW
      : Codec[ParticipantAuthorizationTopologyFormat] = deriveRelaxedCodec
  implicit val topologyFormatRW: Codec[transaction_filter.TopologyFormat] = deriveRelaxedCodec
  implicit val updateFormatRW: Codec[transaction_filter.UpdateFormat] = deriveRelaxedCodec
  implicit val getUpdatesRequest: Codec[update_service.GetUpdatesRequest] = deriveRelaxedCodec
  implicit val getTransactionByIdRequestRW: Codec[update_service.GetTransactionByIdRequest] =
    deriveRelaxedCodec
  implicit val getTransactionByOffsetRequestRW
      : Codec[update_service.GetTransactionByOffsetRequest] =
    deriveRelaxedCodec
  implicit val getUpdateByIdRequestRW: Codec[update_service.GetUpdateByIdRequest] =
    deriveRelaxedCodec
  implicit val getUpdateByOffsetRequestRW: Codec[update_service.GetUpdateByOffsetRequest] =
    deriveRelaxedCodec

  implicit val jsGetUpdatesResponse: Codec[JsGetUpdatesResponse] = deriveConfiguredCodec

  implicit val jsUpdateRW: Codec[JsUpdate.Update] = deriveConfiguredCodec

  implicit val jsUpdateOffsetCheckpoint: Codec[JsUpdate.OffsetCheckpoint] = deriveConfiguredCodec
  implicit val jsUpdateReassignment: Codec[JsUpdate.Reassignment] = deriveConfiguredCodec
  implicit val jsUpdateTransaction: Codec[JsUpdate.Transaction] = deriveConfiguredCodec
  implicit val jsUpdateTopologyTransaction: Codec[JsUpdate.TopologyTransaction] =
    deriveConfiguredCodec

  implicit val jsGetUpdateTreesResponse: Codec[JsGetUpdateTreesResponse] = deriveConfiguredCodec

  implicit val jsGetTransactionTreeResponse: Codec[JsGetTransactionTreeResponse] =
    deriveConfiguredCodec
  implicit val jsGetTransactionResponse: Codec[JsGetTransactionResponse] = deriveConfiguredCodec
  implicit val jsGetUpdateResponse: Codec[JsGetUpdateResponse] = deriveConfiguredCodec

  implicit val jsUpdateTree: Codec[JsUpdateTree.Update] = deriveConfiguredCodec
  implicit val jsUpdateTreeReassignment: Codec[JsUpdateTree.Reassignment] = deriveConfiguredCodec
  implicit val jsUpdateTreeTransaction: Codec[JsUpdateTree.TransactionTree] = deriveConfiguredCodec

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsReassignmentEventSchema: Schema[JsReassignmentEvent.JsReassignmentEvent] =
    Schema.oneOfWrapped

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsUpdateSchema: Schema[JsUpdate.Update] = Schema.oneOfWrapped

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsUpdateTreeSchema: Schema[JsUpdateTree.Update] = Schema.oneOfWrapped
}
