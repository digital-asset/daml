// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter.WildcardFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  ParticipantAuthorizationTopologyFormat,
  TransactionFormat,
  UpdateFormat,
}
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.api.v2.{offset_checkpoint, transaction_filter, update_service}
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  JsReassignment,
  JsTransaction,
  JsTransactionTree,
}
import com.digitalasset.canton.http.json.v2.JsUpdateServiceConverters.toUpdateFormat
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.Schema.SName
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, CodecFormat, Schema, path, query, webSocketBody}

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

// TODO(#23504) remove deprecated methods
@nowarn("cat=deprecation")
class JsUpdateService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
    wsConfig: WebsocketConfig,
    materializer: Materializer,
    val authInterceptor: AuthInterceptor,
) extends Endpoints
    with NamedLogging {

  private def updateServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): update_service.UpdateServiceGrpc.UpdateServiceStub =
    ledgerClient.serviceClient(update_service.UpdateServiceGrpc.stub, token)

  def endpoints() = List(
    websocket(
      JsUpdateService.getUpdatesEndpoint,
      getUpdates,
    ),
    asList(
      JsUpdateService.getUpdatesListEndpoint,
      getUpdates,
      timeoutOpenEndedStream = (r: LegacyDTOs.GetUpdatesRequest) => r.endInclusive.isEmpty,
    ),
    websocket(
      JsUpdateService.getUpdatesFlatEndpoint,
      getFlats,
    ),
    asList(
      JsUpdateService.getUpdatesFlatListEndpoint,
      getFlats,
      timeoutOpenEndedStream = (r: LegacyDTOs.GetUpdatesRequest) => r.endInclusive.isEmpty,
    ),
    websocket(
      JsUpdateService.getUpdatesTreeEndpoint,
      getTrees,
    ),
    asList(
      JsUpdateService.getUpdatesTreeListEndpoint,
      getTrees,
      timeoutOpenEndedStream = (r: LegacyDTOs.GetUpdatesRequest) => r.endInclusive.isEmpty,
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

  private def getUpdates(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[LegacyDTOs.GetUpdatesRequest, JsGetUpdatesResponse, NotUsed] =
    req => {
      implicit val tc = req.traceContext
      Flow[LegacyDTOs.GetUpdatesRequest].map { request =>
        update_service.GetUpdatesRequest(
          beginExclusive = request.beginExclusive,
          endInclusive = request.endInclusive,
          filter = request.filter.map(f =>
            transaction_filter.TransactionFilter(
              filtersByParty = f.filtersByParty,
              filtersForAnyParty = f.filtersForAnyParty,
            )
          ),
          verbose = request.verbose,
          updateFormat = request.updateFormat,
        )
      } via
        prepareSingleWsStream(
          updateServiceClient(caller.token())(TraceContext.empty).getUpdates,
          (r: update_service.GetUpdatesResponse) => protocolConverters.GetUpdatesResponse.toJson(r),
        )
    }

  private def getFlats(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[LegacyDTOs.GetUpdatesRequest, JsGetUpdatesResponse, NotUsed] =
    req => {
      implicit val tc = req.traceContext
      Flow[LegacyDTOs.GetUpdatesRequest].map { req =>
        toGetUpdatesRequest(req, forTrees = false)
      } via
        prepareSingleWsStream(
          updateServiceClient(caller.token())(TraceContext.empty).getUpdates,
          (r: update_service.GetUpdatesResponse) => protocolConverters.GetUpdatesResponse.toJson(r),
        )
    }

  private def getTrees(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[
    LegacyDTOs.GetUpdatesRequest,
    JsGetUpdateTreesResponse,
    NotUsed,
  ] =
    wsReq => {
      implicit val tc: TraceContext = wsReq.traceContext
      Flow[LegacyDTOs.GetUpdatesRequest].map { req =>
        toGetUpdatesRequest(req, forTrees = true)
      } via
        prepareSingleWsStream(
          updateServiceClient(caller.token()).getUpdates,
          (r: update_service.GetUpdatesResponse) =>
            protocolConverters.GetUpdateTreesResponseLegacy.toJson(toGetUpdateTreesResponse(r)),
        )
    }

  private def toGetUpdatesRequest(
      req: LegacyDTOs.GetUpdatesRequest,
      forTrees: Boolean,
  )(implicit traceContext: TraceContext): update_service.GetUpdatesRequest =
    (req.updateFormat, req.filter, req.verbose) match {
      case (Some(_), Some(_), _) =>
        throw RequestValidationErrors.InvalidArgument
          .Reject(
            "Both update_format and filter are set. Please use either backwards compatible arguments (filter and verbose) or update_format, but not both."
          )
          .asGrpcError
      case (Some(_), _, true) =>
        throw RequestValidationErrors.InvalidArgument
          .Reject(
            "Both update_format and verbose are set. Please use either backwards compatible arguments (filter and verbose) or update_format, but not both."
          )
          .asGrpcError
      case (Some(_), None, false) =>
        update_service.GetUpdatesRequest(
          beginExclusive = req.beginExclusive,
          endInclusive = req.endInclusive,
          filter = None,
          verbose = false,
          updateFormat = req.updateFormat,
        )
      case (None, None, _) =>
        throw RequestValidationErrors.InvalidArgument
          .Reject(
            "Either filter/verbose or update_format is required. Please use either backwards compatible arguments (filter and verbose) or update_format."
          )
          .asGrpcError
      case (None, Some(filter), verbose) =>
        update_service.GetUpdatesRequest(
          beginExclusive = req.beginExclusive,
          endInclusive = req.endInclusive,
          filter = None,
          verbose = false,
          updateFormat = Some(toUpdateFormat(filter, verbose, forTrees)),
        )
    }

  private def toGetUpdateTreesResponse(
      update: update_service.GetUpdatesResponse
  ): LegacyDTOs.GetUpdateTreesResponse =
    LegacyDTOs.GetUpdateTreesResponse(
      update.update match {
        case GetUpdatesResponse.Update.Empty => LegacyDTOs.GetUpdateTreesResponse.Update.Empty
        case GetUpdatesResponse.Update.Transaction(tx) =>
          LegacyDTOs.GetUpdateTreesResponse.Update.TransactionTree(toTransactionTree(tx))
        case GetUpdatesResponse.Update.Reassignment(value) =>
          LegacyDTOs.GetUpdateTreesResponse.Update.Reassignment(value)
        case GetUpdatesResponse.Update.OffsetCheckpoint(value) =>
          LegacyDTOs.GetUpdateTreesResponse.Update.OffsetCheckpoint(value)
        case GetUpdatesResponse.Update.TopologyTransaction(_) =>
          LegacyDTOs.GetUpdateTreesResponse.Update.Empty
      }
    )

  private def toTransactionTree(tx: Transaction): LegacyDTOs.TransactionTree =
    LegacyDTOs.TransactionTree(
      updateId = tx.updateId,
      commandId = tx.commandId,
      workflowId = tx.workflowId,
      effectiveAt = tx.effectiveAt,
      offset = tx.offset,
      eventsById = tx.events
        .collect(e =>
          e.event match {
            case Event.Created(created) =>
              created.nodeId -> LegacyDTOs.TreeEvent(LegacyDTOs.TreeEvent.Kind.Created(created))
            case Event.Exercised(exercised) =>
              exercised.nodeId -> LegacyDTOs.TreeEvent(
                LegacyDTOs.TreeEvent.Kind.Exercised(exercised)
              )
          }
        )
        .toMap,
      synchronizerId = tx.synchronizerId,
      traceContext = tx.traceContext,
      recordTime = tx.recordTime,
    )
}

// TODO(#23504) remove deprecated methods
@nowarn("cat=deprecation")
object JsUpdateService extends DocumentationEndpoints {
  import Endpoints.*
  import JsUpdateServiceCodecs.*
  import JsSchema.JsServicesCommonCodecs.*

  private lazy val updates = v2Endpoint.in(sttp.tapir.stringToPath("updates"))

  val getUpdatesEndpoint = updates.get
    .out(
      webSocketBody[
        LegacyDTOs.GetUpdatesRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetUpdatesResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .description("Get updates stream")

  val getUpdatesListEndpoint =
    updates.post
      .in(jsonBody[LegacyDTOs.GetUpdatesRequest])
      .out(jsonBody[Seq[JsGetUpdatesResponse]])
      .description("Query updates list (blocking call)")
      .inStreamListParamsAndDescription()

  val getUpdatesFlatEndpoint = updates.get
    .in(sttp.tapir.stringToPath("flats"))
    .out(
      webSocketBody[
        LegacyDTOs.GetUpdatesRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetUpdatesResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .description("Get flat transactions update stream (deprecated: use v2/updates instead)")

  val getUpdatesFlatListEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("flats"))
      .in(jsonBody[LegacyDTOs.GetUpdatesRequest])
      .out(jsonBody[Seq[JsGetUpdatesResponse]])
      .description(
        "Query flat transactions update list (blocking call, deprecated: use v2/updates instead)"
      )
      .inStreamListParamsAndDescription()

  val getUpdatesTreeEndpoint = updates.get
    .in(sttp.tapir.stringToPath("trees"))
    .out(
      webSocketBody[
        LegacyDTOs.GetUpdatesRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetUpdateTreesResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .description("Get update transactions tree stream (deprecated: use v2/updates instead)")

  val getUpdatesTreeListEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("trees"))
      .in(jsonBody[LegacyDTOs.GetUpdatesRequest])
      .out(jsonBody[Seq[JsGetUpdateTreesResponse]])
      .description(
        "Query update transactions tree list (blocking call, deprecated: use v2/updates instead)"
      )
      .inStreamListParamsAndDescription()

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
    getUpdatesEndpoint,
    getUpdatesListEndpoint,
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
  final case class TopologyTransaction(value: lapi.topology_transaction.TopologyTransaction)
      extends Update
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

// TODO(#23504) remove suppression of deprecation warnings
@nowarn("cat=deprecation")
object JsUpdateServiceCodecs {
  import JsSchema.config
  import JsSchema.JsServicesCommonCodecs.*

  implicit val participantAuthorizationTopologyFormatRW
      : Codec[ParticipantAuthorizationTopologyFormat] = deriveRelaxedCodec
  implicit val topologyFormatRW: Codec[transaction_filter.TopologyFormat] = deriveRelaxedCodec
  implicit val updateFormatRW: Codec[transaction_filter.UpdateFormat] = deriveRelaxedCodec
  implicit val getUpdatesRequestRW: Codec[update_service.GetUpdatesRequest] = deriveRelaxedCodec
  implicit val getUpdatesRequestLegacyRW: Codec[LegacyDTOs.GetUpdatesRequest] = deriveRelaxedCodec
  implicit val getTransactionByIdRequestRW: Codec[update_service.GetTransactionByIdRequest] =
    deriveRelaxedCodec
  implicit val getTransactionByOffsetRequestRW
      : Codec[update_service.GetTransactionByOffsetRequest] =
    deriveRelaxedCodec
  implicit val getUpdateByIdRequestRW: Codec[update_service.GetUpdateByIdRequest] =
    deriveRelaxedCodec
  implicit val getUpdateByOffsetRequestRW: Codec[update_service.GetUpdateByOffsetRequest] =
    deriveRelaxedCodec

  implicit val jsGetUpdatesResponseRW: Codec[JsGetUpdatesResponse] = deriveConfiguredCodec

  implicit val jsUpdateRW: Codec[JsUpdate.Update] = deriveConfiguredCodec

  implicit val jsUpdateOffsetCheckpointRW: Codec[JsUpdate.OffsetCheckpoint] = deriveConfiguredCodec

  implicit val jsUpdateReassignmentRW: Codec[JsUpdate.Reassignment] = deriveConfiguredCodec
  implicit val jsUpdateTransactionRW: Codec[JsUpdate.Transaction] = deriveConfiguredCodec
  implicit val jsUpdateTopologyTransactionRW: Codec[JsUpdate.TopologyTransaction] =
    deriveConfiguredCodec

  implicit val jsGetUpdateTreesResponseRW: Codec[JsGetUpdateTreesResponse] = deriveConfiguredCodec

  implicit val jsGetTransactionTreeResponseRW: Codec[JsGetTransactionTreeResponse] =
    deriveConfiguredCodec
  implicit val jsGetTransactionResponseRW: Codec[JsGetTransactionResponse] = deriveConfiguredCodec
  implicit val jsGetUpdateResponseRW: Codec[JsGetUpdateResponse] = deriveConfiguredCodec

  implicit val jsUpdateTreeRW: Codec[JsUpdateTree.Update] = deriveConfiguredCodec
  implicit val jsUpdateTreeOffsetCheckpointRW: Codec[JsUpdateTree.OffsetCheckpoint] =
    deriveConfiguredCodec
  implicit val jsUpdateTreeReassignmentRW: Codec[JsUpdateTree.Reassignment] = deriveConfiguredCodec
  implicit val jsUpdateTreeTransactionRW: Codec[JsUpdateTree.TransactionTree] =
    deriveConfiguredCodec

  implicit val jsTopologyParticipantAuthorizationAddedRW
      : Codec[lapi.topology_transaction.ParticipantAuthorizationAdded] =
    deriveRelaxedCodec

  implicit val jsTopologyParticipantAuthorizationChangedRW
      : Codec[lapi.topology_transaction.ParticipantAuthorizationChanged] =
    deriveRelaxedCodec

  implicit val jsTopologyParticipantAuthorizationRevokedRW
      : Codec[lapi.topology_transaction.ParticipantAuthorizationRevoked] =
    deriveRelaxedCodec
  implicit val jsTopologyEventEventRW: Codec[lapi.topology_transaction.TopologyEvent.Event] =
    deriveConfiguredCodec
  implicit val jsTopologyEventParticipantAuthorizationAddedRW
      : Codec[lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationAdded] =
    deriveRelaxedCodec
  implicit val jsParticipantAuthorizationChangedRW
      : Codec[lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationChanged] =
    deriveRelaxedCodec
  implicit val jsParticipantAuthorizationRevokedRW
      : Codec[lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationRevoked] =
    deriveRelaxedCodec

  implicit val jsTopologyEventRW: Codec[lapi.topology_transaction.TopologyEvent] =
    deriveRelaxedCodec
  implicit val jsTopologyTransactionRW: Codec[lapi.topology_transaction.TopologyTransaction] =
    deriveRelaxedCodec
  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs

  implicit val jsTopologyTransactionSchema: Schema[lapi.topology_transaction.TopologyTransaction] =
    Schema.derived.name(Some(SName("JsTopologyTransaction")))

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsUpdateSchema: Schema[JsUpdate.Update] = Schema.oneOfWrapped

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsUpdateTreeSchema: Schema[JsUpdateTree.Update] = Schema.oneOfWrapped

}

object JsUpdateServiceConverters {
  def toUpdateFormat(
      filter: LegacyDTOs.TransactionFilter,
      verbose: Boolean,
      forTrees: Boolean,
  ): UpdateFormat = {
    def addWildcardCond(f: Filters): Filters =
      if (
        f.cumulative.map(_.identifierFilter).exists {
          case _: WildcardFilter => true
          case _ => false
        } || !forTrees
      )
        f
      else
        Filters(cumulative =
          f.cumulative :+ CumulativeFilter(
            WildcardFilter(transaction_filter.WildcardFilter(includeCreatedEventBlob = false))
          )
        )

    val eventFormat = EventFormat(
      filtersByParty = filter.filtersByParty.map { case (party, f) =>
        party -> addWildcardCond(f)
      },
      filtersForAnyParty = filter.filtersForAnyParty.map(addWildcardCond),
      verbose = verbose,
    )

    val transactionFormat = TransactionFormat(
      transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      eventFormat = Some(eventFormat),
    )

    UpdateFormat(
      includeTransactions = Some(transactionFormat),
      includeReassignments = Some(eventFormat),
      includeTopologyEvents = None,
    )
  }

}
