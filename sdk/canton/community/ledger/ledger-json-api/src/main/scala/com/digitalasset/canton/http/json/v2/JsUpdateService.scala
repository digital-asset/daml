// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.ledger.api.v2.update_service
import com.daml.ledger.api.v2.offset_checkpoint
import com.daml.ledger.api.v2.reassignment
import com.digitalasset.canton.http.json.v2.JsSchema.JsEvent.CreatedEvent
import com.digitalasset.canton.http.json.v2.JsSchema.{JsTransaction, JsTransactionTree}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import sttp.tapir.{path, query}

import scala.concurrent.{ExecutionContext, Future}

class JsUpdateService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
) extends Endpoints
    with NamedLogging {

  import JsUpdateServiceCodecs.*

  private lazy val updates = v2Endpoint.in(sttp.tapir.stringToPath("updates"))

  private def updateServiceClient(token: Option[String] = None)(implicit
      traceContext: TraceContext
  ): update_service.UpdateServiceGrpc.UpdateServiceStub =
    ledgerClient.serviceClient(update_service.UpdateServiceGrpc.stub, token)

  def endpoints() = List(
    websocket(
      updates.get
        .in(sttp.tapir.stringToPath("flats"))
        .description("Get flat transactions update stream"),
      getFlats,
    ),
    websocket(
      updates.get
        .in(sttp.tapir.stringToPath("trees"))
        .description("Get update transactions tree stream"),
      getTrees,
    ),
    json(
      updates.get
        .in(sttp.tapir.stringToPath("transaction-tree-by-event-id"))
        .in(path[String]("event-id"))
        .in(query[List[String]]("parties"))
        .description("Get transaction tree by event id"),
      getTreeByEventId,
    ),
    json(
      updates.get
        .in(sttp.tapir.stringToPath("transaction-by-event-id"))
        .in(path[String]("event-id"))
        .in(query[List[String]]("parties"))
        .description("Get transaction by event id"),
      getTransactionByEventId,
    ),
    json(
      updates.get
        .in(sttp.tapir.stringToPath("transaction-by-id"))
        .in(path[String]("update-id"))
        .in(query[List[String]]("parties"))
        .description("Get transaction by id"),
      getTransactionById,
    ),
    json(
      updates.get
        .in(sttp.tapir.stringToPath("transaction-tree-by-id"))
        .in(path[String]("update-id"))
        .in(query[List[String]]("parties"))
        .description("Get transaction tree by  id"),
      getTransactionTreeById,
    ),
  )

  private def getTreeByEventId(
      caller: CallerContext
  ): TracedInput[(String, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionTreeResponse]
  ] = { req =>
    implicit val token = caller.token()
    updateServiceClient(caller.token())(req.traceContext)
      .getTransactionTreeByEventId(
        update_service.GetTransactionByEventIdRequest(
          eventId = req.in._1,
          requestingParties = req.in._2,
        )
      )
      .flatMap(protocolConverters.GetTransactionTreeResponse.toJson(_))
      .resultToRight
  }

  private def getTransactionByEventId(
      caller: CallerContext
  ): TracedInput[(String, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionResponse]
  ] =
    req =>
      updateServiceClient(caller.token())(req.traceContext)
        .getTransactionByEventId(
          update_service.GetTransactionByEventIdRequest(
            eventId = req.in._1,
            requestingParties = req.in._2,
          )
        )
        .flatMap(protocolConverters.GetTransactionResponse.toJson(_)(caller.token()))
        .resultToRight

  private def getTransactionById(
      caller: CallerContext
  ): TracedInput[(String, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionResponse]
  ] = { req =>
    updateServiceClient(caller.token())(req.traceContext)
      .getTransactionById(
        update_service.GetTransactionByIdRequest(
          updateId = req.in._1,
          requestingParties = req.in._2,
        )
      )
      .flatMap(protocolConverters.GetTransactionResponse.toJson(_)(caller.token()))
      .resultToRight
  }

  private def getTransactionTreeById(
      caller: CallerContext
  ): TracedInput[(String, List[String])] => Future[
    Either[JsCantonError, JsGetTransactionTreeResponse]
  ] = { req =>
    implicit val token = caller.token()
    implicit val tc = req.traceContext
    updateServiceClient(caller.token())
      .getTransactionTreeById(
        update_service.GetTransactionByIdRequest(
          updateId = req.in._1,
          requestingParties = req.in._2,
        )
      )
      .flatMap(protocolConverters.GetTransactionTreeResponse.toJson(_))
      .resultToRight
  }

  private def getFlats(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[update_service.GetUpdatesRequest, JsGetUpdatesResponse, NotUsed] =
    _ => {
      Flow[update_service.GetUpdatesRequest]
        .flatMapConcat { req =>
          ClientAdapter
            .serverStreaming(
              req,
              updateServiceClient(caller.token())(TraceContext.empty).getUpdates,
            )
            .mapAsync(1)(r => protocolConverters.GetUpdatesResponse.toJson(r)(caller.token()))
        }
    }

  private def getTrees(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[
    update_service.GetUpdatesRequest,
    JsGetUpdateTreesResponse,
    NotUsed,
  ] =
    wsReq => {
      Flow[update_service.GetUpdatesRequest]
        .flatMapConcat { req =>
          implicit val token = caller.token()
          implicit val tc = wsReq.traceContext
          ClientAdapter
            .serverStreaming(
              req,
              updateServiceClient(caller.token()).getUpdateTrees,
            )
            .mapAsync(1)(r => protocolConverters.GetUpdateTreesResponse.toJson(r))
        }
    }

}

object JsReassignmentEvent {
  sealed trait JsReassignmentEvent

  case class JsAssignmentEvent(
      source: String,
      target: String,
      unassign_id: String,
      submitter: String,
      reassignment_counter: Long,
      created_event: CreatedEvent,
  ) extends JsReassignmentEvent

  case class JsUnassignedEvent(value: reassignment.UnassignedEvent) extends JsReassignmentEvent

}

case class JsReassignment(
    update_id: String,
    command_id: String,
    workflow_id: String,
    offset: String,
    event: JsReassignmentEvent.JsReassignmentEvent,
    trace_context: Option[com.daml.ledger.api.v2.trace_context.TraceContext],
    record_time: com.google.protobuf.timestamp.Timestamp,
)

object JsUpdate {
  sealed trait Update
  case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
  case class Reassignment(value: JsReassignment) extends Update
  case class Transaction(value: JsTransaction) extends Update
}

case class JsGetTransactionTreeResponse(transaction: JsTransactionTree)

case class JsGetTransactionResponse(transaction: JsTransaction)

case class JsGetUpdatesResponse(
    update: JsUpdate.Update
)

object JsUpdateTree {
  sealed trait Update
  case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
  case class Reassignment(value: JsReassignment) extends Update
  case class TransactionTree(value: JsTransactionTree) extends Update
}

case class JsGetUpdateTreesResponse(
    update: JsUpdateTree.Update
)

object JsUpdateServiceCodecs {
  import JsCommandServiceCodecs.*
  import JsStateServiceCodecs.*

  implicit val getUpdatesRequest: Codec[update_service.GetUpdatesRequest] = deriveCodec

  implicit val jsGetUpdatesResponse: Codec[JsGetUpdatesResponse] = deriveCodec

  implicit val offsetCheckpoint: Codec[offset_checkpoint.OffsetCheckpoint] = deriveCodec
  implicit val offsetCheckpointDomainTime: Codec[offset_checkpoint.DomainTime] = deriveCodec
  implicit val jsUpdate: Codec[JsUpdate.Update] = deriveCodec
  implicit val jsUpdateOffsetCheckpoint: Codec[JsUpdate.OffsetCheckpoint] = deriveCodec
  implicit val jsUpdateReassignment: Codec[JsUpdate.Reassignment] = deriveCodec
  implicit val jsUpdateTransaction: Codec[JsUpdate.Transaction] = deriveCodec
  implicit val jsReassignment: Codec[JsReassignment] = deriveCodec
  implicit val jsReassignmentEvent: Codec[JsReassignmentEvent.JsReassignmentEvent] = deriveCodec

  implicit val jsReassignmentEventJsUnassignedEvent: Codec[JsReassignmentEvent.JsUnassignedEvent] =
    deriveCodec

  implicit val unassignedEvent: Codec[reassignment.UnassignedEvent] =
    deriveCodec

  implicit val jsGetUpdateTreesResponse: Codec[JsGetUpdateTreesResponse] = deriveCodec

  implicit val jsGetTransactionTreeResponse: Codec[JsGetTransactionTreeResponse] = deriveCodec
  implicit val jsGetTransactionResponse: Codec[JsGetTransactionResponse] = deriveCodec

  implicit val jsUpdateTree: Codec[JsUpdateTree.Update] = deriveCodec
  implicit val jsUpdateTreeReassignment: Codec[JsUpdateTree.Reassignment] = deriveCodec
  implicit val jsUpdateTreeTransaction: Codec[JsUpdateTree.TransactionTree] = deriveCodec
}
