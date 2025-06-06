// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.{event, event_query_service}
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.{JsCantonError, JsEvent}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, Schema}

import scala.concurrent.{ExecutionContext, Future}

class JsEventService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends Endpoints
    with NamedLogging {

  private def eventServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): event_query_service.EventQueryServiceGrpc.EventQueryServiceStub =
    ledgerClient.serviceClient(event_query_service.EventQueryServiceGrpc.stub, token)

  def endpoints() = List(
    withServerLogic(
      JsEventService.getEventsByContractIdEndpoint,
      getEventsByContractId,
    )
  )

  def getEventsByContractId(
      callerContext: CallerContext
  ): TracedInput[event_query_service.GetEventsByContractIdRequest] => Future[
    Either[JsCantonError, JsGetEventsByContractIdResponse]
  ] = req => {
    implicit val tc = req.traceContext
    eventServiceClient(callerContext.token())(req.traceContext)
      .getEventsByContractId(req.in)
      .flatMap(protocolConverters.GetEventsByContractIdResponse.toJson(_))
      .resultToRight
  }
}

final case class JsCreated(
    createdEvent: JsEvent.CreatedEvent,
    synchronizerId: String,
)
final case class JsArchived(
    archivedEvent: JsEvent.ArchivedEvent,
    synchronizerId: String,
)

final case class JsGetEventsByContractIdResponse(
    created: Option[JsCreated],
    archived: Option[JsArchived],
)

object JsEventService extends DocumentationEndpoints {
  import Endpoints.*
  import JsEventServiceCodecs.*

  private lazy val events = v2Endpoint.in(sttp.tapir.stringToPath("events"))
  val getEventsByContractIdEndpoint = events.post
    .in(sttp.tapir.stringToPath("events-by-contract-id"))
    .in(jsonBody[event_query_service.GetEventsByContractIdRequest])
    .out(jsonBody[JsGetEventsByContractIdResponse])
    .description("Get events by contract Id")

  override def documentation: Seq[AnyEndpoint] = Seq(
    getEventsByContractIdEndpoint
  )
}
object JsEventServiceCodecs {
  import JsSchema.*
  import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
  import JsServicesCommonCodecs.*

  implicit val jsCreatedRW: Codec[JsCreated] = deriveConfiguredCodec
  implicit val jsArchivedRW: Codec[JsArchived] = deriveConfiguredCodec

  implicit val archivedEventRW: Codec[event.ArchivedEvent] = deriveRelaxedCodec

  implicit val jsGetEventsByContractIdResponseRW: Codec[JsGetEventsByContractIdResponse] =
    deriveConfiguredCodec

  implicit val getEventsByContractIdRequestRW
      : Codec[event_query_service.GetEventsByContractIdRequest] =
    deriveRelaxedCodec

  implicit val jsCreatedSchema: Schema[JsCreated] = Schema.derived
}
