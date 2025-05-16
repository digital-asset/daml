// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.party_management_service
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.services.admin.PartyManagementClient
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{AnyEndpoint, path, query}

import scala.concurrent.{ExecutionContext, Future}

class JsPartyManagementService(
    partyManagementClient: PartyManagementClient,
    protocolConverters: ProtocolConverters,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends Endpoints
    with NamedLogging {
  import JsPartyManagementService.*

  def endpoints(): List[ServerEndpoint[Any, Future]] =
    List(
      asPagedList(
        JsPartyManagementService.listKnownPartiesEndpoint,
        listKnownParties,
      ),
      withServerLogic(
        allocatePartyEndpoint,
        allocateParty,
      ),
      withServerLogic(
        JsPartyManagementService.getParticipantIdEndpoint,
        getParticipantId,
      ),
      withServerLogic(JsPartyManagementService.getPartyEndpoint, getParty),
      withServerLogic(
        JsPartyManagementService.updatePartyEndpoint,
        updateParty,
      ),
    )

  private val listKnownParties: CallerContext => TracedInput[PagedList[Unit]] => Future[
    Either[JsCantonError, party_management_service.ListKnownPartiesResponse]
  ] = ctx =>
    req =>
      partyManagementClient
        .serviceStub(ctx.token())(req.traceContext)
        .listKnownParties(
          party_management_service.ListKnownPartiesRequest(
            req.in.pageToken.getOrElse(""),
            req.in.pageSize.getOrElse(0),
            "",
          )
        )
        .resultToRight

  private val getParty
      : CallerContext => TracedInput[(String, Option[String], List[String])] => Future[
        Either[JsCantonError, party_management_service.GetPartiesResponse]
      ] = ctx => { req =>
    val parties = req.in._1 +: req.in._3
    val partyRequest = party_management_service.GetPartiesRequest(
      parties = parties,
      identityProviderId = req.in._2.getOrElse(""),
    )
    partyManagementClient
      .serviceStub(ctx.token())(req.traceContext)
      .getParties(partyRequest)
      .resultToRight
  }

  private val getParticipantId: CallerContext => TracedInput[Unit] => Future[
    Either[JsCantonError, party_management_service.GetParticipantIdResponse]
  ] = ctx => { req =>
    partyManagementClient
      .serviceStub(ctx.token())(req.traceContext)
      .getParticipantId(party_management_service.GetParticipantIdRequest())
      .resultToRight
  }

  private val allocateParty: CallerContext => TracedInput[js.AllocatePartyRequest] => Future[
    Either[JsCantonError, party_management_service.AllocatePartyResponse]
  ] =
    caller =>
      req =>
        for {
          request <- protocolConverters.AllocatePartyRequest.fromJson(req.in)
          response <- partyManagementClient
            .serviceStub(caller.token())(req.traceContext)
            .allocateParty(request)
            .resultToRight
        } yield response

  private val updateParty: CallerContext => TracedInput[
    (String, party_management_service.UpdatePartyDetailsRequest)
  ] => Future[Either[JsCantonError, party_management_service.UpdatePartyDetailsResponse]] =
    caller =>
      req =>
        if (req.in._2.partyDetails.map(_.party).contains(req.in._1)) {
          partyManagementClient
            .serviceStub(caller.token())(req.traceContext)
            .updatePartyDetails(req.in._2)
            .resultToRight
        } else {
          implicit val traceContext: TraceContext = req.traceContext
          error(
            JsCantonError.fromErrorCode(
              InvalidArgument.Reject(
                s"${req.in._1} does not match party in body ${req.in._2.partyDetails}"
              )
            )
          )
        }
}

object JsPartyManagementService extends DocumentationEndpoints {
  import Endpoints.*
  import JsPartyManagementCodecs.*

  private val parties = v2Endpoint.in(sttp.tapir.stringToPath("parties"))
  private val partyPath = "party"

  val allocatePartyEndpoint = parties.post
    .in(jsonBody[js.AllocatePartyRequest])
    .out(jsonBody[party_management_service.AllocatePartyResponse])
    .description("Allocate a new party to the participant node")

  val listKnownPartiesEndpoint =
    parties.get
      .out(jsonBody[party_management_service.ListKnownPartiesResponse])
      .inPagedListParams()
      .description("List all known parties.")

  val getParticipantIdEndpoint =
    parties.get
      .in(sttp.tapir.stringToPath("participant-id"))
      .out(jsonBody[party_management_service.GetParticipantIdResponse])
      .description("Get participant id")

  val getPartyEndpoint =
    parties.get
      .in(path[String](partyPath))
      .in(query[Option[String]]("identity-provider-id"))
      .in(query[List[String]]("parties"))
      .out(jsonBody[party_management_service.GetPartiesResponse])
      .description("Get party details")

  val updatePartyEndpoint = parties.patch
    .in(path[String](partyPath))
    .in(jsonBody[party_management_service.UpdatePartyDetailsRequest])
    .out(jsonBody[party_management_service.UpdatePartyDetailsResponse])
    .description("Allocate a new party to the participant node")
  override def documentation: Seq[AnyEndpoint] = Seq(
    listKnownPartiesEndpoint,
    allocatePartyEndpoint,
    getParticipantIdEndpoint,
    getPartyEndpoint,
    updatePartyEndpoint,
  )
}

object JsPartyManagementCodecs {
  import JsSchema.config

  implicit val partyDetails: Codec[party_management_service.PartyDetails] = deriveRelaxedCodec
  implicit val listKnownPartiesResponse: Codec[party_management_service.ListKnownPartiesResponse] =
    deriveRelaxedCodec

  implicit val allocatePartyRequest: Codec[js.AllocatePartyRequest] =
    deriveConfiguredCodec
  implicit val allocatePartyResponse: Codec[party_management_service.AllocatePartyResponse] =
    deriveRelaxedCodec

  implicit val getPartiesRequest: Codec[party_management_service.GetPartiesRequest] =
    deriveRelaxedCodec
  implicit val getPartiesResponse: Codec[party_management_service.GetPartiesResponse] =
    deriveRelaxedCodec

  implicit val updatePartyDetailsRequest
      : Codec[party_management_service.UpdatePartyDetailsRequest] =
    deriveRelaxedCodec
  implicit val updatePartyDetailsResponse
      : Codec[party_management_service.UpdatePartyDetailsResponse] =
    deriveRelaxedCodec

  implicit val getParticipantIdResponse: Codec[party_management_service.GetParticipantIdResponse] =
    deriveRelaxedCodec
}
