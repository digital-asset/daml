// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.party_management_service
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.ledger.client.services.admin.PartyManagementClient
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.generic.auto.*

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{AnyEndpoint, Endpoint, path, query}

import scala.concurrent.{ExecutionContext, Future}

class JsPartyManagementService(
    partyManagementClient: PartyManagementClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends Endpoints
    with NamedLogging {
  import JsPartyManagementService.*

  def endpoints(): List[ServerEndpoint[Any, Future]] =
    List(
      withServerLogic(
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

  private val listKnownParties: CallerContext => TracedInput[Unit] => Future[
    Either[JsCantonError, party_management_service.ListKnownPartiesResponse]
  ] = ctx =>
    req =>
      partyManagementClient
        .serviceStub(ctx.token())(req.traceContext)
        .listKnownParties(party_management_service.ListKnownPartiesRequest())
        .resultToRight
  // TODO (i19538) paging

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

  private val allocateParty
      : CallerContext => TracedInput[party_management_service.AllocatePartyRequest] => Future[
        Either[JsCantonError, party_management_service.AllocatePartyResponse]
      ] =
    caller =>
      req =>
        partyManagementClient
          .serviceStub(caller.token())(req.traceContext)
          .allocateParty(req.in)
          .resultToRight

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

  val allocatePartyEndpoint: Endpoint[
    CallerContext,
    party_management_service.AllocatePartyRequest,
    JsCantonError,
    party_management_service.AllocatePartyResponse,
    Any,
  ] = parties.post
    .in(jsonBody[party_management_service.AllocatePartyRequest])
    .out(jsonBody[party_management_service.AllocatePartyResponse])
    .description("Allocate a new party to the participant node")

  val listKnownPartiesEndpoint =
    parties.get
      .out(jsonBody[party_management_service.ListKnownPartiesResponse])
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

  implicit val partyDetails: Codec[party_management_service.PartyDetails] = deriveCodec
  implicit val listKnownPartiesResponse: Codec[party_management_service.ListKnownPartiesResponse] =
    deriveCodec

  implicit val allocatePartyRequest: Codec[party_management_service.AllocatePartyRequest] =
    deriveCodec
  implicit val allocatePartyResponse: Codec[party_management_service.AllocatePartyResponse] =
    deriveCodec

  implicit val getPartiesRequest: Codec[party_management_service.GetPartiesRequest] =
    deriveCodec
  implicit val getPartiesResponse: Codec[party_management_service.GetPartiesResponse] =
    deriveCodec

  implicit val updatePartyDetailsRequest
      : Codec[party_management_service.UpdatePartyDetailsRequest] =
    deriveCodec
  implicit val updatePartyDetailsResponse
      : Codec[party_management_service.UpdatePartyDetailsResponse] =
    deriveCodec

  implicit val getParticipantIdResponse: Codec[party_management_service.GetParticipantIdResponse] =
    deriveCodec
}
