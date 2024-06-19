// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json2

import com.daml.ledger.api.v2.admin.party_management_service
import com.digitalasset.canton.ledger.client.services.admin.PartyManagementClient
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import sttp.tapir.generic.auto.*

import scala.concurrent.{ExecutionContext, Future}
import com.digitalasset.canton.http.json2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import sttp.tapir.{path, query}

class JsPartyManagementService(
    partyManagementClient: PartyManagementClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext
) extends Endpoints
    with NamedLogging {
  import JsPartyManagementCodecs.*

  private val parties = baseEndpoint.in("parties")
  private val partyPath = "party"

  def endpoints() =
    List(
      json(
        parties.get
          .description("List all known parties."),
        listKnownParties,
      ),
      jsonWithBody(
        parties.post
          .description("Allocate a new party to the participant node"),
        allocateParty,
      ),
      json(
        parties.get
          .in("participant-id")
          .description("Get participant id"),
        getParticipantId,
      ),
      json(
        parties.get
          .in(path[String](partyPath))
          .in(query[Option[String]]("identity-provider-id"))
          .in(query[List[String]]("parties"))
          .description("Get party details"),
        getParty,
      ),
      jsonWithBody(
        parties.patch
          .in(path[String](partyPath))
          .description("Allocate a new party to the participant node"),
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
        .toRight
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
      .toRight
  }

  private val getParticipantId: CallerContext => TracedInput[Unit] => Future[
    Either[JsCantonError, party_management_service.GetParticipantIdResponse]
  ] = ctx => { req =>
    partyManagementClient
      .serviceStub(ctx.token())(req.traceContext)
      .getParticipantId(party_management_service.GetParticipantIdRequest())
      .toRight
  }

  private val allocateParty: CallerContext => (
      TracedInput[Unit],
      party_management_service.AllocatePartyRequest,
  ) => Future[Either[JsCantonError, party_management_service.AllocatePartyResponse]] =
    caller =>
      (req, body) =>
        partyManagementClient
          .serviceStub(caller.token())(req.traceContext)
          .allocateParty(body)
          .toRight

  private val updateParty: CallerContext => (
      TracedInput[String],
      party_management_service.UpdatePartyDetailsRequest,
  ) => Future[Either[JsCantonError, party_management_service.UpdatePartyDetailsResponse]] =
    caller =>
      (req, body) =>
        if (body.partyDetails.map(_.party) == Some(req.in)) {
          partyManagementClient
            .serviceStub(caller.token())(req.traceContext)
            .updatePartyDetails(body)
            .toRight
        } else {
          implicit val traceContext = req.traceContext
          error(
            JsCantonError.fromErrorCode(
              InvalidArgument.Reject(
                s"${req.in} does not match party in body ${body.partyDetails}"
              )
            )
          )
        }
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
