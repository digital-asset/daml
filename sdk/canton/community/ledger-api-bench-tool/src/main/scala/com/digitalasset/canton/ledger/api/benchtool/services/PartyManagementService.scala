// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.services

import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocatePartyRequest,
  ListKnownPartiesRequest,
  PartyManagementServiceGrpc,
}
import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.ledger.api.benchtool.AuthorizationHelper
import io.grpc.Channel
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class PartyManagementService(channel: Channel, authorizationToken: Option[String]) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val service: PartyManagementServiceGrpc.PartyManagementServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(
      PartyManagementServiceGrpc.stub(channel)
    )

  def listKnownParties()(implicit ec: ExecutionContext): Future[Set[String]] =
    service
      .listKnownParties(
        new ListKnownPartiesRequest(
          pageToken = "",
          pageSize = 0,
          identityProviderId = "",
          filterParty = "",
        )
      )
      .map(_.partyDetails.map(_.party).toSet)

  def allocateParty(hint: String, synchronizerId: Option[String] = None)(implicit
      ec: ExecutionContext
  ): Future[Party] =
    service
      .allocateParty(
        AllocatePartyRequest(
          partyIdHint = hint,
          localMetadata = None,
          identityProviderId = "",
          synchronizerId = synchronizerId.getOrElse(""),
          userId = "",
        )
      )
      .map { response =>
        response.partyDetails match {
          case Some(details) => new Party(details.party)
          case _ => throw new NoSuchElementException(s"No party details in response $response.")
        }
      }
      .transformWith {
        case Success(party) =>
          Future.successful {
            logger.info(s"Allocated party: $party")
            party
          }
        case Failure(exception) =>
          Future.failed {
            logger.error(
              s"Error during party allocation. Details: ${exception.getLocalizedMessage}",
              exception,
            )
            exception
          }
      }
}
