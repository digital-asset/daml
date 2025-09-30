// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      .transformWith {
        case Success(response) =>
          Future.successful {
            val party = new Party(response.partyDetails.get.party)
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
