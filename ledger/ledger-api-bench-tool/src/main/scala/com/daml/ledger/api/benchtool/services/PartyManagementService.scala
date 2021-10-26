// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  PartyManagementServiceGrpc,
}
import com.daml.ledger.client.binding.Primitive.Party
import io.grpc.Channel
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class PartyManagementService(channel: Channel) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val service: PartyManagementServiceGrpc.PartyManagementServiceStub =
    PartyManagementServiceGrpc.stub(channel)

  def allocateParty(hint: String)(implicit ec: ExecutionContext): Future[Party] =
    service
      .allocateParty(new AllocatePartyRequest(partyIdHint = hint))
      .transformWith {
        case Success(response) =>
          Future.successful {
            val party = Party(response.partyDetails.get.party)
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
