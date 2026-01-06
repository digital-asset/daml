// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.fixture

import com.daml.grpc.AuthCallCredentials
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocatePartyRequest,
  PartyManagementServiceGrpc,
}

import scala.concurrent.{ExecutionContext, Future}

trait CreatesParties {
  self: CantonFixture =>

  private def createParty(token: Option[String])(
      partyId: String
  )(implicit executionContext: ExecutionContext): Future[String] = {
    val req = AllocatePartyRequest(
      partyIdHint = partyId,
      localMetadata = None,
      identityProviderId = "",
      synchronizerId = "",
      userId = "",
    )
    val stub = PartyManagementServiceGrpc.stub(channel)
    token
      .fold(stub)(AuthCallCredentials.authorizingStub(stub, _))
      .allocateParty(req)
      .map(_.getPartyDetails.party)
  }

  protected def createPrerequisiteParties(token: Option[String], prerequisiteParties: List[String])(
      implicit ec: ExecutionContext
  ): Unit =
    timeouts.default.await_("creating prerequisite parties")(
      Future.sequence(prerequisiteParties.map(createParty(token)))
    )

}
