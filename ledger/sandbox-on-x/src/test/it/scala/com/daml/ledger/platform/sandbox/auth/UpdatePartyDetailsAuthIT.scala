// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.party_management_service._
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID
import scala.concurrent.Future

final class UpdatePartyDetailsAuthIT extends AdminOrIDPAdminServiceCallAuthTests {

  override def serviceCallName: String = "PartyManagementService#GetParties"

  private def allocateParty(token: Option[String], partyId: String, identityProviderId: String) =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .allocateParty(
        AllocatePartyRequest(
          partyIdHint = s"some-party-$partyId",
          displayName = "display-name",
          identityProviderId = identityProviderId,
        )
      )

  private def updatePartyDetailsRequest(partyDetails: PartyDetails) =
    UpdatePartyDetailsRequest(
      partyDetails = Some(partyDetails),
      updateMask = Some(FieldMask(paths = scala.Seq("display_name"))),
    )

  private def updatePartyDetails(token: Option[String], partyDetails: PartyDetails) =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .updatePartyDetails(updatePartyDetailsRequest(partyDetails))

  override def serviceCall(context: ServiceCallContext): Future[Any] = {
    import context._
    val partyId = UUID.randomUUID().toString
    for {
      response <- allocateParty(token, partyId, identityProviderId)
      _ <- updatePartyDetails(token, response.partyDetails.get)
    } yield ()
  }

}
