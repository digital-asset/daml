// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.party_management_service._
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID
import scala.concurrent.Future

final class UpdatePartyDetailsAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "PartyManagementService#GetParties"

  private def allocateParty(token: Option[String], partyId: String) =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .allocateParty(
        AllocatePartyRequest(partyIdHint = s"some-party-$partyId", displayName = "display-name")
      )

  private def updatePartyDetailsRequest(partyDetails: PartyDetails) =
    UpdatePartyDetailsRequest(
      partyDetails = Some(partyDetails),
      updateMask = Some(FieldMask(paths = scala.Seq("display_name"))),
    )

  private def updatePartyDetails(token: Option[String], partyDetails: PartyDetails) =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .updatePartyDetails(updatePartyDetailsRequest(partyDetails))

  override def serviceCallWithToken(token: Option[String]): Future[Any] = {
    val partyId = UUID.randomUUID().toString
    for {
      response <- allocateParty(token, partyId)
      _ <- updatePartyDetails(token, response.partyDetails.get)
    } yield ()
  }

}
