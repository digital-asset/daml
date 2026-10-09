// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID
import scala.concurrent.Future

final class UpdatePartyDetailsAuthIT extends AdminOrIDPAdminServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PartyManagementService#UpdatePartyDetails"

  private def allocateParty(token: Option[String], partyId: String, identityProviderId: String) =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .allocateParty(
        AllocatePartyRequest(
          partyIdHint = s"some-party-$partyId",
          localMetadata = None,
          identityProviderId = identityProviderId,
          synchronizerId = "",
          userId = "",
        )
      )

  private def updatePartyDetailsRequest(partyDetails: PartyDetails) =
    UpdatePartyDetailsRequest(
      partyDetails = Some(partyDetails),
      updateMask = Some(FieldMask(paths = scala.Seq("is_local"))),
    )

  private def updatePartyDetails(token: Option[String], partyDetails: PartyDetails) =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .updatePartyDetails(updatePartyDetailsRequest(partyDetails))

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    import context.*
    val partyId = UUID.randomUUID().toString
    for {
      response <- allocateParty(token, partyId, identityProviderId)
      _ <- updatePartyDetails(
        token,
        response.partyDetails.getOrElse(
          throw new RuntimeException(s"party details for party $partyId were not found")
        ),
      )
    } yield ()
  }

}
