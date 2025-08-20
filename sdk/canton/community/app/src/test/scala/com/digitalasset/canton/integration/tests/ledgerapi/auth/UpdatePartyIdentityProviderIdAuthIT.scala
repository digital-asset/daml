// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer

import java.util.UUID
import scala.concurrent.Future

final class UpdatePartyIdentityProviderIdAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PartyManagementService#UpdatePartyIdentityProviderId"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    val idpId = "idp-id-" + UUID.randomUUID().toString
    for {
      allocatePartyResp <- stub(
        PartyManagementServiceGrpc.stub(channel),
        canBeAnAdmin.token,
      )
        .allocateParty(
          AllocatePartyRequest(
            partyIdHint = "",
            localMetadata = None,
            identityProviderId = "",
            synchronizerId = "",
            userId = "",
          )
        )
      _ <- createConfig(canBeAnAdmin, idpId = Some(idpId))
      party = allocatePartyResp.partyDetails.value.party
      result <- stub(PartyManagementServiceGrpc.stub(channel), context.token)
        .updatePartyIdentityProviderId(
          UpdatePartyIdentityProviderIdRequest(
            party = party,
            sourceIdentityProviderId = "",
            targetIdentityProviderId = idpId,
          )
        )
      // cleanup the idp configuration we created in order to prevent exceeding the max number of possible idp configs
      _ <- parkParties(canBeAnAdmin, List(party), idpId)
      _ <- deleteConfig(canBeAnAdmin, identityProviderId = idpId)
    } yield result
  }

}
