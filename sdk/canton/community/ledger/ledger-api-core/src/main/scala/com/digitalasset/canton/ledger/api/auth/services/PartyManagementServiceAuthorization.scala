// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.PartyManagementServiceAuthorization.updatePartyDetailsClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class PartyManagementServiceAuthorization(
    protected val service: PartyManagementService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends PartyManagementService
    with ProxyCloseable
    with GrpcApiService {

  override def getParticipantId(
      request: GetParticipantIdRequest
  ): Future[GetParticipantIdResponse] =
    authorizer.rpc(service.getParticipantId)(RequiredClaim.Admin())(request)

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    authorizer.rpc(service.getParties)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        Lens.unit[GetPartiesRequest].identityProviderId
      )*
    )(request)

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] =
    authorizer.rpc(service.listKnownParties)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        Lens.unit[ListKnownPartiesRequest].identityProviderId
      )*
    )(request)

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    authorizer.rpc(service.allocateParty)(
      RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
        Lens.unit[AllocatePartyRequest].identityProviderId
      )*
    )(request)

  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] =
    authorizer.rpc(service.updatePartyDetails)(
      updatePartyDetailsClaims(request)*
    )(request)

  override def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderIdRequest
  ): Future[UpdatePartyIdentityProviderIdResponse] =
    authorizer.rpc(service.updatePartyIdentityProviderId)(RequiredClaim.Admin())(request)

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}

object PartyManagementServiceAuthorization {
  def updatePartyDetailsClaims(
      request: UpdatePartyDetailsRequest
  ): List[RequiredClaim[UpdatePartyDetailsRequest]] =
    request.partyDetails match {
      case Some(_) =>
        RequiredClaims.idpAdminClaimsAndMatchingRequestIdpId(
          Lens.unit[UpdatePartyDetailsRequest].partyDetails.identityProviderId
        )
      case None =>
        RequiredClaim.AdminOrIdpAdmin[UpdatePartyDetailsRequest]() :: Nil
    }
}
