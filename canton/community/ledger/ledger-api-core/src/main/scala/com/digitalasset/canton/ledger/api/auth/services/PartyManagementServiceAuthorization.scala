// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
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
    authorizer.requireAdminClaims(service.getParticipantId)(request)

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      Lens.unit[GetPartiesRequest].identityProviderId,
      service.getParties,
    )(request)

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      Lens.unit[ListKnownPartiesRequest].identityProviderId,
      service.listKnownParties,
    )(
      request
    )

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      Lens.unit[AllocatePartyRequest].identityProviderId,
      service.allocateParty,
    )(
      request
    )

  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] = request.partyDetails match {
    case Some(partyDetails) =>
      authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
        Lens.unit[UpdatePartyDetailsRequest].partyDetails.identityProviderId,
        service.updatePartyDetails,
      )(
        request
      )
    case None =>
      authorizer.requireIdpAdminClaims(service.updatePartyDetails)(request)
  }

  override def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderRequest
  ): Future[UpdatePartyIdentityProviderResponse] = {
    authorizer.requireAdminClaims(
      call = service.updatePartyIdentityProviderId
    )(
      request
    )
  }

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
