// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

private[daml] final class PartyManagementServiceAuthorization(
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
      request.identityProviderId,
      service.getParties,
    )(request)

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      request.identityProviderId,
      service.listKnownParties,
    )(
      request
    )

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
      request.identityProviderId,
      service.allocateParty,
    )(
      request
    )

  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] = request.partyDetails match {
    case Some(partyDetails) =>
      authorizer.requireIdpAdminClaimsAndMatchingRequestIdpId(
        partyDetails.identityProviderId,
        service.updatePartyDetails,
      )(
        request
      )
    case None =>
      authorizer.requireIdpAdminClaims(service.updatePartyDetails)(request)
  }

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()

  override def updatePartyIdentityProviderId(
      request: UpdatePartyIdentityProviderRequest
  ): Future[UpdatePartyIdentityProviderResponse] = throw new UnsupportedOperationException()
}
