// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.daml.ledger.api.v1.admin.party_management_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
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
    authorizer.requireAdminOrIDPAdminClaims(service.getParticipantId)(request)

  override def getParties(request: GetPartiesRequest): Future[GetPartiesResponse] =
    authorizer.requireIDPContext(
      request.identityProviderId,
      authorizer.requireAdminOrIDPAdminClaims(service.getParties),
    )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
      request
    )

  override def listKnownParties(
      request: ListKnownPartiesRequest
  ): Future[ListKnownPartiesResponse] =
    authorizer.requireIDPContext(
      request.identityProviderId,
      authorizer.requireAdminOrIDPAdminClaims(service.listKnownParties),
    )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
      request
    )

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    authorizer.requireIDPContext(
      request.identityProviderId,
      authorizer.requireAdminOrIDPAdminClaims(service.allocateParty),
    )((identityProviderId, request) => request.copy(identityProviderId = identityProviderId))(
      request
    )

  override def updatePartyDetails(
      request: UpdatePartyDetailsRequest
  ): Future[UpdatePartyDetailsResponse] =
    authorizer.requireIDPContext(
      request.partyDetails.map(_.identityProviderId).getOrElse(""),
      authorizer.requireAdminOrIDPAdminClaims(service.updatePartyDetails),
    )((identityProviderId, request) =>
      request.copy(partyDetails =
        request.partyDetails.map(_.copy(identityProviderId = identityProviderId))
      )
    )(
      request
    )

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
