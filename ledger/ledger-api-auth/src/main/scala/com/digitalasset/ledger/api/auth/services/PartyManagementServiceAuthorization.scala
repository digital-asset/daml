// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

final class PartyManagementServiceAuthorization(
    protected val service: PartyManagementService with AutoCloseable,
    private val authorizer: Authorizer)
    extends PartyManagementService
    with ProxyCloseable
    with GrpcApiService {

  override def getParticipantId(
      request: GetParticipantIdRequest): Future[GetParticipantIdResponse] =
    authorizer.requireAdminClaims(service.getParticipantId)(request)

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    authorizer.requireAdminClaims(service.allocateParty)(request)

  override def listKnownParties(
      request: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    authorizer.requireAdminClaims(service.listKnownParties)(request)

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
