// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.authorization.services

import com.daml.ledger.participant.state.v1.AuthService
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.admin.party_management_service.{
  AllocatePartyRequest,
  AllocatePartyResponse,
  GetParticipantIdRequest,
  GetParticipantIdResponse,
  ListKnownPartiesRequest,
  ListKnownPartiesResponse,
  PartyManagementServiceGrpc
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import com.digitalasset.platform.server.api.authorization.ApiServiceAuthorization
import io.grpc.ServerServiceDefinition
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class PartyManagementServiceAuthorization(
    protected val service: PartyManagementService with AutoCloseable,
    protected val authService: AuthService)
    extends PartyManagementService
    with ProxyCloseable
    with GrpcApiService {

  protected val logger: Logger = LoggerFactory.getLogger(PartyManagementService.getClass)

  override def getParticipantId(
      request: GetParticipantIdRequest): Future[GetParticipantIdResponse] =
    ApiServiceAuthorization
      .requireAdminClaims()
      .fold(Future.failed(_), _ => service.getParticipantId(request))

  override def allocateParty(request: AllocatePartyRequest): Future[AllocatePartyResponse] =
    ApiServiceAuthorization
      .requireAdminClaims()
      .fold(Future.failed(_), _ => service.allocateParty(request))

  override def listKnownParties(
      request: ListKnownPartiesRequest): Future[ListKnownPartiesResponse] =
    ApiServiceAuthorization
      .requireAdminClaims()
      .fold(Future.failed(_), _ => service.listKnownParties(request))

  override def bindService(): ServerServiceDefinition =
    PartyManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
