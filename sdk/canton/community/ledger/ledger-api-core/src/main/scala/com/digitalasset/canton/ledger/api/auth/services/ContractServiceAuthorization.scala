// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.contract_service.*
import com.daml.ledger.api.v2.contract_service.ContractServiceGrpc.ContractService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class ContractServiceAuthorization(
    protected val service: ContractService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends ContractService
    with ProxyCloseable
    with GrpcApiService {

  override def getContract(request: GetContractRequest): Future[GetContractResponse] =
    authorizer.rpc(service.getContract)(
      ContractServiceAuthorization.requiredClaims(request)*
    )(request)

  override def bindService(): ServerServiceDefinition =
    ContractServiceGrpc.bindService(this, executionContext)
}

object ContractServiceAuthorization {
  def requiredClaims(request: GetContractRequest): List[RequiredClaim[GetContractRequest]] =
    request.queryingParties match {
      case empty if empty.isEmpty => List(RequiredClaim.ReadAsAnyParty[GetContractRequest]())
      case nonEmpty => RequiredClaims.readAsForAllParties[GetContractRequest](nonEmpty)
    }
}
