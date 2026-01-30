// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.contract_service.{ContractServiceGrpc, GetContractRequest}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import io.grpc.Status
import org.scalatest.Assertion

import scala.concurrent.{ExecutionContext, Future}

final class GetContractAuthIT extends SuperReaderServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "ContractService#GetContract"

  override def successfulBehavior(f: Future[Any])(implicit ec: ExecutionContext): Assertion =
    expectFailure(f, Status.Code.INVALID_ARGUMENT)

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(ContractServiceGrpc.stub(channel), context.token)
      .getContract(
        GetContractRequest(
          contractId = "invalid contract id",
          queryingParties = {
            val format = context.eventFormat.getOrElse(eventFormat(getMainActorId))
            if (format.filtersForAnyParty.nonEmpty) Nil // any party case
            else format.filtersByParty.keySet.toSeq
          },
        )
      )

}
