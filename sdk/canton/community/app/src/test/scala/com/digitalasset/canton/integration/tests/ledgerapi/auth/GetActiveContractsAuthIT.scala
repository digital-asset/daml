// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.state_service.{
  GetActiveContractsRequest,
  GetActiveContractsResponse,
  StateServiceGrpc,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer

import scala.concurrent.Future

final class GetActiveContractsAuthIT extends SuperReaderServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "StateService#GetActiveContracts"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    new StreamConsumer[GetActiveContractsResponse](
      stub(StateServiceGrpc.stub(channel), context.token)
        .getActiveContracts(
          GetActiveContractsRequest(
            activeAtOffset = 0,
            eventFormat = context.eventFormat.orElse(Some(eventFormat(getMainActorId))),
          ),
          _,
        )
    ).first()
  }

}
