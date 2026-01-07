// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.state_service.{GetConnectedSynchronizersRequest, StateServiceGrpc}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetConnectedSynchronizersAuthIT
    extends AdminOrIdpAdminOrOperateAsPartyServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "StateService#GetConnectedSynchronizers"

  private def request(party: String, identityProviderId: String)(implicit
      env: TestConsoleEnvironment
  ) =
    new GetConnectedSynchronizersRequest(
      party = party,
      participantId = env.participant1.id.toProtoPrimitive.replace("PAR::", ""),
      identityProviderId = identityProviderId,
    )

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(StateServiceGrpc.stub(channel), context.token)
      .getConnectedSynchronizers(request(getMainActorId, context.identityProviderId))

}
