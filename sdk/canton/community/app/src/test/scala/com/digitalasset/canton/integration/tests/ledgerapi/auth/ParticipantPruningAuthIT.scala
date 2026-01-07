// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.participant_pruning_service.{
  ParticipantPruningServiceGrpc,
  PruneRequest,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

final class ParticipantPruningAuthIT extends AdminServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "ParticipantPruningService#Prune"

  override protected def additionalEnvironmentSetup(
      testConsoleEnvironment: TestConsoleEnvironment
  ): Unit = {
    import testConsoleEnvironment.*
    // ensure pruning call can succeed eventually, by bumping time with ledger-traffic for make ACS commitment reconciliation bounds happy
    eventually() {
      participant1.health.ping(participant1)
      environment.simClock.value.advanceTo(
        environment.simClock.value.now.add(FiniteDuration(2, "seconds"))
      )
      expectSuccess(serviceCall(canReadAsAdminExpiresInAnHour)(testConsoleEnvironment))
    }
    // ensure subsequent pruning call succeeds
    expectSuccess(serviceCall(canReadAsAdminExpiresInAnHour)(testConsoleEnvironment))
  }

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(ParticipantPruningServiceGrpc.stub(channel), context.token)
      .prune(
        PruneRequest(
          pruneUpTo = 1L,
          submissionId = "",
          pruneAllDivulgedContracts = false,
        )
      )

}
