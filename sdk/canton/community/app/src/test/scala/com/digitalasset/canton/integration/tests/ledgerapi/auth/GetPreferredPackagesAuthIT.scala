// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  GetPreferredPackagesRequest,
  InteractiveSubmissionServiceGrpc,
  PackageVettingRequirement,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping

import scala.concurrent.Future

final class GetPreferredPackagesAuthIT extends PublicServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String =
    s"${InteractiveSubmissionService.getClass.getSimpleName}#GetPreferredPackages"

  private val someParty = "someRandomParty"
  override def prerequisiteParties: List[String] = List(someParty)
  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*

    for {
      _ <- stub(InteractiveSubmissionServiceGrpc.stub(channel), context.token)
        .getPreferredPackages(
          GetPreferredPackagesRequest(
            packageVettingRequirements = Seq(
              PackageVettingRequirement(
                parties = Seq(getPartyId(someParty)),
                packageName = Ping.PACKAGE_NAME,
              )
            ),
            synchronizerId = "",
            vettingValidAt = None,
          )
        )
    } yield ()
  }
}
