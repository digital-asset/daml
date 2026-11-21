// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.interactive.interactive_submission_service.InteractiveSubmissionServiceGrpc.InteractiveSubmissionService
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  GetPreferredPackageVersionRequest,
  InteractiveSubmissionServiceGrpc,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal.ping.Ping

import scala.concurrent.Future

final class GetPreferredPackageVersionAuthIT extends PublicServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String =
    s"${InteractiveSubmissionService.getClass.getSimpleName}#GetPreferredPackageVersion"

  private val someParty = "someRandomParty"
  override def prerequisiteParties: List[String] = List(someParty)
  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*

    for {
      _ <- stub(InteractiveSubmissionServiceGrpc.stub(channel), context.token)
        .getPreferredPackageVersion(
          GetPreferredPackageVersionRequest(
            parties = Seq(getPartyId(someParty)),
            packageName = Ping.PACKAGE_NAME,
            synchronizerId = "",
            vettingValidAt = None,
          )
        )
    } yield ()
  }
}
