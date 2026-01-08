// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.interactive.interactive_submission_service.PrepareSubmissionResponse
import com.digitalasset.canton.damltests.java.simpletemplate.SimpleTemplate
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.PackageNotVettedByRecipients
import com.digitalasset.canton.topology.ExternalParty

class NoSubmitterPackageIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasCycleUtils {

  private var aliceE: ExternalParty = _
  private var bobE: ExternalParty = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        ppn.dars.upload(CantonTestsPath, synchronizerId = daId)
        cpn.dars.upload(CantonTestsPath, synchronizerId = daId)

        bobE = ppn.parties.testing.external.enable("Bob")
        aliceE = cpn.parties.testing.external.enable("Alice")
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)

  "Interactive submission" should {

    // Prepare a submission on a participant that has the package
    def prepare()(implicit env: TestConsoleEnvironment): PrepareSubmissionResponse =
      cpn.ledger_api.javaapi.interactive_submission.prepare(
        Seq(aliceE.partyId),
        Seq(
          SimpleTemplate
            .create(aliceE.toProtoPrimitive, bobE.toProtoPrimitive)
            .commands()
            .loneElement
        ),
      )

    "fail if the executing participant does not have the package loaded" in {
      implicit env: TestConsoleEnvironment =>
        // Submit on one that does not have package
        assertThrowsAndLogsCommandFailures(
          epn.ledger_api.commands.external.submit_prepared(aliceE, prepare()),
          (e: LogEntry) => {
            e.shouldBeCantonErrorCode(InvalidPrescribedSynchronizerId)
            e.errorMessage should include regex raw"(?s)Participant PAR::participant2.*has not vetted"
          },
        )
    }

    "fail if the package is loaded but not vetted" in { implicit env: TestConsoleEnvironment =>
      // Load DAR without vetting
      epn.dars.upload(CantonTestsPath, vetAllPackages = false)

      assertThrowsAndLogsCommandFailures(
        epn.ledger_api.commands.external.submit_prepared(aliceE, prepare()),
        (e: LogEntry) => {
          e.shouldBeCantonErrorCode(PackageNotVettedByRecipients)
          e.errorMessage should include regex raw"(?s)Participant PAR::participant2.*has not vetted"
        },
      )

    }

    "pass if the package is vetted" in { implicit env: TestConsoleEnvironment =>
      // Load DAR with vetting
      epn.dars.upload(CantonTestsPath, vetAllPackages = true)
      epn.ledger_api.commands.external.submit_prepared(aliceE, prepare())
    }
  }

}
