// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixture
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  EventsObserver,
  FibonacciCommandGenerator,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.AppendedClues

class FibonacciCommandSubmitterITSpec extends BenchtoolSandboxFixture with AppendedClues {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "FibonacciCommandSubmitterIT" should {
    "populate create fibonacci contracts" onlyRunWithOrGreaterThan ProtocolVersion.dev in { env =>
      import env.*

      val config = WorkflowConfig.FibonacciSubmissionConfig(
        numberOfInstances = 10,
        uniqueParties = false,
        value = 7,
        waitForSubmission = true,
      )

      (for {
        (apiServices, names, submitter) <- benchtoolFixture()
        allocatedParties <- submitter.prepare(config)
        _ = allocatedParties.divulgees shouldBe empty
        generator = new FibonacciCommandGenerator(
          signatory = allocatedParties.signatory,
          config = config,
          names = names,
        )
        _ <- submitter.generateAndSubmit(
          generator = generator,
          config = config,
          baseActAs = List(allocatedParties.signatory) ++ allocatedParties.divulgees,
          maxInFlightCommands = 1,
          submissionBatchSize = 5,
        )
        eventsObserver = EventsObserver(expectedTemplateNames =
          Set(
            "InefficientFibonacci",
            "InefficientFibonacciResult",
          )
        )
        ledgerEnd <- apiServices.stateService.getLedgerEnd()
        _ <- apiServices.updateService.transactionsLedgerEffects(
          config = WorkflowConfig.StreamConfig.TransactionLedgerEffectsStreamConfig(
            name = "dummy-name",
            filters = List(
              WorkflowConfig.StreamConfig.PartyFilter(
                party = allocatedParties.signatory.getValue,
                templates = List.empty,
                interfaces = List.empty,
              )
            ),
            beginOffsetExclusive = 0L,
            endOffsetInclusive = Some(ledgerEnd),
            objectives = None,
            maxItemCount = None,
            timeoutO = None,
          ),
          observer = eventsObserver,
        )
        observerResult <- eventsObserver.result
      } yield {
        observerResult.numberOfCreatesPerTemplateName(
          "InefficientFibonacci"
        ) shouldBe config.numberOfInstances withClue "number of create events"
        succeed
      }).futureValue
    }
  }

}
