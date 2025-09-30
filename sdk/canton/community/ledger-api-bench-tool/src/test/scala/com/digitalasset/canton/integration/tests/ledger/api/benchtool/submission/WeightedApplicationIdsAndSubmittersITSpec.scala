// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledger.api.benchtool.submission

import com.daml.ledger.javaapi.data.Party
import com.daml.scalautil.Statement.discard
import com.daml.timer.Delayed
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.ledger.api.benchtool.BenchtoolSandboxFixture
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig
import com.digitalasset.canton.ledger.api.benchtool.config.WorkflowConfig.FooSubmissionConfig.UserId
import com.digitalasset.canton.ledger.api.benchtool.services.LedgerApiServices
import com.digitalasset.canton.ledger.api.benchtool.submission.{
  CompletionsObserver,
  ObservedCompletions,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.{AppendedClues, Checkpoints, OptionValues}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

class WeightedUserIdsAndSubmittersITSpec
    extends BenchtoolSandboxFixture
    with AppendedClues
    with OptionValues
    with Checkpoints {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val timeout: FiniteDuration = 2.minutes

  "benchtool" should {
    "populate participant with contracts using specified user-ids and submitters" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      env =>
        import env.*

        val submissionConfig = WorkflowConfig.FooSubmissionConfig(
          numberOfInstances = 100,
          numberOfObservers = 1,
          numberOfExtraSubmitters = 3,
          uniqueParties = false,
          instanceDistribution = List(
            WorkflowConfig.FooSubmissionConfig.ContractDescription(
              template = "Foo1",
              weight = 1,
              payloadSizeBytes = 0,
            )
          ),
          nonConsumingExercises = None,
          consumingExercises = None,
          userIds = List(
            UserId(
              userId = "App-1",
              weight = 90,
            ),
            UserId(
              userId = "App-2",
              weight = 10,
            ),
          ),
        )
        (for {
          (apiServices, allocatedParties, fooSubmission) <- benchtoolFooSubmissionFixture(
            submissionConfig
          )
          _ <- fooSubmission.performSubmission(submissionConfig = submissionConfig)
          completionsApp1 <- observeCompletions(
            parties = List(allocatedParties.signatory),
            apiServices = apiServices,
            userId = "App-1",
          )
          completionsApp2 <- observeCompletions(
            parties = List(allocatedParties.signatory),
            apiServices = apiServices,
            userId = "App-2",
          )
          completionsApp1Submitter0 <- observeCompletions(
            parties = List(allocatedParties.extraSubmitters.head),
            apiServices = apiServices,
            userId = "App-1",
          )
          completionsApp1Submitter1 <- observeCompletions(
            parties = List(allocatedParties.extraSubmitters(1)),
            apiServices = apiServices,
            userId = "App-1",
          )
        } yield {
          val cp = new Checkpoint
          // App only filters
          cp(discard(completionsApp1.completions.size shouldBe 91))
          cp(discard(completionsApp2.completions.size shouldBe 9))
          // App and party filters
          cp(
            discard(
              completionsApp1Submitter0.completions.size shouldBe completionsApp1.completions.size
            )
          )
          cp(discard(completionsApp1Submitter0.completions.size shouldBe 91))
          cp(discard(completionsApp1Submitter1.completions.size shouldBe 9))
          cp.reportAll()
          succeed
        }).futureValue(timeout = PatienceConfiguration.Timeout(timeout))
    }
  }

  private def observeCompletions(
      parties: List[Party],
      userId: String,
      apiServices: LedgerApiServices,
  )(implicit ec: ExecutionContext): Future[ObservedCompletions] = {
    val observer = CompletionsObserver()
    Delayed.by(t = Duration(5, TimeUnit.SECONDS))(observer.cancel())
    apiServices.commandCompletionService.completions(
      config = WorkflowConfig.StreamConfig.CompletionsStreamConfig(
        name = "dummy-name",
        parties = parties.map(_.getValue),
        userId = userId,
        beginOffsetExclusive = None,
        objectives = None,
        timeoutO = None,
        maxItemCount = None,
      ),
      observer = observer,
    )
  }

}
