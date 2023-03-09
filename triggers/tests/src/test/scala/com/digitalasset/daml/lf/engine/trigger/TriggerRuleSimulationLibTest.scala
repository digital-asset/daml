// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, ParticipantId, singleParticipant}
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.trigger.Runner.{
  numberOfActiveContracts,
  numberOfInFlightCommands,
  numberOfInFlightCreateCommands,
  numberOfPendingContracts,
}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue
import com.daml.platform.services.time.TimeProviderType
import org.scalacheck.Gen
import org.scalatest.{Assertion, Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class TriggerRuleSimulationLibTest
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues {

  import TriggerRuleSimulationLib._
  import TriggerRuleSimulationLibTest._

  override protected def config: Config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.Static
      )
    )
  )

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, allowInFlightCommandOverflows = true)
    )

  def forAll[T](gen: Gen[T], sampleSize: Int = 100, parallelism: Int = 1)(
      test: T => Future[Assertion]
  ): Future[Assertion] = {
    // TODO: ????: use results (e.g. submissions and ACS/inflight changes) of simulator runs to infer additional events
    Source(0 to sampleSize)
      .map(_ => gen.sample)
      .collect { case Some(data) => data }
      .mapAsync(parallelism) { data =>
        test(data)
      }
      .takeWhile(_ == succeed, inclusive = true)
      .runWith(Sink.last)
  }

  "Trigger rule simulation" should {
    "correctly log metrics for initState lambda" in {
      forAll(initState) { acs =>
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          (trigger, simulator) = getSimulator(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
            packageId,
            applicationId,
            compiledPackages,
            config.participants(ParticipantId).apiServer.timeProviderType,
            triggerRunnerConfiguration,
            party,
          )
          (submissions, metrics, state) <- simulator.initialStateLambda(acs)
        } yield {
          metrics.evaluation.submissions should be(submissions.size)
          metrics.submission.creates should be(submissions.map(numberOfCreateCommands).sum)
          metrics.submission.createAndExercises should be(
            submissions.map(numberOfCreateAndExerciseCommands).sum
          )
          metrics.submission.exercises should be(submissions.map(numberOfExerciseCommands).sum)
          metrics.submission.exerciseByKeys should be(
            submissions.map(numberOfExerciseByKeyCommands).sum
          )
          metrics.evaluation.steps should be(
            metrics.evaluation.getTimes + metrics.evaluation.submissions + 1
          )
          metrics.startState.acs.activeContracts should be(acs.size)
          metrics.startState.acs.pendingContracts should be(0)
          metrics.startState.inFlight.commands should be(0)
          Some(metrics.endState.acs.activeContracts) should be(
            numberOfActiveContracts(state, trigger.level, trigger.version)
          )
          Some(metrics.endState.acs.pendingContracts) should be(
            numberOfPendingContracts(state, trigger.level, trigger.version)
          )
          Some(metrics.endState.inFlight.commands) should be(
            numberOfInFlightCommands(state, trigger.level, trigger.version)
          )
        }
      }
    }

    "correctly log metrics for updateState lambda" in {
      forAll(updateState) { case (acs, userState, msg) =>
        for {
          client <- ledgerClient()
          party <- allocateParty(client)
          (trigger, simulator) = getSimulator(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
            packageId,
            applicationId,
            compiledPackages,
            config.participants(ParticipantId).apiServer.timeProviderType,
            triggerRunnerConfiguration,
            party,
          )
          converter = new Converter(compiledPackages, trigger)
          startState = converter
            .fromTriggerUpdateState(
              acs,
              userState,
              Map.empty,
              TriggerParties(Party(party), Set.empty),
              triggerRunnerConfiguration,
            )
          (submissions, metrics, endState) <- simulator.updateStateLambda(startState, msg)
        } yield {
          metrics.evaluation.submissions should be(submissions.size)
          metrics.submission.creates should be(submissions.map(numberOfCreateCommands).sum)
          metrics.submission.createAndExercises should be(
            submissions.map(numberOfCreateAndExerciseCommands).sum
          )
          metrics.submission.exercises should be(submissions.map(numberOfExerciseCommands).sum)
          metrics.submission.exerciseByKeys should be(
            submissions.map(numberOfExerciseByKeyCommands).sum
          )
          metrics.evaluation.steps should be(
            metrics.evaluation.getTimes + metrics.evaluation.submissions + 1
          )
          Some(metrics.startState.acs.activeContracts) should be(
            numberOfActiveContracts(startState, trigger.level, trigger.version)
          )
          Some(metrics.startState.acs.pendingContracts) should be(
            numberOfPendingContracts(startState, trigger.level, trigger.version)
          )
          Some(metrics.startState.inFlight.commands) should be(
            numberOfInFlightCommands(startState, trigger.level, trigger.version)
          )
          Some(metrics.endState.acs.activeContracts) should be(
            numberOfActiveContracts(endState, trigger.level, trigger.version)
          )
          Some(metrics.endState.acs.pendingContracts) should be(
            numberOfPendingContracts(endState, trigger.level, trigger.version)
          )
          Some(metrics.endState.inFlight.commands) should be(
            numberOfInFlightCommands(endState, trigger.level, trigger.version)
          )

          val pendingContractSubmissions = submissions
            .map(request =>
              numberOfCreateCommands(request) + numberOfCreateAndExerciseCommands(request)
            )
            .sum
          msg match {
            case TriggerMsg.Completion(completion)
                if completion.getStatus.code == 0 && completion.commandId.nonEmpty =>
              // Completion success for a request this trigger produced
              metrics.startState.acs.activeContracts should be(metrics.endState.acs.activeContracts)
              metrics.startState.acs.pendingContracts should be(
                metrics.endState.acs.pendingContracts + pendingContractSubmissions
              )
              metrics.startState.inFlight.commands should be(
                metrics.endState.inFlight.commands + submissions.size
              )

            case TriggerMsg.Completion(completion)
                if completion.getStatus.code != 0 && completion.commandId.nonEmpty =>
              // Completion failure for a request this trigger produced
              val completionFailureSubmissions = numberOfInFlightCreateCommands(
                completion.commandId,
                endState,
                trigger.level,
                trigger.version,
              )
              metrics.startState.acs.activeContracts should be(metrics.endState.acs.activeContracts)
              metrics.startState.acs.pendingContracts should be(
                metrics.endState.acs.pendingContracts + pendingContractSubmissions - completionFailureSubmissions
                  .getOrElse(0)
              )
              metrics.startState.inFlight.commands should be(
                metrics.endState.inFlight.commands + submissions.size - 1
              )

            case TriggerMsg.Transaction(transaction) if transaction.commandId.nonEmpty =>
              // Transaction events are related to a command this trigger produced
              val transactionCreates = transaction.events.count(_.event.isCreated)
              val transactionArchives = transaction.events.count(_.event.isArchived)
              metrics.endState.acs.activeContracts should be(
                metrics.startState.acs.activeContracts + transactionCreates - transactionArchives
              )
              metrics.startState.acs.pendingContracts should be(
                metrics.endState.acs.pendingContracts + pendingContractSubmissions - (transactionCreates + transactionArchives)
              )
              metrics.startState.inFlight.commands should be(
                metrics.endState.inFlight.commands + submissions.size - 1
              )

            case TriggerMsg.Transaction(transaction) =>
              // Transaction events are related to events this trigger subscribed to, but not to commands produced by this trigger
              val transactionCreates = transaction.events.count(_.event.isCreated)
              val transactionArchives = transaction.events.count(_.event.isArchived)
              metrics.endState.acs.activeContracts should be(
                metrics.startState.acs.activeContracts + transactionCreates - transactionArchives
              )
              metrics.startState.acs.pendingContracts should be(
                metrics.endState.acs.pendingContracts + pendingContractSubmissions
              )
              metrics.startState.inFlight.commands should be(
                metrics.endState.inFlight.commands + submissions.size
              )

            case _ =>
              metrics.startState.acs.activeContracts should be(metrics.endState.acs.activeContracts)
              metrics.startState.acs.pendingContracts should be(
                metrics.endState.acs.pendingContracts + pendingContractSubmissions
              )
              metrics.startState.inFlight.commands should be(
                metrics.endState.inFlight.commands + submissions.size
              )
          }
        }
      }
    }
  }
}

object TriggerRuleSimulationLibTest {
  // TODO: ????: extract the following 3 generators from user specified Daml code
  private val acsGen: Gen[Seq[CreatedEvent]] = Gen.const(Seq.empty)

  private val userStateGen: Gen[SValue] = Gen.choose(1L, 10L).map(SValue.SInt64)

  private val msgGen: Gen[TriggerMsg] = Gen.const(TriggerMsg.Heartbeat)

  // TODO: ????: use results (e.g. submissions and ACS/inflight changes) of simulator runs to infer initialization events
  val initState: Gen[Seq[CreatedEvent]] = acsGen

  // TODO: ????: use results (e.g. submissions and ACS/inflight changes) of simulator runs to infer update events
  val updateState: Gen[(Seq[CreatedEvent], SValue, TriggerMsg)] =
    for {
      acs <- acsGen
      userState <- userStateGen
      msg <- msgGen
    } yield (acs, userState, msg)
}
