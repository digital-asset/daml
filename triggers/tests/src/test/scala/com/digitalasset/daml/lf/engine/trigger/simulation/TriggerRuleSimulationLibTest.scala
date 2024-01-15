// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package simulation

import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.trigger.Runner.{
  numberOfActiveContracts,
  numberOfInFlightCommands,
  numberOfInFlightCreateCommands,
  numberOfPendingContracts,
}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SValue
import org.scalacheck.Gen
import org.scalatest.{Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.UUID

class TriggerRuleSimulationLibTestV1 extends TriggerRuleSimulationLibTest(LanguageMajorVersion.V1)
class TriggerRuleSimulationLibTestV2 extends TriggerRuleSimulationLibTest(LanguageMajorVersion.V2)

class TriggerRuleSimulationLibTest(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with TryValues
    with TriggerRuleSimulationLibTestGenerators {

  import AbstractTriggerTest._
  import TriggerRuleSimulationLib._

  override protected def triggerRunnerConfiguration: TriggerRunnerConfig =
    super.triggerRunnerConfiguration.copy(hardLimit =
      super.triggerRunnerConfiguration.hardLimit
        .copy(allowTriggerTimeouts = true, allowInFlightCommandOverflows = true)
    )

  "Trigger rule simulation" should {
    "correctly log metrics for initState lambda" in {
      for {
        client <- defaultLedgerClient()
        party <- allocateParty(client)
        (trigger, simulator) = getSimulator(
          client,
          QualifiedName.assertFromString("Cats:feedingTrigger"),
          packageId,
          applicationId,
          compiledPackages,
          timeProviderType,
          triggerRunnerConfiguration,
          party,
        )
        result <- forAll(initState) { acs =>
          for {
            (submissions, metrics, state) <- simulator.initialStateLambda(acs)
          } yield {
            withClue((acs, state, submissions, metrics)) {
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
      } yield result
    }

    "correctly log metrics for updateState lambda" in {
      for {
        client <- defaultLedgerClient()
        party <- allocateParty(client)
        (trigger, simulator) = getSimulator(
          client,
          QualifiedName.assertFromString("Cats:feedingTrigger"),
          packageId,
          applicationId,
          compiledPackages,
          timeProviderType,
          triggerRunnerConfiguration,
          party,
        )
        converter = new Converter(compiledPackages, trigger)
        result <- forAll(updateState) { case (acs, userState, inFlightCmds, msg) =>
          val startState = converter
            .fromTriggerUpdateState(
              acs,
              userState,
              inFlightCmds,
              TriggerParties(party, Set.empty),
              triggerRunnerConfiguration,
            )

          for {
            (submissions, metrics, endState) <- simulator.updateStateLambda(startState, msg)
          } yield {
            withClue((startState, msg, endState, submissions, metrics)) {
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
                  numberOfCreateCommands(request) + numberOfCreateAndExerciseCommands(
                    request
                  ) + numberOfExerciseCommands(request) + numberOfExerciseByKeyCommands(request)
                )
                .sum
              msg match {
                case TriggerMsg.Completion(completion)
                    if completion.getStatus.code == 0 && completion.commandId.nonEmpty =>
                  // Completion success for a request this trigger (should have) produced
                  metrics.startState.acs.activeContracts should be(
                    metrics.endState.acs.activeContracts
                  )
                  metrics.endState.acs.pendingContracts should be(
                    metrics.startState.acs.pendingContracts + pendingContractSubmissions
                  )
                  metrics.endState.inFlight.commands should be(
                    metrics.startState.inFlight.commands + submissions.size
                  )

                case TriggerMsg.Completion(completion)
                    if completion.getStatus.code != 0 && completion.commandId.nonEmpty =>
                  // Completion failure for a request this trigger (should have) produced
                  val completionFailureSubmissions = numberOfInFlightCreateCommands(
                    converter.fromCommandId(completion.commandId),
                    endState,
                    trigger.level,
                    trigger.version,
                  )
                  val submissionDelta = if (inFlightCmds.contains(completion.commandId)) 1 else 0
                  metrics.startState.acs.activeContracts should be(
                    metrics.endState.acs.activeContracts
                  )
                  metrics.endState.acs.pendingContracts should be(
                    metrics.startState.acs.pendingContracts + pendingContractSubmissions - completionFailureSubmissions
                      .getOrElse(0)
                  )
                  metrics.endState.inFlight.commands should be(
                    metrics.startState.inFlight.commands + submissions.size - submissionDelta
                  )

                case TriggerMsg.Transaction(transaction) if transaction.commandId.nonEmpty =>
                  // Transaction events are related to a command this trigger (should have) produced
                  val transactionCreates = transaction.events.count(_.event.isCreated)
                  val transactionArchives = transaction.events.count(_.event.isArchived)
                  val submissionDelta = if (inFlightCmds.contains(transaction.commandId)) 1 else 0
                  metrics.endState.acs.activeContracts should be(
                    metrics.startState.acs.activeContracts + transactionCreates - transactionArchives
                  )
                  metrics.endState.acs.pendingContracts should be(
                    metrics.startState.acs.pendingContracts + pendingContractSubmissions // - (transactionCreates + transactionArchives)
                  )
                  metrics.endState.inFlight.commands should be(
                    metrics.startState.inFlight.commands + submissions.size - submissionDelta
                  )

                case TriggerMsg.Transaction(transaction) =>
                  // Transaction events are related to events this trigger subscribed to, but not to commands produced by this trigger
                  val transactionCreates = transaction.events.count(_.event.isCreated)
                  val transactionArchives = transaction.events.count(_.event.isArchived)
                  metrics.endState.acs.activeContracts should be(
                    metrics.startState.acs.activeContracts + transactionCreates - transactionArchives
                  )
                  metrics.endState.acs.pendingContracts should be(
                    metrics.startState.acs.pendingContracts + pendingContractSubmissions
                  )
                  metrics.endState.inFlight.commands should be(
                    metrics.startState.inFlight.commands + submissions.size
                  )

                case _ =>
                  metrics.startState.acs.activeContracts should be(
                    metrics.endState.acs.activeContracts
                  )
                  metrics.endState.acs.pendingContracts should be(
                    metrics.startState.acs.pendingContracts + pendingContractSubmissions
                  )
                  metrics.endState.inFlight.commands should be(
                    metrics.startState.inFlight.commands + submissions.size
                  )
              }
            }
          }
        }
      } yield result
    }
  }
}

trait TriggerRuleSimulationLibTestGenerators extends CatGenerators {

  import TriggerRuleSimulationLib.{CommandsInFlight, TriggerExperiment}

  private val maxNumOfCats = 10L

  private val userStateGen: Gen[SValue] = Gen.choose(0L, maxNumOfCats).map(SValue.SInt64)

  private val basicTesting = TriggerExperiment(
    acsGen = Gen.const(Seq.empty),
    userStateGen = userStateGen,
    inFlightCmdGen = Gen.const(Map.empty),
    msgGen = Gen.const(TriggerMsg.Heartbeat),
  )

  private val activeContractTesting = TriggerExperiment(
    acsGen = for {
      party <- partyGen
      numOfCats <- Gen.choose(0, maxNumOfCats)
      amountOfFood <- Gen.choose(0, maxNumOfCats)
      cats = (0L to numOfCats).map(createCat(party, _))
      food = (0L to amountOfFood).map(createFood(party, _))
    } yield cats ++ food,
    userStateGen = userStateGen,
    inFlightCmdGen = Gen.const(Map.empty),
    msgGen = Gen.const(TriggerMsg.Heartbeat),
  )

  private val transactionTesting = TriggerExperiment(
    acsGen = Gen.const(Seq.empty),
    userStateGen = userStateGen,
    inFlightCmdGen = Gen.const(Map.empty),
    msgGen = for {
      party <- partyGen
      numOfCats <- Gen.choose(0, maxNumOfCats)
      amountOfFood <- Gen.choose(0, maxNumOfCats)
      transaction <- transactionGen(party, numOfCats = numOfCats, amountOfFood = amountOfFood)
    } yield TriggerMsg.Transaction(transaction),
  )

  private val transactionInFlightTesting = {
    val cmdId = UUID.randomUUID().toString
    val inFlightCmdGen = for {
      cmds <- Gen.listOfN(maxNumOfCats.toInt, commandGen(maxNumOfCats))
    } yield (cmdId, cmds)

    TriggerExperiment(
      acsGen = Gen.const(Seq.empty),
      userStateGen = userStateGen,
      inFlightCmdGen = Gen.mapOfN(maxNumOfCats.toInt, inFlightCmdGen),
      msgGen = for {
        party <- partyGen
        numOfCats <- Gen.choose(0, maxNumOfCats)
        amountOfFood <- Gen.choose(0, maxNumOfCats)
        transaction <- transactionGen(
          party,
          numOfCats = numOfCats,
          amountOfFood = amountOfFood,
          knownCmdId = cmdId,
        )
      } yield TriggerMsg.Transaction(transaction),
    )
  }

  private val completionTesting = {
    val cmdId = UUID.randomUUID().toString

    TriggerExperiment(
      acsGen = Gen.const(Seq.empty),
      userStateGen = userStateGen,
      inFlightCmdGen = Gen.const(Map(cmdId -> Seq.empty)),
      msgGen = for {
        party <- partyGen
        completion <- Gen.frequency(
          9 -> Gen.frequency(
            1 -> successfulCompletionGen(party),
            9 -> successfulCompletionGen(party, cmdId),
          ),
          1 -> Gen.frequency(
            1 -> failingCompletionGen(party),
            9 -> failingCompletionGen(party, cmdId),
          ),
        )
      } yield TriggerMsg.Completion(completion),
    )
  }

  private val completionInFlightTesting = {
    val cmdId = UUID.randomUUID().toString
    val inFlightCmdGen = for {
      cmds <- Gen.listOfN(maxNumOfCats.toInt, commandGen(maxNumOfCats))
    } yield (cmdId, cmds)

    TriggerExperiment(
      acsGen = Gen.const(Seq.empty),
      userStateGen = userStateGen,
      inFlightCmdGen = Gen.mapOfN(maxNumOfCats.toInt, inFlightCmdGen),
      msgGen = for {
        party <- partyGen
        completion <- Gen.frequency(
          9 -> Gen.frequency(
            1 -> successfulCompletionGen(party),
            9 -> successfulCompletionGen(party, cmdId),
          ),
          1 -> Gen.frequency(
            1 -> failingCompletionGen(party),
            9 -> failingCompletionGen(party, cmdId),
          ),
        )
      } yield TriggerMsg.Completion(completion),
    )
  }

  val initState: Gen[Seq[CreatedEvent]] = Gen.frequency(
    1 -> basicTesting.initState,
    1 -> activeContractTesting.initState,
    1 -> transactionTesting.initState,
    1 -> transactionInFlightTesting.initState,
    1 -> completionTesting.initState,
    1 -> completionInFlightTesting.initState,
  )

  val updateState: Gen[(Seq[CreatedEvent], SValue, CommandsInFlight, TriggerMsg)] = Gen.frequency(
    1 -> basicTesting.updateState,
    1 -> activeContractTesting.updateState,
    1 -> transactionTesting.updateState,
    1 -> transactionInFlightTesting.updateState,
    1 -> completionTesting.updateState,
    1 -> completionInFlightTesting.updateState,
  )
}
