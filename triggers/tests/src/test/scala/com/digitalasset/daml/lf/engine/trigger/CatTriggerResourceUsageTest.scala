// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Command.Command
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, ParticipantId, singleParticipant}
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.{Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}

class CatTriggerResourceUsageTest
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues
    with CatTriggerResourceUsageTestGenerators {

  import TriggerRuleSimulationLib._

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

  "Trigger rule simulation" should {
    "with single/point-in-time trigger rule evaluation" should {
      "identify that the number of submissions is dependent on ACS size of the starting state" should {
        def stateSizeGen =
          ((0L to 10L) ++ (20L to 100L by 20L) ++ (200L to 400L by 100L)).iterator

        "for Cats:feedingTrigger initState lambda" ignore {
          for {
            client <- ledgerClient()
            party <- allocateParty(client)
            (_, simulator) = getSimulator(
              client,
              QualifiedName.assertFromString("Cats:feedingTrigger"),
              packageId,
              ApplicationId("submissions-and-acs"),
              compiledPackages,
              config.participants(ParticipantId).apiServer.timeProviderType,
              triggerRunnerConfiguration,
              party,
            )
            result <- forAll(monotonicACS(party, stateSizeGen)) { acs =>
              for {
                (submissions, metrics, state) <- simulator.initialStateLambda(acs)
              } yield {
                withClue((acs, state, submissions, metrics)) {
                  acs.size should be(2 * submissions.size)
                }
              }
            }
          } yield result
        }

        "for Cats:feedingTrigger updateState lambda" ignore {
          for {
            client <- ledgerClient()
            party <- allocateParty(client)
            (trigger, simulator) = getSimulator(
              client,
              QualifiedName.assertFromString("Cats:feedingTrigger"),
              packageId,
              ApplicationId("submissions-and-acs"),
              compiledPackages,
              config.participants(ParticipantId).apiServer.timeProviderType,
              triggerRunnerConfiguration,
              party,
            )
            converter = new Converter(compiledPackages, trigger)
            userState = SValue.SInt64(400L)
            msg = TriggerMsg.Heartbeat
            result <- forAll(monotonicACS(party, stateSizeGen)) { acs =>
              val startState = converter
                .fromTriggerUpdateState(
                  acs,
                  userState,
                  parties = TriggerParties(Party(party), Set.empty),
                  triggerConfig = triggerRunnerConfiguration,
                )

              for {
                (submissions, metrics, endState) <- simulator.updateStateLambda(startState, msg)
              } yield {
                withClue((startState, msg, endState, submissions, metrics)) {
                  acs.size should be(2 * submissions.size)
                }
              }
            }
          } yield result
        }
      }

      "identify that ACS size has linear growth" should {
        val growthRate = 100L
        val userStateGen = (0L to 1000L by growthRate).iterator

        "for Cats:overflowTrigger updateState lambda" ignore {
          for {
            client <- ledgerClient()
            party <- allocateParty(client)
            (trigger, simulator) = getSimulator(
              client,
              QualifiedName.assertFromString("Cats:overflowTrigger"),
              packageId,
              ApplicationId("acs-constant-growth"),
              compiledPackages,
              config.participants(ParticipantId).apiServer.timeProviderType,
              triggerRunnerConfiguration,
              party,
            )
            converter = new Converter(compiledPackages, trigger)
            acs = Seq.empty
            msg = TriggerMsg.Heartbeat
            result <- forAll(userStateGen) { userState =>
              val startState = converter
                .fromTriggerUpdateState(
                  acs,
                  SValue.SInt64(userState),
                  parties = TriggerParties(Party(party), Set.empty),
                  triggerConfig = triggerRunnerConfiguration,
                )

              for {
                (submissions, _, _) <- simulator.updateStateLambda(startState, msg)
              } yield {
                // TODO: validate that all submissions are unique (and not just the same ones!)
                submissions.size shouldBe growthRate
              }
            }
          } yield result
        }
      }

      "identify that rule evaluation time has a linear relationship with ACS size" should {
        def warmUpAcsSizeGen = 0L to 10L
        def acsSizeGen =
          (0L to 100L by 20L) ++ (150L to 400L by 50L) ++ (500L to 1000L by 100L) ++ (2000L to 5000L by 1000L)

        "for Cats:neverFeedingTrigger updateState lambda" in {
          for {
            client <- ledgerClient()
            party <- allocateParty(client)
            (trigger, simulator) = getSimulator(
              client,
              QualifiedName.assertFromString("Cats:neverFeedingTrigger"),
              packageId,
              ApplicationId("acs-and-rule-evaluation-time"),
              compiledPackages,
              config.participants(ParticipantId).apiServer.timeProviderType,
              triggerRunnerConfiguration,
              party,
            )
            converter = new Converter(compiledPackages, trigger)
            // For this test, we only care about the Daml trigger performing a single Cat/Food contract match
            numOfCats = 1L
            userState = SValue.SInt64(numOfCats)
            msg = TriggerMsg.Heartbeat
            // We first perform some warm up work
            _ <- Future.sequence {
              warmUpAcsSizeGen.map { acsSize =>
                val startState = converter
                  .fromTriggerUpdateState(
                    acsGen(party, acsSize),
                    userState,
                    parties = TriggerParties(Party(party), Set.empty),
                    triggerConfig = triggerRunnerConfiguration,
                  )

                for {
                  _ <- simulator.updateStateLambda(startState, msg)
                } yield ()
              }
            }
            // Now we measure our rule evaluation timings
            data <- Future.sequence {
              acsSizeGen.map { acsSize =>
                val startState = converter
                  .fromTriggerUpdateState(
                    acsGen(party, numOfCats, acsSize),
                    userState,
                    parties = TriggerParties(Party(party), Set.empty),
                    triggerConfig = triggerRunnerConfiguration,
                  )

                for {
                  (_, metrics, _) <- simulator.updateStateLambda(startState, msg)
                } yield {
                  // As Cats:neverFeedingTrigger always performs a worse case contract key search, complexity should be
                  // dominated by the time to search the underlying SMap (which is backed by a Scala TreeMap) - i.e. log(n)
                  // Ref: https://docs.scala-lang.org/overviews/collections-2.13/performance-characteristics.html
                  val complexity = if (acsSize == 0L) 0.0 else Math.pow(acsSize.toDouble, 2) * Math.log(acsSize.toDouble)

                  new Regression.Data(complexity, metrics.evaluation.ruleEvaluation)
                }
              }
            }
            model <- Regression.linear(data.filter { case Regression.Data(x, y) =>
              x.isFinite && y.isFinite
            })
          } yield {
            withClue((model, data)) {
              println(s"DEBUGGY: $model")
              // Want, at the very least, a P_75 model fit
              model.fitProbability should be >= 0.75
              // Expect complexity to be increasing in size
              model.gradient should be > 0.0
            }
          }
        }
      }
    }

    "with sequenced/chained/iterative trigger rule evaluation" should {
      // Daml query statements filter out contracts that have command submissions operating on them (c.f. pending
      // contracts being locally "locked"), and so repeated trigger rule evaluations can not produce the same submissions
      "duplicate command submissions are **not** generated" should {
        "using Cats:feedingTrigger updateState lambda" ignore {
          for {
            client <- ledgerClient()
            party <- allocateParty(client)
            (trigger, simulator) = getSimulator(
              client,
              QualifiedName.assertFromString("Cats:feedingTrigger"),
              packageId,
              ApplicationId("no-duplicate-command-submissions"),
              compiledPackages,
              config.participants(ParticipantId).apiServer.timeProviderType,
              triggerRunnerConfiguration,
              party,
            )
            converter = new Converter(compiledPackages, trigger)
            catPopulation = 10L
            acs = acsGen(party, catPopulation)
            userState = SValue.SInt64(catPopulation)
            startState = converter
              .fromTriggerUpdateState(
                acs,
                userState,
                parties = TriggerParties(Party(party), Set.empty),
                triggerConfig = triggerRunnerConfiguration,
              )
            result <- triggerIterator(simulator, startState)
              .take(3)
              .findDuplicateCommandRequests
              .runWith(Sink.seq)
          } yield {
            result should not be empty
            all(result) shouldBe empty
          }
        }
      }
    }
  }
}

trait CatTriggerResourceUsageTestGenerators extends CatGenerators {

  def acsGen(owner: String, size: Long): Seq[CreatedEvent] =
    acsGen(owner, size, size)

  def acsGen(owner: String, numOfCats: Long, amountOfFood: Long): Seq[CreatedEvent] =
    (0L to numOfCats).map(i => createCat(owner, i)) ++ (0L to amountOfFood).map(i =>
      createFood(owner, i)
    )

  def monotonicACS(owner: String, sizeGen: Iterator[Long]): Iterator[Seq[CreatedEvent]] =
    sizeGen.map(acsGen(owner, _))

  def triggerIterator(simulator: TriggerRuleSimulationLib, startState: SValue)(implicit
      ec: ExecutionContext
  ): Source[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue), NotUsed] = {
    Source
      .repeat(TriggerMsg.Heartbeat)
      .scanAsync[(Seq[SubmitRequest], Option[TriggerRuleMetrics.RuleMetrics], SValue)](
        (Seq.empty, None, startState)
      ) { case ((_, _, state), msg) =>
        for {
          (submissions, metrics, nextState) <- simulator.updateStateLambda(state, msg)
        } yield (submissions, Some(metrics), nextState)
      }
      .collect { case (submissions, Some(metrics), state) => (submissions, metrics, state) }
  }

  implicit class TriggerRuleSimulationHelpers(
      source: Source[(Seq[SubmitRequest], TriggerRuleMetrics.RuleMetrics, SValue), NotUsed]
  ) {
    def findDuplicateCommandRequests: Source[Set[Command], NotUsed] = {
      source
        .scan[(Set[Command], Set[Command])]((Set.empty, Set.empty)) {
          case ((observations, _), (submissions, _, _)) =>
            val newObservations = submissions.flatMap(_.getCommands.commands.map(_.command)).toSet
            val newDuplicates = newObservations.filter(observations.contains)

            (observations ++ newObservations, newDuplicates)
        }
        .map(_._2)
    }
  }
}
