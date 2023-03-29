// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, ParticipantId, singleParticipant}
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.{Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class CatTriggerResourceUsageTest
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues
    with CatTriggerResourceUsageTestGenerators {

  import TriggerRuleSimulationLib._

  // Used to control degree of parallelism in mapAsync streaming operations
  private[this] val parallelism = 8

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

  override implicit lazy val materializer: Materializer = Materializer(ActorSystem("Simulation"))

  override implicit val executionContext: ExecutionContext = materializer.executionContext

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
            result <- forAll(monotonicACS(party, stateSizeGen), parallelism = parallelism) { acs =>
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
            result <- forAll(monotonicACS(party, stateSizeGen), parallelism = parallelism) { acs =>
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
            result <- forAll(userStateGen, parallelism = parallelism) { userState =>
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
        val maxAcsSize = 5000L
        def acsSizeGen = 0L to maxAcsSize
        val runtime = Runtime.getRuntime
        val memBean = ManagementFactory.getMemoryMXBean
        val gcBeans = ManagementFactory.getGarbageCollectorMXBeans

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
            data <- Future.sequence {
              acsSizeGen.map { acsSize =>
                val startState = converter
                  .fromTriggerUpdateState(
                    acsGen(party, acsSize, acsSize),
                    userState,
                    parties = TriggerParties(Party(party), Set.empty),
                    triggerConfig = triggerRunnerConfiguration,
                  )

                for {
                  (submissions, metrics, _) <- simulator.updateStateLambda(startState, msg)
                } yield {
                  // As Cats:neverFeedingTrigger always performs a worse case contract key search, time to search the
                  // underlying SMap (which is backed by a Scala TreeMap) should be log(n)
                  // In addition, we also need to query the ACS (for Cat contracts) which should add an additional n
                  // Ref: https://docs.scala-lang.org/overviews/collections-2.13/performance-characteristics.html
//                  val complexity =
//                    if (acsSize == 0L) 0.0
//                    else acsSize.toDouble + Math.log(acsSize.toDouble)
                  val percentageMemUsed =
                    (runtime.totalMemory() - runtime.freeMemory()) / runtime.maxMemory().toDouble
                  val percentageHeapUsed =
                    memBean.getHeapMemoryUsage.getUsed / memBean.getHeapMemoryUsage.getMax.toDouble
                  val gcTime = gcBeans.asScala.map(_.getCollectionTime).sum
                  val gcCount = gcBeans.asScala.map(_.getCollectionCount).sum

                  (
                    acsSize.toDouble,
                    submissions.size,
                    metrics.evaluation.ruleEvaluation,
                    metrics.evaluation.stepIteratorMean,
                    metrics.endState.acs.activeContracts - metrics.startState.acs.activeContracts,
                    metrics.evaluation.steps,
                    percentageMemUsed,
                    percentageHeapUsed,
                    gcTime,
                    gcCount,
                  )
                }
              }
            }
          } yield {
//            val xs = DenseMatrix(data.map(d => Array(d._2, d._3.toNanos.toDouble)): _*)
//            val y = DenseVector(data.map(_._1).toArray)
//            val model = leastSquares(xs, y)
//
//            withClue((model.coefficients, model.rSquared)) {
//              // Expect a gradient of 0 - hence only an intercept coefficient should be calculated
//              model.coefficients.length shouldBe 1
//
//              val c = model.coefficients(0)
//              val errors = data.map(d => Math.pow(c - d._1, 2))
//              val fittedValues = errors.filter(_ <= 9)
//
//              // Want 75% of data to fit model correctly
//              (fittedValues.length / data.length.toDouble) should be >= 0.75
            val csvHeader =
              "acs,submissions,rule-evaluation,step-evaluation,acs-change,steps,used-mem-percent,used-heap-percent,gc-time,gc-count"
            val csvData = data.map(d =>
              s"${d._1},${d._2},${d._3.toNanos.toDouble},${d._4.toNanos.toDouble},${d._5},${d._6},${d._7},${d._8},${d._9},${d._10}"
            )

            Files.write(
              Paths.get("/tmp/graph-data.csv"),
              (csvHeader +: csvData).mkString("\n").getBytes(StandardCharsets.UTF_8),
            )

            fail("DEBUGGY")
//            }
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
