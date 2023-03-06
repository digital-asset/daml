// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.refinements.ApiTypes.{ApplicationId, Party}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, ParticipantId, singleParticipant}
import com.daml.lf.data.Ref.{Identifier, QualifiedName}
import com.daml.lf.engine.trigger.Runner.{
  numberOfActiveContracts,
  numberOfInFlightCommands,
  numberOfPendingContracts,
}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue
import com.daml.platform.services.time.TimeProviderType
import com.daml.script.converter.Converter.Implicits._
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Inside, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class TriggerRuleSimulationLibTest
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with Inside
    with SuiteResourceManagementAroundAll
    with TryValues
    with ScalaCheckPropertyChecks
    with ScalaFutures {

  import TriggerRuleSimulationLibTest._

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  implicit override val patienceConfig: PatienceConfig =
    super.patienceConfig.copy(timeout = scaled(Span(30, Seconds)))

  override protected def config: Config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        timeProviderType = TimeProviderType.Static
      )
    )
  )

  def getSimulator(
      client: LedgerClient,
      name: QualifiedName,
      actAs: String,
      readAs: Set[String] = Set.empty,
  ): (TriggerDefinition, TriggerRuleSimulationLib) = {
    val triggerId = Identifier(packageId, name)

    Trigger.newTriggerLogContext(
      triggerId,
      Party(actAs),
      Party.subst(readAs),
      "trigger-simulation",
      ApplicationId("trigger-simulation-app"),
    ) { implicit triggerContext: TriggerLogContext =>
      val trigger = Trigger.fromIdentifier(compiledPackages, triggerId).toOption.get
      val runner = Runner(
        compiledPackages,
        trigger,
        triggerRunnerConfiguration,
        client,
        config.participants(ParticipantId).apiServer.timeProviderType,
        applicationId,
        TriggerParties(
          actAs = Party(actAs),
          readAs = Party.subst(readAs),
        ),
      )

      (
        trigger.defn,
        new TriggerRuleSimulationLib(
          triggerRunnerConfiguration,
          trigger.defn.level,
          trigger.defn.version,
          runner,
        ),
      )
    }
  }

  "Trigger rule simulation" should {
    "correctly log metrics for initState lambda" in {
      forAll(initState) { acs =>
        val result = for {
          client <- ledgerClient()
          party <- allocateParty(client)
          (trigger, simulator) = getSimulator(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
            party,
          )
          (submissions, metrics, state) <- simulator.initialStateLambda(acs)
        } yield (trigger, submissions, metrics, state)

        whenReady(result) { case (trigger, submissions, metrics, state) =>
          metrics.evaluation.submissions should be(submissions.size)
          // TODO: submission breakdown counts
          metrics.evaluation.steps should be(
            metrics.evaluation.getTimes + metrics.evaluation.submissions + 1
          )
          metrics.startState.acs.activeContracts should be(acs.size)
          metrics.startState.acs.pendingContracts should be(0)
          metrics.startState.inFlight should be(0)
          metrics.endState.acs.activeContracts should be(
            numberOfActiveContracts(state, trigger.level, trigger.version)
          )
          metrics.endState.acs.pendingContracts should be(
            numberOfPendingContracts(state, trigger.level, trigger.version)
          )
          metrics.endState.inFlight should be(
            numberOfInFlightCommands(state, trigger.level, trigger.version)
          )
        }
      }
    }

    "correctly log metrics for updateState lambda" in {
      forAll(updateState) { case (acs, userState, msg) =>
        val result = for {
          client <- ledgerClient()
          party <- allocateParty(client)
          (trigger, simulator) = getSimulator(
            client,
            QualifiedName.assertFromString("Cats:feedingTrigger"),
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
            .orConverterException
          (submissions, metrics, endState) <- simulator.updateStateLambda(startState, msg)
        } yield (trigger, submissions, metrics, startState, endState)

        whenReady(result) { case (trigger, submissions, metrics, startState, endState) =>
          metrics.evaluation.submissions should be(submissions.size)
          // TODO: submission breakdown counts
          metrics.evaluation.steps should be(
            metrics.evaluation.getTimes + metrics.evaluation.submissions + 1
          )
          metrics.startState.acs.activeContracts should be(
            numberOfActiveContracts(startState, trigger.level, trigger.version)
          )
          metrics.startState.acs.pendingContracts should be(
            numberOfPendingContracts(startState, trigger.level, trigger.version)
          )
          metrics.startState.inFlight should be(
            numberOfInFlightCommands(startState, trigger.level, trigger.version)
          )
          metrics.endState.acs.activeContracts should be(
            numberOfActiveContracts(endState, trigger.level, trigger.version)
          )
          metrics.endState.acs.pendingContracts should be(
            numberOfPendingContracts(endState, trigger.level, trigger.version)
          )
          metrics.endState.inFlight should be(
            numberOfInFlightCommands(endState, trigger.level, trigger.version)
          )
          msg match {
            case TriggerMsg.Completion(completion) if completion.getStatus.code == 0 =>
              metrics.startState.acs.activeContracts should be(metrics.endState.acs.activeContracts)
              // FIXME: use submissions to determine pending counts
              metrics.startState.acs.pendingContracts should be(
                metrics.endState.acs.pendingContracts
              )
              metrics.startState.inFlight should be(metrics.endState.inFlight)

            case TriggerMsg.Completion(_) =>
              metrics.startState.acs.activeContracts should be(metrics.endState.acs.activeContracts)
              // FIXME: account for failures!!
              metrics.startState.acs.pendingContracts should be(
                metrics.endState.acs.pendingContracts
              )
              metrics.startState.inFlight should be(metrics.endState.inFlight)

            case TriggerMsg.Transaction(_) =>
              // FIXME: ACS/in-flight change counts
              succeed

            case TriggerMsg.Heartbeat =>
              metrics.startState.acs.activeContracts should be(metrics.endState.acs.activeContracts)
            // FIXME: use submissions to determine pending counts
          }
        }
      }
    }
  }
}

object TriggerRuleSimulationLibTest {
  // TODO: extract the following generators from user specified Daml code
  private val acsGen: Gen[Seq[CreatedEvent]] = Gen.const(Seq.empty)

  private val userStateGen: Gen[SValue] = Gen.choose(1L, 10L).map(SValue.SInt64(_))

  private val msgGen: Gen[TriggerMsg] = Gen.const(TriggerMsg.Heartbeat)

  // TODO: use results (e.g. submissions and ACS/inflight changes) of simulator runs to infer initialization events
  val initState: Gen[Seq[CreatedEvent]] = acsGen

  // TODO: use results (e.g. submissions and ACS/inflight changes) of simulator runs to infer update events
  val updateState: Gen[(Seq[CreatedEvent], SValue, TriggerMsg)] =
    for {
      acs <- acsGen
      userState <- userStateGen
      msg <- msgGen
    } yield (acs, userState, msg)
}
