// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.trigger.simulation.{TriggerRuleSimulationLib, TriggerSimulationTesting}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.speedy.SValue
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import java.util.UUID
import scala.concurrent.Future

class CatFeedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {

  import AbstractTriggerTest._
  import TriggerRuleSimulationLib._

  "Cat slow feeding trigger" should {
    "initialize lambda" in {
      val setup = (_: ApiTypes.Party) => Seq.empty

      initialStateLambdaAssertion(setup) { case (state, submissions) =>
        val userState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState""", state)

        assertEqual(userState, "< False, 0 >")
        submissions should be ('empty)
      }
    }

    "update lambda" should {
      val templateId = "Cats:Cat"
      val knownContractId = ""
      val unknownContractId = ""
      val breedingRate = 25
      val userStartState = "3"
      val commandId = UUID.randomUUID().toString
      val acsF = (party: ApiTypes.Party) => Seq(createdEvent(templateId, s"<$party, 1>", contractId = knownContractId))

      "for heartbeats" in {
        val setup = { (party: ApiTypes.Party) =>
            (
              unsafeSValueFromLf(s"<False, $userStartState>"),
              acsF(party),
              TriggerMsg.Heartbeat,
            )
        }

        updateStateLambdaAssertion(setup)  { case (startState, endState, submissions) =>
          val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState._2""", endState)
          val startACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", startState)
          val endACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", endState)

          assertEqual(userEndState, s"$userStartState + $breedingRate")
          submissions.map(numberOfExerciseCommands).sum should be(breedingRate)
          startACS should be(endACS)
        }
      }

      "for completions" in {
        val setup = { (party: ApiTypes.Party) =>
            (
              unsafeSValueFromLf(s"<False, $userStartState>"),
              acsF(party),
              TriggerMsg.Completion(completion(commandId)),
            )
        }

        updateStateLambdaAssertion(setup) { case (startState, endState, submissions) =>
          val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState._2""", endState)
          val startACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", startState)
          val endACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", endState)

          assertEqual(userEndState, userStartState)
          submissions should be('empty)
          startACS should be(endACS)
        }
      }

      "for created events" in {
        val setup = { (party: ApiTypes.Party) =>
            (
              unsafeSValueFromLf(s"<False, $userStartState>"),
              Seq.empty,
              TriggerMsg.Transaction(Transaction(events = Seq(Created(createdEvent(templateId, s"<$party, 2>", contractId = unknownContractId))))),
            )
        }

        updateStateLambdaAssertion(setup) { case (startState, endState, submissions) =>
          val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState._2""", endState)
          val sizeOfEndACS = unsafeSValueApp(s"\\(st: TriggerState @Int64) -> sizeof(st.acs.activeContracts $templateId)", endState)

          assertEqual(userEndState, userStartState)
          submissions should be('empty)
          sizeOfEndACS should be(unsafeSValueFromLf("1"))
        }
      }

      "for archive events" in {
        val setup = { (party: ApiTypes.Party) =>
            (
              unsafeSValueFromLf(s"<False, $userStartState>"),
              acsF(party),
              TriggerMsg.Transaction(Transaction(events = Seq(Archived(archivedEvent(templateId, knownContractId))))),
            )
        }

        updateStateLambdaAssertion(setup) { case (startState, endState, submissions) =>
          val userEndState = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.userState._2""", endState)
          val endACS = unsafeSValueApp("""\(st: TriggerState @Int64) -> st.acs.activeContracts""", endState)

          assertEqual(userEndState, userStartState)
          submissions should be('empty)
          endACS should be('empty)
        }
      }
    }
  }

  private def initialStateLambdaAssertion(setup: ApiTypes.Party => Seq[CreatedEvent])(assertion: (SValue, Seq[SubmitRequest]) => Assertion): Future[Assertion] = {
    for {
      client <- defaultLedgerClient()
      party <- allocateParty(client)
      (_, simulator) = getSimulator(
        client,
        QualifiedName.assertFromString("Cats:slowFeedingTrigger"),
        packageId,
        applicationId,
        compiledPackages,
        timeProviderType,
        triggerRunnerConfiguration,
        party.unwrap,
      )
      acs <- setup(party)
      (submissions, _, state) <- simulator.initialStateLambda(acs)
    } yield {
      assertion(state, submissions)
    }
  }

  private def updateStateLambdaAssertion(setup: ApiTypes.Party => (SValue, Seq[CreatedEvent], TriggerMsg))(assertion: (SValue, SValue, Seq[SubmitRequest]) => Assertion): Future[Assertion] = {
    for {
      client <- defaultLedgerClient()
      party <- allocateParty(client)
      (trigger, simulator) = getSimulator(
        client,
        QualifiedName.assertFromString("Cats:slowFeedingTrigger"),
        packageId,
        applicationId,
        compiledPackages,
        timeProviderType,
        triggerRunnerConfiguration,
        party.unwrap,
      )
      (startUserState, acs, msg) <- setup(party)
      converter = new Converter(compiledPackages, trigger)
      startState = converter.fromTriggerUpdateState(
        acs,
        startUserState,
        parties = TriggerParties(party, Set.empty),
        triggerConfig = triggerRunnerConfiguration,
      )
      (submissions, _, endState) <- simulator.updateStateLambda(startState, msg)
    } yield {
      assertion(startState, endState, submissions)
    }
  }
}
