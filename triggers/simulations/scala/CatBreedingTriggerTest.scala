// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.lf.data.Ref
import com.daml.lf.engine.trigger.simulation.{TriggerRuleSimulationLib, TriggerSimulationTesting}
import com.daml.lf.engine.trigger.test.AbstractTriggerTest
import com.daml.lf.language.StablePackage
import com.daml.lf.speedy.SValue
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import java.util.UUID
import scala.concurrent.Future

class CatBreedingTriggerTest extends AsyncWordSpec with TriggerSimulationTesting {

  import AbstractTriggerTest._
  import TriggerRuleSimulationLib._

  val tuple2TyCon: String = s"'${StablePackage.DA.Types.packageId}':DA.Types:Tuple2"

  "Cat slow breeding trigger" should {
    "initialize lambda" in {
      val setup = (_: ApiTypes.Party) => Seq.empty

      initialStateLambdaAssertion(setup) { case (state, submissions) =>
        implicit trigger =>
          assertEqual(userState(state), s"$tuple2TyCon @Bool @Int64 { _1 = False, _2 = 0 }")
          submissions should be(Symbol("empty"))
      }
    }

    "update lambda" should {
      val templateId = s"$packageId:Cats:Cat"
      val templateTyCon = s"'$packageId':Cats:Cat"
      val knownContractId = s"known-${UUID.randomUUID()}"
      val unknownContractId = s"unknown-${UUID.randomUUID()}"
      val breedingRate = 25
      val userStartState = 3
      val commandId = s"command-${UUID.randomUUID()}"
      val acsF = (party: ApiTypes.Party) =>
        Seq(
          createdEvent(
            templateId,
            s"$templateTyCon { owner = ${mkParty(party)}, name = 1 }",
            contractId = knownContractId,
          )
        )

      "for heartbeats" in {
        val setup = { (party: ApiTypes.Party) =>
          (
            unsafeSValueFromLf(s"$tuple2TyCon @Bool @Int64 { _1 = False, _2 = $userStartState }"),
            acsF(party),
            TriggerMsg.Heartbeat,
          )
        }
        updateStateLambdaAssertion(setup) { case (startState, endState, submissions) =>
          implicit trigger =>
            val userEndState = unsafeSValueApp(
              s"\\(tuple: $tuple2TyCon) -> $tuple2TyCon @Bool @Int64 {_2} tuple",
              userState(endState),
            )
            val startACS = activeContracts(startState)
            val endACS = activeContracts(endState)

            assertEqual(userEndState, s"${userStartState + breedingRate}")
            submissions.map(numberOfCreateCommands).sum should be(breedingRate)
            startACS should be(endACS)
        }
      }

      "for completions" in {
        val setup = { (party: ApiTypes.Party) =>
          (
            unsafeSValueFromLf(s"$tuple2TyCon @Bool @Int64 { _1 = False, _2 = $userStartState }"),
            acsF(party),
            TriggerMsg.Completion(completion(commandId)),
          )
        }

        updateStateLambdaAssertion(setup) { case (startState, endState, submissions) =>
          implicit trigger =>
            val userEndState = unsafeSValueApp(
              s"\\(tuple: $tuple2TyCon) -> $tuple2TyCon @Bool @Int64 {_2} tuple",
              userState(endState),
            )
            val startACS = activeContracts(startState)
            val endACS = activeContracts(endState)

            assertEqual(userEndState, userStartState.toString)
            submissions should be(Symbol("empty"))
            startACS should be(endACS)
        }
      }

      "for created events" in {
        val setup = { (party: ApiTypes.Party) =>
          (
            unsafeSValueFromLf(s"$tuple2TyCon @Bool @Int64 { _1 = False, _2 = $userStartState }"),
            Seq.empty,
            TriggerMsg.Transaction(
              transaction(
                createdEvent(
                  templateId,
                  s"$templateTyCon { owner = ${mkParty(party)}, name = 2 }",
                  contractId = unknownContractId,
                )
              )
            ),
          )
        }

        updateStateLambdaAssertion(setup) { case (_, endState, submissions) =>
          implicit trigger =>
            val userEndState = unsafeSValueApp(
              s"\\(tuple: $tuple2TyCon) -> $tuple2TyCon @Bool @Int64 {_2} tuple",
              userState(endState),
            )
            val templateTyRep = unsafeSValueFromLf(
              s"DA.Internal.Any:TemplateTypeRep { getTemplateTypeRep = type_rep @$templateTyCon }"
            )
            val sizeOfEndACS = activeContracts(endState)(trigger)(templateTyRep).size

            assertEqual(userEndState, userStartState.toString)
            submissions should be(Symbol("empty"))
            SValue.SInt64(sizeOfEndACS.toLong) should be(unsafeSValueFromLf("1"))
        }
      }

      "for archive events" in {
        val setup = { (party: ApiTypes.Party) =>
          (
            unsafeSValueFromLf(s"$tuple2TyCon @Bool @Int64 { _1 = False, _2 = $userStartState }"),
            acsF(party),
            TriggerMsg.Transaction(transaction(archivedEvent(templateId, knownContractId))),
          )
        }

        updateStateLambdaAssertion(setup) { case (_, endState, submissions) =>
          implicit trigger =>
            val userEndState = unsafeSValueApp(
              s"\\(tuple: $tuple2TyCon) -> $tuple2TyCon @Bool @Int64 {_2} tuple",
              userState(endState),
            )
            val endACS = activeContracts(endState)

            assertEqual(userEndState, userStartState.toString)
            submissions should be(Symbol("empty"))
            endACS should be(Symbol("empty"))
        }
      }
    }
  }

  private def initialStateLambdaAssertion(setup: ApiTypes.Party => Seq[CreatedEvent])(
      assertion: (SValue, Seq[SubmitRequest]) => TriggerDefinition => Assertion
  ): Future[Assertion] = {
    for {
      client <- defaultLedgerClient()
      party <- allocateParty(client)
      (trigger, simulator) = getSimulator(
        client,
        Ref.QualifiedName.assertFromString("Cats:slowBreedingTrigger"),
        packageId,
        applicationId,
        compiledPackages,
        timeProviderType,
        triggerRunnerConfiguration,
        party.unwrap,
      )
      acs = setup(party)
      (submissions, _, state) <- simulator.initialStateLambda(acs)
    } yield {
      assertion(state, submissions)(trigger)
    }
  }

  private def updateStateLambdaAssertion(
      setup: ApiTypes.Party => (SValue, Seq[CreatedEvent], TriggerMsg)
  )(
      assertion: (SValue, SValue, Seq[SubmitRequest]) => TriggerDefinition => Assertion
  ): Future[Assertion] = {
    for {
      client <- defaultLedgerClient()
      party <- allocateParty(client)
      (trigger, simulator) = getSimulator(
        client,
        Ref.QualifiedName.assertFromString("Cats:slowBreedingTrigger"),
        packageId,
        applicationId,
        compiledPackages,
        timeProviderType,
        triggerRunnerConfiguration,
        party.unwrap,
      )
      (startUserState, acs, msg) = setup(party)
      converter = new Converter(compiledPackages, trigger)
      startState = converter.fromTriggerUpdateState(
        acs,
        startUserState,
        parties = TriggerParties(party, Set.empty),
        triggerConfig = triggerRunnerConfiguration,
      )
      (submissions, _, endState) <- simulator.updateStateLambda(startState, msg)
    } yield {
      assertion(startState, endState, submissions)(trigger)
    }
  }
}
