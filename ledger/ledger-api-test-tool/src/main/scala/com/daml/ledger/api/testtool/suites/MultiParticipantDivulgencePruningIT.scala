// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Primitive.Party
import com.daml.ledger.test.model.Test.Dummy
import com.daml.ledger.test.semantic.DivulgenceTests._

import scala.concurrent.ExecutionContext

class MultiParticipantDivulgencePruningIT extends LedgerTestSuite {
  test(
    "PRRetroactiveDivulgences",
    "Divulgence pruning succeeds",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)
      contract <- alpha.create(alice, Contract(alice))

      // Retroactively divulge Alice's contract to bob
      _ <- alpha.exercise(
        alice,
        divulgence.exerciseDivulge(_, contract),
      )

      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  test(
    "PRLocalAndNonLocalRetroactiveDivulgences",
    "Divuglence pruning succeeds if first divulgence is not a disclosure but happens in the same transaction as the create",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)

      divulgeNotDiscloseTemplate <- alpha.create(alice, DivulgeNotDiscloseTemplate(alice, bob))

      // Alice creates contract in a context not visible to Bob and follows with a divulgence to Bob in the same transaction
      contract <- alpha.exerciseAndGetContract[Contract](
        alice,
        divulgeNotDiscloseTemplate.exerciseDivulgeNoDisclose(_, divulgence),
      )

      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  test(
    "PRDisclosureAndRetroactiveDivulgence",
    "Disclosure pruning succeeds",
    allocate(SingleParty, SingleParty),
    runConcurrently = false, // pruning call may interact with other tests
  )(implicit ec => { case Participants(Participant(alpha, alice), Participant(beta, bob)) =>
    for {
      divulgence <- createDivulgence(alice, bob, alpha, beta)
      // Alice's contract creation is disclosed to Bob
      contract <- alpha.exerciseAndGetContract[Contract](
        alice,
        divulgence.exerciseCreateAndDisclose,
      )
      _ <- divulgencePruneAndCheck(alice, bob, alpha, beta, contract, divulgence)
    } yield ()
  })

  private def createDivulgence(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
  )(implicit ec: ExecutionContext) =
    for {
      divulgenceHelper <- alpha.create(alice, DivulgenceProposal(alice, bob))
      divulgence <- beta.exerciseAndGetContract[Divulgence](bob, divulgenceHelper.exerciseAccept)
    } yield divulgence

  private def divulgencePruneAndCheck(
      alice: Party,
      bob: Party,
      alpha: ParticipantTestContext,
      beta: ParticipantTestContext,
      contract: Primitive.ContractId[Contract],
      divulgence: binding.Primitive.ContractId[Divulgence],
  )(implicit ec: ExecutionContext) = {
    def bobCanFetch = beta.exerciseAndGetContract[Dummy](
      bob,
      divulgence.exerciseCanFetch(_, contract),
    )

    def pruneBetaAtCurrentOffset(
        pruneAllDivulgedContracts: Boolean = true
    )(implicit ec: ExecutionContext) =
      for {
        offset <- beta.currentEnd()
        // Dummy create to be able to prune
        _ <- beta.create(bob, DivulgenceProposal(bob, alice))
        _ <- beta.prune(offset, pruneAllDivulgedContracts = pruneAllDivulgedContracts)
      } yield ()

    for {
      _ <- bobCanFetch

      offsetAfter_divulgence_1 <- beta.currentEnd()

      // Alice re-divulges the contract to Bob
      _ <- alpha.exerciseAndGetContract[Contract](
        alice,
        divulgence.exerciseDivulge(_, contract),
      )

      _ <- beta.prune(offsetAfter_divulgence_1)

      // Check that Bob can still fetch the contract after pruning the first transaction
      _ <- bobCanFetch
      _ <- pruneBetaAtCurrentOffset(pruneAllDivulgedContracts = false)

      // Check that Bob can still fetch the contract after pruning the second transaction without all divulged contracts
      _ <- bobCanFetch
      _ <- pruneBetaAtCurrentOffset()

      _ <- bobCanFetch
        .mustFail("Bob cannot access the divulged contract after all divulgence pruning")
    } yield ()
  }
}
