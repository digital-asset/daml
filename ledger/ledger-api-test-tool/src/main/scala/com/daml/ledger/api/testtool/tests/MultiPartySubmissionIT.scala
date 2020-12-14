// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test._
import io.grpc.Status
import scalaz.syntax.tag._

final class MultiPartySubmissionIT extends LedgerTestSuite {

  test(
    "MPSsubmit",
    "Submit creates a multi-party contract",
    allocate(Parties(2)),
  )(implicit ec => {
    case Participants(Participant(ledger, alice, bob)) =>
      // Create a contract for (Alice, Bob)
      val request = ledger.submitRequest(
        actAs = List(alice, bob),
        readAs = List.empty,
        commands = MultiPartyContract(Primitive.List(alice, bob), "").create.command,
      )

      for {
        _ <- ledger.submit(request)
        completions <- ledger.firstCompletions(bob)
      } yield {
        assert(completions.length == 1)
        assert(completions.head.commandId == request.commands.get.commandId)
      }
  })

  test(
    "MPSsubmitAndWait",
    "SubmitAndWait creates a multi-party contract of the expected template",
    allocate(Parties(2)),
  )(implicit ec => {
    case Participants(Participant(ledger, alice, bob)) =>
      for {
        // Create a contract for (Alice, Bob)
        _ <- ledger.create(
          actAs = List(alice, bob),
          readAs = List.empty,
          template = MultiPartyContract(Primitive.List(alice, bob), "")
        )
        active <- ledger.activeContracts(bob)
      } yield {
        assert(active.length == 1)
        assert(active.head.templateId.get == MultiPartyContract.id.unwrap)
      }
  })

  test(
    "MPSsubmitAndWaitInsufficientAuthorization",
    "SubmitAndWait should fail to create a multi-party contract with insufficient authorization",
    allocate(Parties(3)),
  )(implicit ec => {
    case Participants(Participant(ledger, alice, bob, charlie)) =>
      for {
        // Create a contract for (Alice, Bob, Charlie), but only submit as (Alice, Bob).
        // Should fail because required authorizer Charlie is missing from submitters.
        failure <- ledger
          .create(
            actAs = List(alice, bob),
            readAs = List.empty,
            template = MultiPartyContract(Primitive.List(alice, bob, charlie), "")
          )
          .failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          None,
        )
      }
  })

  test(
    "MPSsubmitAndWaitMultiPartyChoice",
    "SubmitAndWait exercises a multi-party choice",
    allocate(Parties(4)),
  )(implicit ec => {
    case Participants(Participant(ledger, alice, bob, charlie, david)) =>
      for {
        // Create a contract for (Alice, Bob)
        contract <- ledger.create(
          actAs = List(alice, bob),
          readAs = List.empty,
          template = MultiPartyContract(Primitive.List(alice, bob), "")
        )

        // Exercise a choice to add (Charlie, David)
        // Requires authorization from all four parties
        _ <- ledger.exercise(
          actAs = List(alice, bob, charlie, david),
          readAs = List.empty,
          exercise = contract
            .exerciseAddSignatories(unusedActorArgument, Primitive.List(alice, bob, charlie, david))
        )
        active <- ledger.activeContracts(david)
      } yield {
        assert(active.length == 1)
        assert(active.head.templateId.get == MultiPartyContract.id.unwrap)
      }
  })

  test(
    "MPSsubmitAndWaitMultiPartyChoiceReadDelegation",
    "SubmitAndWait exercises a multi-party choice with read delegation",
    allocate(Parties(4)),
  )(implicit ec => {
    case Participants(Participant(ledger, alice, bob, charlie, david)) =>
      for {
        // Create a contract for (Alice, Bob)
        contract <- ledger.create(
          actAs = List(alice, bob),
          readAs = List.empty,
          template = MultiPartyContract(Primitive.List(alice, bob), "")
        )

        // Exercise a choice to duplicate contract for (Charlie, David)
        // Requires authorization from all the new parties,
        // and read delegation from one of the old parties
        _ <- ledger.exercise(
          actAs = List(charlie, david),
          readAs = List(alice),
          exercise =
            contract.exerciseDuplicateFor(unusedActorArgument, Primitive.List(charlie, david)))
        active <- ledger.activeContracts(david)
      } yield {
        assert(active.length == 1)
        assert(active.head.templateId.get == MultiPartyContract.id.unwrap)
      }
  })

  test(
    "MPSsubmitAndWaitMultiPartyChoiceMissingReadDelegation",
    "SubmitAndWait should fail to exercise a multi-party choice with missing read delegation",
    allocate(Parties(4)),
  )(implicit ec => {
    case Participants(Participant(ledger, alice, bob, charlie, david)) =>
      for {
        // Create a contract for (Alice, Bob)
        contract <- ledger.create(
          actAs = List(alice, bob),
          readAs = List.empty,
          template = MultiPartyContract(List(alice, bob), "")
        )

        // Exercise a choice to duplicate contract for (Charlie, David)
        // Should fail, as the new parties do not see the original contract
        failure <- ledger
          .exercise(
            actAs = List(charlie, david),
            readAs = List.empty,
            exercise =
              contract.exerciseDuplicateFor(unusedActorArgument, Primitive.List(charlie, david)))
          .failed
      } yield {
        assertGrpcError(
          failure,
          Status.Code.INVALID_ARGUMENT,
          None,
        )
      }
  })

  // The "actor" argument in the generated methods to exercise choices is not used
  private[this] val unusedActorArgument: Primitive.Party = Primitive.Party("")
}
