// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.UUID
import java.util.regex.Pattern

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.client.binding.Primitive.{Party, List => PList}
import com.daml.ledger.test.model.Test._
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

final class MultiPartySubmissionIT extends LedgerTestSuite {

  test(
    "MPSSubmit",
    "Submit creates a multi-party contract",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    // Create a contract for (Alice, Bob)
    val request = ledger.submitRequest(
      actAs = List(alice, bob),
      readAs = List.empty,
      commands = MultiPartyContract(PList(alice, bob), "").create.command,
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
    "MPSCreateSuccess",
    "Create succeeds with sufficient authorization",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob)) =>
    for {
      // Create a contract for (Alice, Bob)
      _ <- ledger.create(
        actAs = List(alice, bob),
        readAs = List.empty,
        template = MultiPartyContract(PList(alice, bob), ""),
      )
    } yield ()
  })

  test(
    "MPSCreateInsufficientAuthorization",
    "Create fails with insufficient authorization",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie)) =>
    for {
      // Create a contract for (Alice, Bob, Charlie), but only submit as (Alice, Bob).
      // Should fail because required authorizer Charlie is missing from submitters.
      failure <- ledger
        .create(
          actAs = List(alice, bob),
          readAs = List.empty,
          template = MultiPartyContract(PList(alice, bob, charlie), ""),
        )
        .mustFail("submitting a contract with a missing authorizers")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        None,
      )
    }
  })

  test(
    "MPSAddSignatoriesSuccess",
    "Exercise AddSignatories succeeds with sufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create a contract for (Alice, Bob)
      (contract, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Exercise a choice to add (Charlie, David)
      // Requires authorization from all four parties
      _ <- ledger.exercise(
        actAs = List(alice, bob, charlie, david),
        readAs = List.empty,
        exercise = contract.exerciseMPAddSignatories(unusedActor, PList(alice, bob, charlie, david)),
      )
    } yield ()
  })

  test(
    "MPSAddSignatoriesInsufficientAuthorization",
    "Exercise AddSignatories fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create a contract for (Alice, Bob)
      (contract, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Exercise a choice to add (Charlie, David) to the list of signatories
      // Should fail as it's missing authorization from one of the original signatories (Alice)
      failure <- ledger
        .exercise(
          actAs = List(bob, charlie, david),
          readAs = List.empty,
          exercise =
            contract.exerciseMPAddSignatories(unusedActor, PList(alice, bob, charlie, david)),
        )
        .mustFail("exercising a choice with a missing authorizers")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        None,
      )
    }
  })

  test(
    "MPSFetchOtherSuccess",
    "Exercise FetchOther succeeds with sufficient authorization and read delegation",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (contractA, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      _ <- ledger.exercise(
        actAs = List(charlie, david),
        readAs = List(alice),
        exercise = contractB.exerciseMPFetchOther(unusedActor, contractA, PList(charlie, david)),
      )
    } yield ()
  })

  test(
    "MPSFetchOtherInsufficientAuthorization",
    "Exercise FetchOther fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (contractA, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      // Should fail with an authorization error
      failure <- ledger
        .exercise(
          actAs = List(charlie, david),
          readAs = List(bob, alice),
          exercise = contractB.exerciseMPFetchOther(unusedActor, contractA, PList(charlie, david)),
        )
        .mustFail("exercising a choice without authorization to fetch another contract")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(Pattern.compile("of the fetched contract to be an authorizer, but authorizers were")),
      )
    }
  })

  test(
    "MPSFetchOtherInvisible",
    "Exercise FetchOther fails because the contract isn't visible",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (contractA, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      // Should fail with an interpretation error because the fetched contract isn't visible to any submitter
      failure <- ledger
        .exercise(
          actAs = List(charlie, david),
          readAs = List.empty,
          exercise = contractB.exerciseMPFetchOther(unusedActor, contractA, PList(charlie, david)),
        )
        .mustFail("exercising a choice without authorization to fetch another contract")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.ABORTED,
        Some(Pattern.compile("Contract could not be found")),
      )
    }
  })

  test(
    "MPSFetchOtherByKeyOtherSuccess",
    "Exercise FetchOtherByKey succeeds with sufficient authorization and read delegation",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (_, keyA) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      _ <- ledger.exercise(
        actAs = List(charlie, david),
        readAs = List(alice),
        exercise = contractB.exerciseMPFetchOtherByKey(unusedActor, keyA, PList(charlie, david)),
      )
    } yield ()
  })

  test(
    "MPSFetchOtherByKeyInsufficientAuthorization",
    "Exercise FetchOtherByKey fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (_, keyA) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      // Should fail with an authorization error
      failure <- ledger
        .exercise(
          actAs = List(charlie, david),
          readAs = List(bob, alice),
          exercise = contractB.exerciseMPFetchOtherByKey(unusedActor, keyA, PList(charlie, david)),
        )
        .mustFail("exercising a choice without authorization to fetch another contract by key")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(Pattern.compile("of the fetched contract to be an authorizer, but authorizers were")),
      )
    }
  })

  test(
    "MPSFetchOtherByKeyInvisible",
    "Exercise FetchOtherByKey fails because the contract isn't visible",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (_, keyA) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      // Should fail with an interpretation error because the fetched contract isn't visible to any submitter
      failure <- ledger
        .exercise(
          actAs = List(charlie, david),
          readAs = List.empty,
          exercise = contractB.exerciseMPFetchOtherByKey(unusedActor, keyA, PList(charlie, david)),
        )
        .mustFail("exercising a choice without authorization to fetch another contract by key")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(Pattern.compile("dependency error: couldn't find key")),
      )
    }
  })

  test(
    "MPSLookupOtherByKeyOtherSuccess",
    "Exercise LookupOtherByKey succeeds with sufficient authorization and read delegation",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (contractA, keyA) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      _ <- ledger.exercise(
        actAs = List(charlie, david),
        readAs = List(alice),
        exercise = contractB
          .exerciseMPLookupOtherByKey(unusedActor, keyA, PList(charlie, david), Some(contractA)),
      )
    } yield ()
  })

  test(
    "MPSLookupOtherByKeyInsufficientAuthorization",
    "Exercise LookupOtherByKey fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (contractA, keyA) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      // Should fail with an authorization error
      failure <- ledger
        .exercise(
          actAs = List(charlie, david),
          readAs = List(bob, alice),
          exercise = contractB
            .exerciseMPLookupOtherByKey(unusedActor, keyA, PList(charlie, david), Some(contractA)),
        )
        .mustFail("exercising a choice without authorization to look up another contract by key")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(Pattern.compile("requires authorizers (.*) for lookup by key, but it only has")),
      )
    }
  })

  test(
    "MPSLookupOtherByKeyInvisible",
    "Exercise LookupOtherByKey fails because the contract isn't visible",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, alice, bob, charlie, david)) =>
    for {
      // Create contract A for (Alice, Bob)
      (contractA, keyA) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      // Should fail with an interpretation error because the fetched contract isn't visible to any submitter
      failure <- ledger
        .exercise(
          actAs = List(charlie, david),
          readAs = List.empty,
          exercise = contractB
            .exerciseMPLookupOtherByKey(unusedActor, keyA, PList(charlie, david), Some(contractA)),
        )
        .mustFail("exercising a choice without authorization to look up another contract by key")
    } yield {
      assertGrpcError(
        failure,
        Status.Code.INVALID_ARGUMENT,
        Some(Pattern.compile("User abort: LookupOtherByKey value matches")),
      )
    }
  })

  private[this] def createMultiPartyContract(
      ledger: ParticipantTestContext,
      submitters: List[Party],
      value: String = UUID.randomUUID().toString,
  )(implicit
      ec: ExecutionContext
  ): Future[(Primitive.ContractId[MultiPartyContract], MultiPartyContract)] =
    ledger
      .create(
        actAs = submitters,
        readAs = List.empty,
        template = MultiPartyContract(submitters, value),
      )
      .map(cid => cid -> MultiPartyContract(submitters, value))

  // The "actor" argument in the generated methods to exercise choices is not used
  private[this] val unusedActor: Party = Party("")
}
