// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.model.test.MultiPartyContract
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}

import java.util.UUID
import java.util.regex.Pattern
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

final class MultiPartySubmissionIT extends LedgerTestSuite {
  implicit val multiPartyContractCompanion: ContractCompanion.WithoutKey[
    MultiPartyContract.Contract,
    MultiPartyContract.ContractId,
    MultiPartyContract,
  ] = MultiPartyContract.COMPANION

  test(
    "MPSSubmit",
    "Submit creates a multi-party contract",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    // Create a contract for (Alice, Bob)
    val request = ledger.submitRequest(
      actAs = List(alice, bob),
      readAs = List.empty,
      commands = new MultiPartyContract(List(alice, bob).map(_.getValue).asJava, "").create.commands,
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
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob))) =>
    for {
      // Create a contract for (Alice, Bob)
      _ <- ledger.create(
        actAs = List(alice, bob),
        readAs = List.empty,
        template = new MultiPartyContract(List(alice, bob).map(_.getValue).asJava, ""),
      )
    } yield ()
  })

  test(
    "MPSCreateInsufficientAuthorization",
    "Create fails with insufficient authorization",
    allocate(Parties(3)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie))) =>
    for {
      // Create a contract for (Alice, Bob, Charlie), but only submit as (Alice, Bob).
      // Should fail because required authorizer Charlie is missing from submitters.
      failure <- ledger
        .create(
          actAs = List(alice, bob),
          readAs = List.empty,
          template = new MultiPartyContract(List(alice, bob, charlie).map(_.getValue).asJava, ""),
        )
        .mustFail("submitting a contract with a missing authorizers")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Interpreter.AuthorizationError,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "MPSAddSignatoriesSuccess",
    "Exercise AddSignatories succeeds with sufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
    for {
      // Create a contract for (Alice, Bob)
      (contract, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Exercise a choice to add (Charlie, David)
      // Requires authorization from all four parties
      _ <- ledger.exercise(
        actAs = List(alice, bob, charlie, david),
        readAs = List.empty,
        exercise =
          contract.exerciseMPAddSignatories(List(alice, bob, charlie, david).map(_.getValue).asJava),
      )
    } yield ()
  })

  test(
    "MPSAddSignatoriesInsufficientAuthorization",
    "Exercise AddSignatories fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
    for {
      // Create a contract for (Alice, Bob)
      (contract, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Exercise a choice to add (Charlie, David) to the list of signatories
      // Should fail as it's missing authorization from one of the original signatories (Alice)
      failure <- ledger
        .exercise(
          actAs = List(bob, charlie, david),
          readAs = List.empty,
          exercise = contract.exerciseMPAddSignatories(
            List(alice, bob, charlie, david).map(_.getValue).asJava
          ),
        )
        .mustFail("exercising a choice with a missing authorizers")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Interpreter.AuthorizationError,
        None,
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "MPSFetchOtherSuccess",
    "Exercise FetchOther succeeds with sufficient authorization and read delegation",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
    for {
      // Create contract A for (Alice, Bob)
      (contractA, _) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      _ <- ledger.exercise(
        actAs = List(charlie, david),
        readAs = List(alice),
        exercise =
          contractB.exerciseMPFetchOther(contractA, List(charlie, david).map(_.getValue).asJava),
      )
    } yield ()
  })

  test(
    "MPSFetchOtherInsufficientAuthorization",
    "Exercise FetchOther fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
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
          exercise =
            contractB.exerciseMPFetchOther(contractA, List(charlie, david).map(_.getValue).asJava),
        )
        .mustFail("exercising a choice without authorization to fetch another contract")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Interpreter.AuthorizationError,
        Some(Pattern.compile("of the fetched contract to be an authorizer, but authorizers were")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "MPSFetchOtherInvisible",
    "Exercise FetchOther fails because the contract isn't visible",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
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
          exercise =
            contractB.exerciseMPFetchOther(contractA, List(charlie, david).map(_.getValue).asJava),
        )
        .mustFail("exercising a choice without authorization to fetch another contract")
    } yield {
      assertGrpcErrorRegex(
        failure,
        ConsistencyErrors.ContractNotFound,
        Some(Pattern.compile("Contract could not be found")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  private[this] def createMultiPartyContract(
      ledger: ParticipantTestContext,
      submitters: List[Party],
      value: String = UUID.randomUUID().toString,
  )(implicit
      ec: ExecutionContext
  ): Future[(MultiPartyContract.ContractId, MultiPartyContract)] =
    ledger
      .create(
        actAs = submitters,
        readAs = List.empty,
        template = new MultiPartyContract(submitters.map(_.getValue).asJava, value),
      )
      .map(cid => cid -> new MultiPartyContract(submitters.map(_.getValue).asJava, value))
}
