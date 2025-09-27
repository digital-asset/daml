// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.experimental.test.MultiPartyContract
import com.digitalasset.base.error.{ErrorCategory, ErrorCode}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

import java.util.UUID
import java.util.regex.Pattern
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

final class ContractKeysMultiPartySubmissionIT extends LedgerTestSuite {
  implicit val multiPartyContractCompanion: ContractCompanion.WithKey[
    MultiPartyContract.Contract,
    MultiPartyContract.ContractId,
    MultiPartyContract,
    MultiPartyContract,
  ] = MultiPartyContract.COMPANION

  test(
    "MPSFetchOtherByKeyOtherSuccess",
    "Exercise FetchOtherByKey succeeds with sufficient authorization and read delegation",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
    for {
      // Create contract A for (Alice, Bob)
      (_, keyA) <- createMultiPartyContract(ledger, List(alice, bob))

      // Create contract B for (Alice, Bob, Charlie, David)
      (contractB, _) <- createMultiPartyContract(ledger, List(alice, bob, charlie, david))

      // Fetch contract A through contract B as (Charlie, David)
      _ <- ledger.exercise(
        actAs = List(charlie, david),
        readAs = List(alice),
        exercise =
          contractB.exerciseMPFetchOtherByKey(keyA, List(charlie, david).map(_.getValue).asJava),
      )
    } yield ()
  })

  test(
    "MPSFetchOtherByKeyInsufficientAuthorization",
    "Exercise FetchOtherByKey fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
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
          exercise =
            contractB.exerciseMPFetchOtherByKey(keyA, List(charlie, david).map(_.getValue).asJava),
        )
        .mustFail("exercising a choice without authorization to fetch another contract by key")
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
    "MPSFetchOtherByKeyInvisible",
    "Exercise FetchOtherByKey fails because the contract isn't visible",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
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
          exercise =
            contractB.exerciseMPFetchOtherByKey(keyA, List(charlie, david).map(_.getValue).asJava),
        )
        .mustFail("exercising a choice without authorization to fetch another contract by key")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound,
        Some(Pattern.compile("dependency error: couldn't find key")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "MPSLookupOtherByKeyOtherSuccess",
    "Exercise LookupOtherByKey succeeds with sufficient authorization and read delegation",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
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
          .exerciseMPLookupOtherByKey(
            keyA,
            List(charlie, david).map(_.getValue).asJava,
            Some(contractA).toJava,
          ),
      )
    } yield ()
  })

  test(
    "MPSLookupOtherByKeyInsufficientAuthorization",
    "Exercise LookupOtherByKey fails with insufficient authorization",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
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
            .exerciseMPLookupOtherByKey(
              keyA,
              List(charlie, david).map(_.getValue).asJava,
              Some(contractA).toJava,
            ),
        )
        .mustFail("exercising a choice without authorization to look up another contract by key")
    } yield {
      assertGrpcErrorRegex(
        failure,
        CommandExecutionErrors.Interpreter.AuthorizationError,
        Some(Pattern.compile("requires authorizers (.*) for lookup by key, but it only has")),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })

  test(
    "MPSLookupOtherByKeyInvisible",
    "Exercise LookupOtherByKey fails because the contract isn't visible",
    allocate(Parties(4)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(alice, bob, charlie, david))) =>
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
            .exerciseMPLookupOtherByKey(
              keyA,
              List(charlie, david).map(_.getValue).asJava,
              Some(contractA).toJava,
            ),
        )
        .mustFail("exercising a choice without authorization to look up another contract by key")
    } yield {
      val trace =
        """    in choice [0-9a-f]{8}:Test:MultiPartyContract:MPLookupOtherByKey on contract [0-9a-f]{10} \(#0\)
          |    in exercise command [0-9a-f]{8}:Test:MultiPartyContract:MPLookupOtherByKey on contract [0-9a-f]{10}.""".stripMargin
      assertGrpcError(
        failure,
        new ErrorCode(
          CommandExecutionErrors.Interpreter.FailureStatus.id,
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        )(
          CommandExecutionErrors.Interpreter.FailureStatus.parent
        ) {},
        Some("LookupOtherByKey value matches"),
        checkDefiniteAnswerMetadata = true,
        additionalErrorAssertions = throwable =>
          assertMatches(
            "exercise_trace",
            extractErrorInfoMetadataValue(throwable, "exercise_trace"),
            Pattern.compile(trace),
          ),
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
