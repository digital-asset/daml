// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, Update}
import com.daml.ledger.test.java.semantic.deeplynestedvalue.{Contract, Handler, Nat, nat}
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class DeeplyNestedValueIT extends LedgerTestSuite {
  implicit val handlerCompanion
      : ContractCompanion.WithoutKey[Handler.Contract, Handler.ContractId, Handler] =
    Handler.COMPANION

  @tailrec
  private[this] def toNat(i: Long, acc: Nat = new nat.Z(javaapi.data.Unit.getInstance)): Nat =
    if (i == 0) acc else toNat(i - 1, new nat.S(acc))

  private[this] def waitForUpdateId(
      alpha: ParticipantTestContext,
      party: Party,
      command: Update[?],
  )(implicit
      ec: ExecutionContext
  ): Future[Either[Throwable, String]] =
    alpha
      .submitAndWait(
        alpha.submitAndWaitRequest(party, command.commands)
      )
      .transform(x => Success(x.map(_.updateId).toEither))

  private[this] def camlCase(s: String) =
    s.split(" ").iterator.map(_.capitalize).mkString("")

  List[Long](46, 100, 101, 110, 200).foreach { nesting =>
    val accepted = nesting <= 100
    val result = if (accepted) "Accept" else "Reject"

    // Once converted to Nat, `n` will have a nesting `nesting`.
    // Note that Nat.Z(()) has nesting 1.
    val n = nesting - 1

    // Choice argument are always wrapped in a record
    val nChoiceArgument = n - 1

    // The nesting of the payload of a `Contract` is one more than the nat it contains
    val nContract = n - 1

    def test[T](description: String, errorCodeIfExpected: ErrorCode)(
        update: ExecutionContext => (
            ParticipantTestContext,
            Party,
        ) => Future[Either[Throwable, T]]
    ): Unit =
      super.test(
        result + camlCase(description) + nesting.toString,
        s"${result.toLowerCase}s $description with a nesting of $nesting",
        allocate(SingleParty),
      )(implicit ec => { case Participants(Participant(alpha, Seq(party))) =>
        update(ec)(alpha, party).map {
          case Right(_) if accepted => ()
          case Left(err: Throwable) if !accepted =>
            assertGrpcError(
              err,
              errorCodeIfExpected,
              None,
              checkDefiniteAnswerMetadata = true,
            )
          case otherwise =>
            fail("Unexpected " + otherwise.fold(err => s"failure: $err", _ => "success"))
        }
      })

    test(
      "create command",
      CommandExecutionErrors.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      waitForUpdateId(alpha, party, new Contract(party, nContract, toNat(nContract)).create)
    }

    test(
      "exercise command",
      CommandExecutionErrors.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForUpdateId(
          alpha,
          party,
          handler.exerciseDestruct(toNat(nChoiceArgument)),
        )
      } yield result
    }

    test(
      "create argument in CreateAndExercise command",
      CommandExecutionErrors.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      waitForUpdateId(
        alpha,
        party,
        new Contract(party, nContract, toNat(nContract)).createAnd
          .exerciseArchive(),
      )
    }

    test(
      "choice argument in CreateAndExercise command",
      CommandExecutionErrors.Preprocessing.PreprocessingFailed,
    ) { implicit ec => (alpha, party) =>
      waitForUpdateId(
        alpha,
        party,
        new Handler(party).createAnd.exerciseDestruct(toNat(nChoiceArgument)),
      )
    }

    test(
      "exercise argument",
      CommandExecutionErrors.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <-
          waitForUpdateId(
            alpha,
            party,
            handler.exerciseConstructThenDestruct(nChoiceArgument),
          )
      } yield result
    }

    test(
      "exercise output",
      CommandExecutionErrors.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <-
          waitForUpdateId(alpha, party, handler.exerciseConstruct(n))
      } yield result
    }

    test(
      "create argument",
      CommandExecutionErrors.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForUpdateId(alpha, party, handler.exerciseCreate(nContract))
      } yield result
    }

  }
}
