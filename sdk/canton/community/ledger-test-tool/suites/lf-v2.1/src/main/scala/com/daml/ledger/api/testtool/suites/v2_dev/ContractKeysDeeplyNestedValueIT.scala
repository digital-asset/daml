// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_dev

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.javaapi.data.codegen.{ContractCompanion, Update}
import com.daml.ledger.test.java.experimental.deeplynestedvalue.Handler as Handler
import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class ContractKeysDeeplyNestedValueIT extends LedgerTestSuite {
  implicit val handlerCompanion
      : ContractCompanion.WithoutKey[Handler.Contract, Handler.ContractId, Handler] =
    Handler.COMPANION

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

    // The nesting of the key of a `ContractWithKey` is one more than the nat it contains
    val nKey = n - 1

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
      "contract key",
      CommandExecutionErrors.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForUpdateId(alpha, party, handler.exerciseCreateKey(nKey))
      } yield result
    }

    if (accepted) {
      // Because we cannot create contracts with nesting > 100,
      // it does not make sense to test fetch of those kinds of contracts.
      test(
        "fetch by key",
        CommandExecutionErrors.Interpreter.ValueNesting,
      ) { implicit ec => (alpha, party) =>
        for {
          handler: Handler.ContractId <- alpha.create(party, new Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(nKey))
          result <- waitForUpdateId(alpha, party, handler.exerciseFetchByKey(nKey))
        } yield result
      }
    }

    test(
      "failing lookup by key",
      CommandExecutionErrors.Interpreter.ValueNesting,
    ) { implicit ec => (alpha, party) =>
      for {
        handler: Handler.ContractId <- alpha.create(party, new Handler(party))
        result <- waitForUpdateId(alpha, party, handler.exerciseLookupByKey(nKey))
      } yield result
    }

    if (accepted) {
      // Because we cannot create contracts with key nesting > 100,
      // it does not make sens to test successful lookup for those keys.
      test(
        "successful lookup by key",
        CommandExecutionErrors.Interpreter.ValueNesting,
      ) { implicit ec => (alpha, party) =>
        for {
          handler: Handler.ContractId <- alpha.create(party, new Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(nKey))
          result <-
            waitForUpdateId(alpha, party, handler.exerciseLookupByKey(nKey))
        } yield result
      }
    }

  }
}
