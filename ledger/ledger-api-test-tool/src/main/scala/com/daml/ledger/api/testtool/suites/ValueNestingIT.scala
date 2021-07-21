// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.test.semantic.ValueNesting._
import io.grpc.Status

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class ValueNestingIT extends LedgerTestSuite {

  @tailrec
  private[this] def toNat(i: Long, acc: Nat = Nat.Z(())): Nat =
    if (i == 0) acc else toNat(i - 1, Nat.S(acc))

  private[this] def toEither[X](future: Future[X])(implicit
      ec: ExecutionContext
  ): Future[Either[Throwable, X]] =
    future.transform(x => Success(x.toEither))

  private[this] def camlCase(s: String) =
    s.split(" ").iterator.map(_.capitalize).mkString("")

  List[Long](30, 100, 101, 110, 200).foreach { depth =>
    val accepted = depth <= 100
    val result = if (accepted) "Accept" else "Reject"

    // Once converted to Nat, `n` will have depth `depth`.
    // Note that Nat.Z(()) has depth 2.
    val n = depth - 2

    // Choice argument are always wrapped in a record
    val nChoiceArgument = n - 1

    // The depth of the payload of a `Contract` is one more than the nat it contains
    val nContract = n - 1

    // The depth of the key of a `ContractWithKey` is one more than the nat it contains
    val nKey = n - 1

    def test[T](description: String)(
        update: ExecutionContext => (
            ParticipantTestContext,
            Party,
        ) => Future[Either[Throwable, T]]
    ) =
      super.test(
        result + camlCase(description) + depth.toString,
        s"${result.toLowerCase}s $description with depth of $depth",
        allocate(SingleParty),
      )(implicit ec => { case Participants(Participant(alpha, party)) =>
        update(ec)(alpha, party).map {
          case Right(_) if accepted => ()
          case Left(err: Throwable) if !accepted =>
            assertGrpcError(err, Status.Code.INVALID_ARGUMENT, None)
          case otherwise =>
            fail("Unexpected " + otherwise.fold(err => s"failure: $err", _ => "success"))
        }
      })

    test("create command") { implicit ec => (alpha, party) =>
      toEither(alpha.create(party, Contract(party, nContract, toNat(nContract))))
    }

    test("exercise command") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(
          alpha.exercise(party, handler.exerciseDestruct(_, toNat(nChoiceArgument)))
        )
      } yield result
    }

    test("create argument in CreateAndExercise command") { implicit ec => (alpha, party) =>
      toEither(
        alpha
          .submitAndWaitForTransactionTree(
            alpha
              .submitAndWaitRequest(
                party,
                Contract(party, nContract, toNat(nContract)).createAnd
                  .exerciseArchive(party)
                  .command,
              )
          )
      )
    }

    test("choice argument in CreateAndExercise command") { implicit ec => (alpha, party) =>
      toEither(
        alpha
          .submitAndWaitForTransactionTree(
            alpha
              .submitAndWaitRequest(
                party,
                Handler(party).createAnd.exerciseDestruct(party, toNat(nChoiceArgument)).command,
              )
          )
      )
    }

    test("exercise argument") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(
          alpha.exercise(party, handler.exerciseConstructThenDestruct(_, nChoiceArgument))
        )
      } yield result
    }

    test("exercise output") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseConstruct(_, n)))
      } yield result
    }

    test("create argument") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseCreate(_, nContract)))
      } yield result
    }

    test("contract key") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseCreateKey(_, nKey)))
      } yield result
    }

    if (accepted) {
      // Because we cannot create contracts with depth > 100,
      // it does not make sense to test fetch of those kinds of contracts.
      test("fetch by key") { implicit ec => (alpha, party) =>
        for {
          handler <- alpha.create(party, Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(_, nKey))
          result <- toEither(alpha.exercise(party, handler.exerciseFetchByKey(_, nKey)))
        } yield result
      }
    }

    test("failing lookup by key") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseLookupByKey(_, nKey)))
      } yield result
    }

    if (accepted) {
      // Because we cannot create contracts with key depth > 100,
      // it does not make sens to test successful lookup of key with depth > 100.
      test("successful lookup by key") { implicit ec => (alpha, party) =>
        for {
          handler <- alpha.create(party, Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(_, nKey))
          result <- toEither(alpha.exercise(party, handler.exerciseLookupByKey(_, nKey)))
        } yield result
      }
    }

  }
}
