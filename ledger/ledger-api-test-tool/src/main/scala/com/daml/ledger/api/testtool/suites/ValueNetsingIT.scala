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
  def toNat(i: Long, acc: Nat = Nat.Z(())): Nat =
    if (i == 0) acc else toNat(i - 1, Nat.S(acc))

  @tailrec
  def toLong(n: Nat, acc: Long = 0): Int =
    n match {
      case Nat.Z(_) => 0
      case Nat.S(n) => toLong(n, acc + 1)
    }

  def toEither[X](future: Future[X])(implicit ec: ExecutionContext): Future[Either[Throwable, X]] =
    future.transform(x => Success(x.toEither))

  List[Long](30, 100, 101, 110, 200).foreach { depth =>
    val accepted = depth <= 100
    val result = if (accepted) "Accept" else "Reject"

    def test[T](description: String)(
        update: ExecutionContext => (
            ParticipantTestContext,
            Party,
        ) => Future[Either[Throwable, T]]
    ) =
      super.test(
        s"${result}$description$depth",
        s"${result.toLowerCase}s $description with of $depth",
        allocate(SingleParty),
      )(implicit ec => { case Participants(Participant(alpha, party)) =>
        update(ec)(alpha, party).map {
          case Right(_) if accepted => ()
          case Left(err: Throwable) if !accepted =>
            assertGrpcError(err, Status.Code.INVALID_ARGUMENT, None)
          case otherwise => fail("Unexpected " + otherwise.fold(_ => "success", _ => "failure"))
        }
      })

    test("CreateArgument") { implicit ec => (alpha, party) =>
      toEither(alpha.create(party, Contract(party, depth, toNat(depth - 2))))

    }

    test("ExerciseArgument") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseDestruct(_, toNat(depth - 2))))
      } yield result
    }

    test("ExerciseOutput") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseConstruct(_, depth - 2)))
      } yield result
    }

    test("Create") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseCreate(_, depth - 2)))
      } yield result
    }

    test("CreateKey") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseCreateKey(_, depth - 2)))
      } yield result
    }

    if (accepted)
      test("FetchByKey") { implicit ec => (alpha, party) =>
        for {
          handler <- alpha.create(party, Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(_, depth - 2))
          result <- toEither(alpha.exercise(party, handler.exerciseFetchByKey(_, depth - 2)))
        } yield result
      }

    test("FailingLookupByKey") { implicit ec => (alpha, party) =>
      for {
        handler <- alpha.create(party, Handler(party))
        result <- toEither(alpha.exercise(party, handler.exerciseLookupByKey(_, depth - 2)))
      } yield result
    }

    if (accepted)
      test("SuccessfulLookupByKey") { implicit ec => (alpha, party) =>
        for {
          handler <- alpha.create(party, Handler(party))
          _ <- alpha.exercise(party, handler.exerciseCreateKey(_, depth - 2))
          result <- toEither(alpha.exercise(party, handler.exerciseLookupByKey(_, depth - 2)))
        } yield result
      }

  }
}
