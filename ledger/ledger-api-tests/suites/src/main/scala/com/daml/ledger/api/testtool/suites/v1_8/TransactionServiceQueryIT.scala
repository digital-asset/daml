// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.client.binding
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.model.Test.Dummy._
import com.daml.ledger.test.model.Test._

import scala.concurrent.{ExecutionContext, Future}

class TransactionServiceQueryIT extends LedgerTestSuite {
  test(
    "TXSmokeSubmitAndWaitForTransaction",
    "Smoke the SubmitAndWaitForTransaction",
    allocate(SingleParty),
    timeoutScale = 10.0,
    repeated = 100,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    val parallelism = 64
    val sequentialRuns = 10
    val contractsPerTransaction = 10

    (1 to sequentialRuns)
      .foldLeft(Future.unit) { case (f, idx) =>
        f.flatMap { _ =>
          parallelRun(ledger)(party, parallelism, sequentialRuns, idx, contractsPerTransaction)
        }
      }
  })

  private def parallelRun(ledger: ParticipantTestContext)(
      party: Primitive.Party,
      parallelism: Int,
      sequentialRuns: Int,
      idx: Int,
      contractsPerTransaction: Int,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    println(s"Running sequential batch $idx/$sequentialRuns of size $parallelism")
    val parallelRuns = (1 to parallelism).map { _ =>
      for {
        _ <- Future.unit
        transientTx <- ledger.submitAndWaitForTransaction(
          buildCreateAndExerciseRequest(
            ledger,
            party,
            contractsPerTransaction,
          )
        )

        req = ledger.getTransactionByIdRequest(
          transientTx.transaction.get.transactionId,
          Seq(party),
        )
        _ <- ledger.flatTransactionById(req)
        _ <- ledger.transactionTreeById(req)
      } yield ()
    }

    Future.sequence(parallelRuns).map(_ => ())
  }

  private def buildCreateAndExerciseRequest(
      ledger: ParticipantTestContext,
      party: binding.Primitive.Party,
      contractsPerTransaction: Int,
  ) = {
    val createAndExercise =
      (1 to contractsPerTransaction).map { _ =>
        Dummy(party).createAnd.exerciseClone(party).command
      }
    ledger.submitAndWaitRequest(
      party = party,
      commands = createAndExercise: _*,
    )
  }

  test(
    "TXTransactionTreeByIdBasic",
    "Expose a visible transaction tree by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      byId <- ledger.transactionTreeById(tree.transactionId, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", tree, byId)
    }
  })

  test(
    "TXInvisibleTransactionTreeById",
    "Do not expose an invisible transaction tree by identifier",
    allocate(SingleParty, SingleParty),
  )(implicit ec => { case Participants(Participant(alpha, party), Participant(beta, intruder)) =>
    for {
      dummy <- alpha.create(party, Dummy(party))
      tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
      _ <- synchronize(alpha, beta)
      failure <- beta
        .transactionTreeById(tree.transactionId, intruder)
        .mustFail("subscribing to an invisible transaction tree")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })

  test(
    "TXTransactionTreeByIdNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction tree by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .transactionTreeById("a" * 60, party)
        .mustFail("looking up an non-existent transaction tree")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })

  test(
    "TXFlatTransactionByIdBasic",
    "Expose a visible transaction by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1)
      byId <- ledger.flatTransactionById(transaction.transactionId, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", transaction, byId)
    }
  })

  test(
    "TXInvisibleFlatTransactionById",
    "Do not expose an invisible flat transaction by identifier",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, intruder)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      failure <- ledger
        .flatTransactionById(tree.transactionId, intruder)
        .mustFail("looking up an invisible flat transaction")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })

  test(
    "TXFlatTransactionByIdNotFound",
    "Return NOT_FOUND when looking up a non-existent flat transaction by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .flatTransactionById("a" * 60, party)
        .mustFail("looking up a non-existent flat transaction")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })

  test(
    "TXTransactionTreeByEventIdBasic",
    "Expose a visible transaction tree by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      byId <- ledger.transactionTreeByEventId(tree.rootEventIds.head, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", tree, byId)
    }
  })

  test(
    "TXInvisibleTransactionTreeByEventId",
    "Do not expose an invisible transaction tree by event identifier",
    allocate(SingleParty, SingleParty),
    timeoutScale = 2.0,
  )(implicit ec => { case Participants(Participant(alpha, party), Participant(beta, intruder)) =>
    for {
      dummy <- alpha.create(party, Dummy(party))
      tree <- alpha.exercise(party, dummy.exerciseDummyChoice1)
      _ <- synchronize(alpha, beta)
      failure <- beta
        .transactionTreeByEventId(tree.rootEventIds.head, intruder)
        .mustFail("looking up an invisible transaction tree")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })

  test(
    "TXTransactionTreeByEventIdNotFound",
    "Return NOT_FOUND when looking up a non-existent transaction tree by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .transactionTreeByEventId(s"#${"a" * 60}:000", party)
        .mustFail("looking up a non-existent transaction tree")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })

  test(
    "TXFlatTransactionByEventIdBasic",
    "Expose a visible flat transaction by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1)
      event = transaction.events.head.event
      eventId = event.archived.map(_.eventId).get
      byId <- ledger.flatTransactionByEventId(eventId, party)
    } yield {
      assertEquals("The transaction fetched by identifier does not match", transaction, byId)
    }
  })

  test(
    "TXInvisibleFlatTransactionByEventId",
    "Do not expose an invisible flat transaction by event identifier",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, party, intruder)) =>
    for {
      dummy <- ledger.create(party, Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1)
      failure <- ledger
        .flatTransactionByEventId(tree.rootEventIds.head, intruder)
        .mustFail("looking up an invisible flat transaction")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })

  test(
    "TXFlatTransactionByEventIdNotFound",
    "Return NOT_FOUND when looking up a non-existent flat transaction by event identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      failure <- ledger
        .flatTransactionByEventId(s"#${"a" * 60}:000", party)
        .mustFail("looking up a non-existent flat transaction")
    } yield {
      assertGrpcError(
        failure,
        LedgerApiErrors.RequestValidation.NotFound.Transaction,
        Some("Transaction not found, or not visible."),
      )
    }
  })
}
