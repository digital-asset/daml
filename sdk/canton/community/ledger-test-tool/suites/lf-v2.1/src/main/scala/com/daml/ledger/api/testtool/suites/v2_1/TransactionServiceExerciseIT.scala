// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.TransactionOps.*
import com.daml.ledger.test.java.model.test.{
  AgreementFactory,
  CreateAndFetch,
  Dummy,
  DummyFactory,
  DummyWithParam,
}
import com.digitalasset.base.error.{ErrorCategory, ErrorCode}
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.api.util.TimestampConversion
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.platform.store.utils.EventOps.EventOps

class TransactionServiceExerciseIT extends LedgerTestSuite {
  import CompanionImplicits.*

  test(
    "TXUseCreateToExercise",
    "Should be able to directly use a contract identifier to exercise a choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummyFactory <- ledger.create(party, new DummyFactory(party))
      transactions <- ledger.exercise(party, dummyFactory.exerciseDummyFactoryCall())
    } yield {
      val events = transactions.events.filter(e => transactions.rootNodeIds().contains(e.nodeId))
      val exercised = events.filter(_.event.isExercised)
      assert(exercised.sizeIs == 1, s"Only one exercise expected, got ${exercised.size}")
      assert(
        exercised.headOption.value.getExercised.contractId == dummyFactory.contractId,
        s"The identifier of the exercised contract should have been ${dummyFactory.contractId} but instead it was ${exercised.headOption.value.getExercised.contractId}",
      )
    }
  })

  test(
    "TXContractIdFromExerciseWhenFilter",
    "Expose contract identifiers that are results of exercising choices when filtering by template",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      factory <- ledger.create(party, new DummyFactory(party))
      _ <- ledger.exercise(party, factory.exerciseDummyFactoryCall())
      dummyWithParam <- ledger.transactionsByTemplateId(
        DummyWithParam.TEMPLATE_ID,
        Some(Seq(party)),
      )
      dummyFactory <- ledger.transactionsByTemplateId(
        DummyFactory.TEMPLATE_ID,
        Some(Seq(party)),
      )
    } yield {
      val create = assertSingleton("GetCreate", dummyWithParam.flatMap(createdEvents))
      assertEquals(
        "Create should be of DummyWithParam",
        create.getTemplateId,
        DummyWithParam.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      )
      val archive = assertSingleton("GetArchive", dummyFactory.flatMap(archivedEvents))
      assertEquals(
        "Archive should be of DummyFactory",
        archive.getTemplateId,
        DummyFactory.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      )
      assertEquals(
        "Mismatching archived contract identifier",
        archive.contractId,
        factory.contractId,
      )
    }
  })

  test(
    "TXNotArchiveNonConsuming",
    "Expressing a non-consuming choice on a contract should not result in its archival",
    allocate(SingleParty, SingleParty),
  )(implicit ec => {
    case p @ Participants(Participant(alpha, Seq(receiver)), Participant(beta, Seq(giver))) =>
      for {
        agreementFactory: AgreementFactory.ContractId <- beta.create(
          giver,
          new AgreementFactory(receiver, giver),
        )
        _ <- p.synchronize
        _ <- alpha.exercise(receiver, agreementFactory.exerciseCreateAgreement())
        transactions <- alpha.transactions(AcsDelta, receiver, giver)
      } yield {
        assert(
          !transactions.exists(_.events.exists(_.event.isArchived)),
          s"The transaction include an archival: ${transactions.flatMap(_.events).filter(_.event.isArchived)}",
        )
      }
  })

  test(
    "TXFetchContractCreatedInTransaction",
    "It should be possible to fetch a contract created within a transaction",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      createAndFetch: CreateAndFetch.ContractId <- ledger.create(party, new CreateAndFetch(party))(
        CreateAndFetch.COMPANION
      )
      transaction <- ledger.exercise(
        party,
        createAndFetch.exerciseCreateAndFetch_Run(),
        AcsDelta,
      )
    } yield {
      val _ = assertSingleton("There should be only one create", createdEvents(transaction))
      val exercise =
        assertSingleton("There should be only one archive", archivedEvents(transaction))
      assertEquals(
        "The contract identifier of the exercise does not match",
        createAndFetch.contractId,
        exercise.contractId,
      )
    }
  })

  test(
    "TXRejectOnFailingAssertion",
    "Reject a transaction on a failing assertion",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    for {
      dummy: Dummy.ContractId <- ledger.create(party, new Dummy(party))
      failure <- ledger
        .exercise(
          party,
          dummy
            .exerciseConsumeIfTimeIsBetween(TimestampConversion.MAX, TimestampConversion.MAX),
        )
        .mustFail("exercising with a failing assertion")
    } yield {
      assertGrpcError(
        failure,
        new ErrorCode(
          CommandExecutionErrors.Interpreter.FailureStatus.id,
          ErrorCategory.InvalidGivenCurrentSystemStateOther,
        )(
          CommandExecutionErrors.Interpreter.FailureStatus.parent
        ) {},
        Some("Assertion failed"),
        checkDefiniteAnswerMetadata = true,
      )
    }
  })
}
