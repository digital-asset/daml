// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Synchronize.synchronize
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsRequest
import com.daml.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.daml.ledger.api.v1.commands.DisclosedContract
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.{Transaction, TransactionTree}
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.Party
import com.daml.ledger.test.java.model.test._
import com.daml.ledger.test.java.semantic.divulgencetests.DummyFlexibleController

import scala.concurrent.{ExecutionContext, Future}

class TransactionServiceQueryIT extends LedgerTestSuite {
  import CompanionImplicits._

  test(
    "TXTransactionTreeByIdBasic",
    "Expose a visible transaction tree by identifier",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    for {
      dummy <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1())
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
      dummy <- alpha.create(party, new Dummy(party))
      tree <- alpha.exercise(party, dummy.exerciseDummyChoice1())
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
      dummy <- ledger.create(party, new Dummy(party))
      transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1())
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
      dummy <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1())
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
      dummy <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1())
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
      dummy <- alpha.create(party, new Dummy(party))
      tree <- alpha.exercise(party, dummy.exerciseDummyChoice1())
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
      dummy <- ledger.create(party, new Dummy(party))
      transaction <- ledger.exerciseForFlatTransaction(party, dummy.exerciseDummyChoice1())
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
      dummy <- ledger.create(party, new Dummy(party))
      tree <- ledger.exercise(party, dummy.exerciseDummyChoice1())
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

  test(
    "TXFlatTransactionByIdExplicitDisclosure",
    "GetTransactionById returns an empty transaction when an explicitly disclosed contract is exercised",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, owner, stranger)) =>
    testFlatExplicitDisclosure(
      ledger = ledger,
      owner = owner,
      stranger = stranger,
      submitAndWaitRequest = (disclosedContract, contractId) =>
        ledger
          .submitAndWaitRequest(stranger, contractId.exerciseFlexibleConsume(stranger).commands)
          .update(_.commands.disclosedContracts := scala.Seq(disclosedContract)),
      getTransaction = (updateId, stranger) => ledger.flatTransactionById(updateId, stranger),
      getTransactionTree = (updateId, stranger) => ledger.transactionTreeById(updateId, stranger),
    )
  })

  test(
    "TXFlatTransactionByEventIdExplicitDisclosure",
    "GetTransactionByEventId returns an empty transaction when an explicitly disclosed contract is exercised",
    allocate(TwoParties),
  )(implicit ec => { case Participants(Participant(ledger, owner, stranger)) =>
    testFlatExplicitDisclosure(
      ledger = ledger,
      owner = owner,
      stranger = stranger,
      submitAndWaitRequest = (disclosedContract, contractId) =>
        ledger
          .submitAndWaitRequest(stranger, contractId.exerciseFlexibleConsume(stranger).commands)
          .update(_.commands.disclosedContracts := scala.Seq(disclosedContract)),
      getTransaction = (updateId, stranger) =>
        ledger.transactionTreeById(updateId, stranger).flatMap { txTree =>
          ledger.flatTransactionByEventId(txTree.rootEventIds.head, stranger)
        },
      getTransactionTree = (updateId, stranger) => ledger.transactionTreeById(updateId, stranger),
    )
  })

  test(
    "TXFlatTransactionByIdTransientContract",
    "GetTransactionById returns an empty transaction on a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, owner)) =>
    for {
      response <- ledger.submitAndWaitForTransactionId(
        ledger.submitAndWaitRequest(owner, new Dummy(owner).createAnd().exerciseArchive().commands)
      )
      _ <- fetchAndCompareTransactions(
        getTransaction = ledger.flatTransactionById(response.transactionId, owner),
        getTransactionTree = ledger.transactionTreeById(response.transactionId, owner),
      )
    } yield ()
  })

  test(
    "TXFlatTransactionByEventIdTransientContract",
    "GetTransactionById returns an empty transaction on a transient contract",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, owner)) =>
    for {
      response <- ledger.submitAndWaitForTransactionTree(
        ledger.submitAndWaitRequest(owner, new Dummy(owner).createAnd().exerciseArchive().commands)
      )
      eventId = response.transaction.get.rootEventIds.head
      _ <- fetchAndCompareTransactions(
        getTransaction = ledger.flatTransactionByEventId(eventId, owner),
        getTransactionTree = ledger.transactionTreeByEventId(eventId, owner),
      )
    } yield ()
  })

  test(
    "TXFlatTransactionByIdNonConsumingChoice",
    "GetTransactionById returns an empty transaction when command contains only a nonconsuming choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, owner)) =>
    for {
      contractId: Dummy.ContractId <- ledger.create(owner, new Dummy(owner))
      response <- ledger.submitAndWaitForTransactionId(
        ledger.submitAndWaitRequest(owner, contractId.exerciseDummyNonConsuming().commands)
      )
      _ <- fetchAndCompareTransactions(
        getTransaction = ledger.flatTransactionById(response.transactionId, owner),
        getTransactionTree = ledger.transactionTreeById(response.transactionId, owner),
      )
    } yield ()
  })

  test(
    "TXFlatTransactionByEventIdNonConsumingChoice",
    "GetTransactionById returns an empty transaction when command contains only a nonconsuming choice",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, owner)) =>
    for {
      contractId: Dummy.ContractId <- ledger.create(owner, new Dummy(owner))
      response <- ledger.submitAndWaitForTransactionTree(
        ledger.submitAndWaitRequest(owner, contractId.exerciseDummyNonConsuming().commands)
      )
      eventId = response.transaction.get.rootEventIds.head
      _ <- fetchAndCompareTransactions(
        getTransaction = ledger.flatTransactionByEventId(eventId, owner),
        getTransactionTree = ledger.transactionTreeByEventId(eventId, owner),
      )
    } yield ()
  })

  private def testFlatExplicitDisclosure(
      ledger: ParticipantTestContext,
      owner: Party,
      stranger: Party,
      submitAndWaitRequest: (
          DisclosedContract,
          DummyFlexibleController.ContractId,
      ) => SubmitAndWaitRequest,
      getTransaction: (String, Party) => Future[Transaction],
      getTransactionTree: (String, Party) => Future[TransactionTree],
  )(implicit ec: ExecutionContext): Future[Unit] = for {
    contractId: DummyFlexibleController.ContractId <- ledger.create(
      owner,
      new DummyFlexibleController(owner),
    )
    ownerAcs <- ledger.activeContracts(
      new GetActiveContractsRequest(
        filter = Some(filterByPartyAndTemplate(owner, DummyFlexibleController.TEMPLATE_ID))
      )
    )
    createdEvent = assertSingleton("Owners' created event", ownerAcs._2)
    disclosedContract = createEventToDisclosedContract(createdEvent)
    submitResponse <- ledger.submitAndWaitForTransactionId(
      submitAndWaitRequest(disclosedContract, contractId)
    )
    _ <- fetchAndCompareTransactions(
      getTransaction = getTransaction(submitResponse.transactionId, stranger),
      getTransactionTree = getTransactionTree(submitResponse.transactionId, stranger),
    )
  } yield ()

  private def fetchAndCompareTransactions(
      getTransaction: => Future[Transaction],
      getTransactionTree: => Future[TransactionTree],
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      flatTransaction <- getTransaction
      transactionTree <- getTransactionTree
    } yield assertEquals(
      "The flat transaction should contain the same details (except events) as the transaction tree",
      flatTransaction,
      Transaction(
        transactionId = transactionTree.transactionId,
        commandId = transactionTree.commandId,
        workflowId = transactionTree.workflowId,
        effectiveAt = transactionTree.effectiveAt,
        events = Seq.empty,
        offset = transactionTree.offset,
        traceContext = transactionTree.traceContext,
      ),
    )

  private def filterByPartyAndTemplate(
      owner: Party,
      templateId: javaapi.data.Identifier,
  ): TransactionFilter = {
    val templateIdScalaPB = Identifier.fromJavaProto(templateId.toProto)

    new TransactionFilter(
      Map(
        owner.getValue -> new Filters(
          Some(
            InclusiveFilters(templateFilters =
              Seq(TemplateFilter(Some(templateIdScalaPB), includeCreatedEventBlob = true))
            )
          )
        )
      )
    )
  }

  private def createEventToDisclosedContract(ev: CreatedEvent): DisclosedContract =
    DisclosedContract(
      templateId = ev.templateId,
      contractId = ev.contractId,
      createdEventBlob = ev.createdEventBlob,
    )
}
