// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.*
import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.FutureAssertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v2.event.Event.Event
import com.daml.ledger.api.v2.event.{CreatedEvent, InterfaceView}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{EventFormat, TransactionFormat}
import com.daml.ledger.api.v2.value.{Identifier, Record}
import com.daml.ledger.javaapi
import com.daml.ledger.javaapi.data.codegen.ContractCompanion
import com.daml.ledger.test.java.semantic.interfaceviews.{
  I,
  I2,
  INoTemplate,
  T1,
  T2,
  T3,
  T4,
  T5,
  T6,
}
import com.daml.ledger.test.java.{carbonv1, carbonv2}
import com.daml.logging.LoggingContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors

import java.util.regex.Pattern
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class InterfaceSubscriptionsIT extends InterfaceSubscriptionsITBase("IS")
class InterfaceSubscriptionsWithEventBlobsIT extends InterfaceSubscriptionsITBase("ISWP")

abstract class InterfaceSubscriptionsITBase(prefix: String) extends LedgerTestSuite {
  private def archive(ledger: ParticipantTestContext, party: Party)(
      c1: T1.ContractId,
      c2: T2.ContractId,
      c3: T3.ContractId,
      c4: T4.ContractId,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- ledger.exercise(party, c1.exerciseArchive())
      _ <- ledger.exercise(party, c2.exerciseArchive())
      _ <- ledger.exercise(party, c3.exerciseArchive())
      _ <- ledger.exercise(party, c4.exerciseArchive())
    } yield ()

  implicit val t1Companion: ContractCompanion.WithoutKey[T1.Contract, T1.ContractId, T1] =
    T1.COMPANION
  implicit val t2Companion: ContractCompanion.WithoutKey[T2.Contract, T2.ContractId, T2] =
    T2.COMPANION
  implicit val t3Companion: ContractCompanion.WithoutKey[T3.Contract, T3.ContractId, T3] =
    T3.COMPANION
  implicit val t4Companion: ContractCompanion.WithoutKey[T4.Contract, T4.ContractId, T4] =
    T4.COMPANION
  implicit val t5Companion: ContractCompanion.WithoutKey[T5.Contract, T5.ContractId, T5] =
    T5.COMPANION
  implicit val t6Companion: ContractCompanion.WithoutKey[T6.Contract, T6.ContractId, T6] =
    T6.COMPANION

  test(
    s"${prefix}TransactionsBasic",
    "Basic functionality for interface subscriptions on transaction streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T1(party, 1))
      c2 <- create(party, new T2(party, 2))
      c3 <- create(party, new T3(party, 3))
      c4 <- create(party, new T4(party, 4))
      _ <- archive(ledger, party)(c1, c2, c3, c4)
      txReq <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq(T1.TEMPLATE_ID),
          Seq((I.TEMPLATE_ID, true)),
          verbose = true,
        )
      )
      txs <- transactions(txReq)
      txReqWithoutInterfaceViews <- getTransactionsRequest(
        transactionFormat(
          parties = Some(Seq(party)),
          templateIds = Seq(T1.TEMPLATE_ID),
          interfaceFilters = Seq((I.TEMPLATE_ID, false)),
        )
      )
      txsWithoutInterfaceViews <- transactions(txReqWithoutInterfaceViews)
      created = txs.flatMap(createdEvents)
      exercisedImplementedInterfaces = Some(
        txs.flatMap(archivedEvents).map(_.implementedInterfaces)
      )
    } yield {
      basicAssertions(
        created,
        exercisedImplementedInterfaces,
        c1.contractId,
        c2.contractId,
        c3.contractId,
      )
      assertEquals(
        "If no inclusion of interface views requested, then we should have the create events, but no views rendered",
        txsWithoutInterfaceViews.flatMap(createdEvents).map(_.interfaceViews),
        Seq(Nil, Nil, Nil),
      )
      assertEquals(
        "If no inclusion of interface views requested, then we should have the archive events, but no implemented_interfaces rendered",
        txsWithoutInterfaceViews.flatMap(archivedEvents).map(_.implementedInterfaces),
        Seq(Nil, Nil, Nil),
      )
    }
  })

  test(
    s"${prefix}TransactionLedgerEffectsBasic",
    "Basic functionality for interface subscriptions on ledger effects transaction streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T1(party, 1))
      c2 <- create(party, new T2(party, 2))
      c3 <- create(party, new T3(party, 3))
      c4 <- create(party, new T4(party, 4))
      _ <- ledger.exercise(party, c2.exerciseChoiceT2())
      _ <- ledger.exercise(party, c2.toInterface(I.INTERFACE).exerciseChoiceI())
      _ <- archive(ledger, party)(c1, c2, c3, c4)
      txReq <- getTransactionsRequest(
        transactionFormat(
          parties = Some(Seq(party)),
          templateIds = Seq(T1.TEMPLATE_ID),
          interfaceFilters = Seq((I.TEMPLATE_ID, true)),
          transactionShape = LedgerEffects,
          verbose = true,
        )
      )
      txs <- transactions(txReq)
      txReqWithoutInterfaceViews <- getTransactionsRequest(
        transactionFormat(
          parties = Some(Seq(party)),
          templateIds = Seq(T1.TEMPLATE_ID),
          interfaceFilters = Seq((I.TEMPLATE_ID, false)),
          transactionShape = LedgerEffects,
        )
      )
      txsWithoutInterfaceViews <- transactions(txReqWithoutInterfaceViews)
      created = txs.flatMap(createdEvents)
      exercisedImplementedInterfaces = Some(
        txs.flatMap(exercisedEvents).map(_.implementedInterfaces)
      )
    } yield {
      basicAssertions(
        created,
        exercisedImplementedInterfaces,
        c1.contractId,
        c2.contractId,
        c3.contractId,
        exercisedEvents = true,
      )
      assertEquals(
        "If no inclusion of interface views requested, then we should have the create events, but no views rendered",
        txsWithoutInterfaceViews.flatMap(createdEvents).map(_.interfaceViews),
        Seq(Nil, Nil, Nil),
      )
      assertEquals(
        "If no inclusion of interface views requested, then we should have the exercise events, but no implemented_interfaces rendered",
        txsWithoutInterfaceViews.flatMap(exercisedEvents).map(_.implementedInterfaces),
        Seq(Nil, Nil, Nil, Nil, Nil),
      )
    }
  })

  test(
    s"${prefix}AcsBasic",
    "Basic functionality for interface subscriptions on ACS streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T1(party, 1))
      c2 <- create(party, new T2(party, 2))
      c3 <- create(party, new T3(party, 3))
      c4 <- create(party, new T4(party, 4))
      end <- ledger.currentEnd()
      createdEvents <- activeContracts(
        activeContractsRequest(
          parties = Some(Seq(party)),
          templateIds = Seq(T1.TEMPLATE_ID),
          interfaceFilters = Seq((I.TEMPLATE_ID, true)),
          activeAtOffset = end,
        )
      )
      // archive to avoid interference with subsequent tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield basicAssertions(createdEvents, None, c1.contractId, c2.contractId, c3.contractId)
  })

  private def basicAssertions(
      createdEvents: Vector[CreatedEvent],
      exercisedImplementedInterfacesO: Option[Vector[Seq[Identifier]]],
      c1: String,
      c2: String,
      c3: String,
      exercisedEvents: Boolean = false,
  ): Unit = {

    def checkArgumentsNonEmpty(event: CreatedEvent, id: Int): Unit =
      assertEquals(
        s"Create event $id createArguments must NOT be empty",
        event.createArguments.isEmpty,
        false,
      )

    val transactionNum = 3
    val (transactionNumExercised, archivedEventIndexF) =
      if (exercisedEvents) (5, (i: Int) => i + 2)
      else (3, (i: Int) => i)

    assertLength(
      s"$transactionNum created transactions found",
      transactionNum,
      createdEvents,
    ).discard
    exercisedImplementedInterfacesO.foreach(exercisedImplementedInterfaces =>
      assertLength(
        s"$transactionNumExercised exercised transactions should be found",
        transactionNumExercised,
        exercisedImplementedInterfaces,
      ).discard
    )

    // T1
    val createdEvent1 = createdEvents(0)
    assertLength("Create event 1 has a view", 1, createdEvent1.interfaceViews).discard
    assertEquals(
      "Create event 1 template ID",
      createdEvent1.templateId.get.toString,
      T1.TEMPLATE_ID_WITH_PACKAGE_ID.toV1.toString,
    )
    assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1)
    assertViewEquals(createdEvent1.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
      assertLength("View1 has 2 fields", 2, value.fields).discard
      assertEquals("View1.a", value.fields(0).getValue.getInt64, 1)
      assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      assert(
        value.fields.forall(_.label.nonEmpty),
        "Expected a view with labels (verbose)",
      )
    }
    checkArgumentsNonEmpty(createdEvent1, id = 1)
    assert(
      createdEvent1.getCreateArguments.fields.forall(_.label.nonEmpty),
      "Expected a contract with labels (verbose)",
    )
    exercisedImplementedInterfacesO.foreach(exercisedImplementedInterfaces =>
      assertEquals(
        "Archive event 1 has correct implemented_interfaces",
        exercisedImplementedInterfaces(archivedEventIndexF(0)),
        Seq(I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1),
      )
    )

    // T2
    val createdEvent2 = createdEvents(1)
    assertLength("Create event 2 has a view", 1, createdEvent2.interfaceViews).discard
    assertEquals(
      "Create event 2 template ID",
      createdEvent2.templateId.get.toString,
      T2.TEMPLATE_ID_WITH_PACKAGE_ID.toV1.toString,
    )
    assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2)
    assertViewEquals(createdEvent2.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
      assertLength("View2 has 2 fields", 2, value.fields).discard
      assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
      assertEquals("View2.b", value.fields(1).getValue.getBool, false)
    }
    checkArgumentsNonEmpty(createdEvent2, id = 2)
    exercisedImplementedInterfacesO.foreach(exercisedImplementedInterfaces =>
      assertEquals(
        "Archive event 2 has correct implemented_interfaces",
        exercisedImplementedInterfaces(archivedEventIndexF(1)),
        Seq(I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1),
      )
    )

    // T3
    val createdEvent3 = createdEvents(2)
    assertLength("Create event 3 has a view", 1, createdEvent3.interfaceViews).discard
    assertEquals(
      "Create event 3 template ID",
      createdEvent3.templateId.get.toString,
      T3.TEMPLATE_ID_WITH_PACKAGE_ID.toV1.toString,
    )
    assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3)
    assertViewFailed(createdEvent3.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1)
    checkArgumentsNonEmpty(createdEvent3, id = 3)
    exercisedImplementedInterfacesO.foreach(exercisedImplementedInterfaces =>
      assertEquals(
        "Archive event 3 has correct implemented_interfaces",
        exercisedImplementedInterfaces(archivedEventIndexF(2)),
        Seq(I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1),
      )
    )

  }

  test(
    s"${prefix}MultipleWitness",
    "Multiple witness",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party1, party2))) =>
    import ledger.*
    for {
      c <- create(party1, new T6(party1, party2))
      txReq <- getTransactionsRequest(
        TransactionFormat(
          eventFormat = Some(
            EventFormat(
              filtersByParty = Map(
                party1.getValue -> filters(
                  Seq.empty,
                  Seq((I.TEMPLATE_ID, true)),
                ),
                party2.getValue -> filters(
                  Seq.empty,
                  Seq((I2.TEMPLATE_ID, true)),
                ),
              ),
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        )
      )
      mergedTransactions <- transactions(txReq)

      txReq1 <- getTransactionsRequest(
        transactionFormat(Some(Seq(party1)), Seq.empty, Seq((I.TEMPLATE_ID, true)))
      )
      party1Transactions <- transactions(txReq1)
      // archive active contract to avoid interference with subsequent tests
      _ <- ledger.exercise(party1, c.exerciseArchive())
    } yield {
      assertLength("single transaction found", 1, mergedTransactions).discard
      val createdEvent1 = createdEvents(mergedTransactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c.contractId)
      assertViewEquals(createdEvent1.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
        assertLength("View1 has 2 fields", 2, value.fields).discard
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 6)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
      assertViewEquals(createdEvent1.interfaceViews, I2.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
        assertLength("View2 has 1 field", 1, value.fields).discard
        assertEquals("View2.c", value.fields(0).getValue.getInt64, 7)
      }

      assertLength("single transaction found", 1, party1Transactions).discard
      val createdEvent2 = createdEvents(party1Transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent2.contractId, c.contractId)
      assertLength("single view found", 1, createdEvent2.interfaceViews).discard
      assertViewEquals(createdEvent2.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
        assertLength("View1 has 2 fields", 2, value.fields).discard
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 6)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
    }
  })

  test(
    s"${prefix}MultipleViews",
    "Multiple interface views populated for one event",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c <- create(party, new T5(party, 31337))
      _ <- ledger.exercise(party, c.exerciseArchive())
      txReq <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq.empty,
          Seq((I.TEMPLATE_ID, true), (I2.TEMPLATE_ID, true)),
        )
      )
      transactions <- transactions(txReq)
    } yield {
      assertLength("Two transactions found", 2, transactions).discard
      val createdEvent = createdEvents(transactions(0)).head
      val archivedEvent = archivedEvents(transactions(1)).head
      assertEquals("Create event with correct contract ID", createdEvent.contractId, c.contractId)
      assertViewEquals(createdEvent.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
        assertLength("View1 has 2 fields", 2, value.fields).discard
        assertEquals("View1.a", value.fields(0).getValue.getInt64, 31337)
        assertEquals("View1.b", value.fields(1).getValue.getBool, true)
      }
      assertViewEquals(createdEvent.interfaceViews, I2.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
        assertLength("View2 has 1 field", 1, value.fields).discard
        assertEquals("View2.c", value.fields(0).getValue.getInt64, 1)
      }
      assertEquals(
        "Archived event with correct contract ID",
        archivedEvent.contractId,
        c.contractId,
      )
      assertEquals(
        "Archive event has both implemented_interfaces",
        archivedEvent.implementedInterfaces.toSet,
        Set(
          I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
          I2.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
        ),
      )
    }
  })

  test(
    s"${prefix}TransactionsIrrelevantTransactions",
    "Subscribing on transaction stream by interface with no relevant transactions",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      _ <- create(party, new T4(party, 4))
      txReq <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq.empty,
          Seq((INoTemplate.TEMPLATE_ID, true)),
        )
      )
      transactions <- transactions(txReq)
    } yield {
      assertLength("0 transactions should be found", 0, transactions).discard
      ()
    }
  })

  test(
    s"${prefix}TransactionsDuplicateInterfaceFilters",
    "Subscribing on transaction stream by interface with duplicate filters and not verbose",
    allocate(SingleParty),
    limitation = TestConstraints.GrpcOnly("Labels are always emitted by Transcode/SchemaProcessor"),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T1(party, 1))
      c2 <- create(party, new T2(party, 2))
      c3 <- create(party, new T3(party, 3))
      c4 <- create(party, new T4(party, 4))
      txReq <- getTransactionsRequest(
        transactionFormat(
          parties = Some(Seq(party)),
          templateIds = Seq(T1.TEMPLATE_ID),
          interfaceFilters = Seq((I.TEMPLATE_ID, false), (I.TEMPLATE_ID, true)),
        )
      ).map(_.update(_.updateFormat.includeTransactions.eventFormat.verbose := false))
      transactions <- transactions(txReq)
      // archive to avoid interference with subsequent tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.contractId)
      val createdEvent2 = createdEvents(transactions(1)).head
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.contractId)
      // Expect view to be delivered even though there is an ambiguous
      // includeInterfaceView flag set to true and false at the same time (true wins)
      assertViewEquals(createdEvent2.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
        assertLength("View2 has 2 fields", 2, value.fields).discard
        assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
        assertEquals("View2.b", value.fields(1).getValue.getBool, false)
        assert(
          value.fields.forall(_.label.isEmpty),
          s"Expected a view with no labels (verbose = false)",
        )
      }
      val createdEvent3 = createdEvents(transactions(2)).head
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.contractId)
    }
  })

  test(
    s"${prefix}TransactionsDuplicateTemplateFilters",
    "Subscribing on transaction stream by template with duplicate filters",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T1(party, 1))
      c2 <- create(party, new T2(party, 2))
      c3 <- create(party, new T3(party, 3))
      c4 <- create(party, new T4(party, 4))
      txReq <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq(T1.TEMPLATE_ID, T1.TEMPLATE_ID),
          Seq((I.TEMPLATE_ID, true)),
        )
      )
      transactions <- transactions(txReq)
      // archive to avoid interference with subsequent tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.contractId)
      assertEquals(
        "Create event 1 createArguments must NOT be empty",
        createdEvent1.createArguments.isEmpty,
        false,
      )
      val createdEvent2 = createdEvents(transactions(1)).head
      assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2.contractId)
      // Expect view to be delivered even though there is an ambiguous
      // includeInterfaceView flag set to true and false at the same time.
      assertViewEquals(createdEvent2.interfaceViews, I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1) { value =>
        assertLength("View2 has 2 fields", 2, value.fields).discard
        assertEquals("View2.a", value.fields(0).getValue.getInt64, 2)
        assertEquals("View2.b", value.fields(1).getValue.getBool, false)
      }
      val createdEvent3 = createdEvents(transactions(2)).head
      assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3.contractId)
    }
  })

  test(
    s"${prefix}TransactionsNoIncludedView",
    "Subscribing on transaction stream by interface or template without included views",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T1(party, 1))
      c2 <- create(party, new T2(party, 2))
      c3 <- create(party, new T3(party, 3))
      c4 <- create(party, new T4(party, 4))
      txReq <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq(T1.TEMPLATE_ID),
          Seq((I.TEMPLATE_ID, false)),
        )
      )
      transactions <- transactions(txReq)
      // archive to avoid interference with subsequent tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      assertLength("3 transactions found", 3, transactions).discard
      val interfaceViewCount: Int =
        transactions.flatMap(createdEvents).map(_.interfaceViews.size).sum
      assertEquals("No views have been computed and produced", 0, interfaceViewCount)
      val createArgumentsCount: Int =
        transactions.flatMap(createdEvents).map(_.createArguments.isDefined).count(_ == true)
      assertEquals("All 3 create arguments must be delivered", 3, createArgumentsCount)

      // T1
      val createdEvent1 = createdEvents(transactions(0)).head
      assertEquals(
        "Create event 1 template ID",
        createdEvent1.templateId.get.toString,
        T1.TEMPLATE_ID_WITH_PACKAGE_ID.toV1.toString,
      )
      assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1.contractId)
    }
  })

  test(
    s"${prefix}TransactionsEquivalentFilters",
    "Subscribing by interface or all implementing templates gives the same result",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    val allImplementations = Seq(T1.TEMPLATE_ID, T2.TEMPLATE_ID, T3.TEMPLATE_ID)
    for {
      c1 <- create(party, new T1(party, 1))
      c2 <- create(party, new T2(party, 2))
      c3 <- create(party, new T3(party, 3))
      c4 <- create(party, new T4(party, 4))
      // 1. Subscribe by the interface
      txReq1 <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq.empty,
          Seq((I.TEMPLATE_ID, true)),
        )
      )
      transactions1 <- transactions(txReq1)
      // 2. Subscribe by all implementing templates
      txReq2 <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          allImplementations,
          Seq.empty,
        )
      )
      transactions2 <- transactions(txReq2)
      // 3. Subscribe by both the interface and all templates (redundant filters)
      txReq3 <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          allImplementations,
          Seq((I.TEMPLATE_ID, true)),
        )
      )
      transactions3 <- transactions(txReq3)
      // archive to avoid interference with subsequent tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      assertLength("3 transactions found", 3, transactions1).discard
      assertEquals(
        "1 and 2 find the same transactions (but not the same views)",
        transactions1.map(_.updateId),
        transactions2.map(_.updateId),
      )
      assertEquals(
        "2 and 3 find the same contract_arguments (but not the same views)",
        transactions2.map(updateTransaction()),
        transactions3.map(updateTransaction(emptyView = true)),
      )
      assertEquals(
        "1 and 3 produce the same views (but not the same create arguments)",
        // do not check on details since tid is contained and it is expected to be different
        transactions1
          .map(updateTransaction(emptyDetails = true))
          .map(hideTraceIdFromStatusMessages),
        transactions3
          .map(
            updateTransaction(
              emptyContractKey = true,
              emptyDetails = true,
            )
          )
          .map(hideTraceIdFromStatusMessages),
      )
    }
  })

  test(
    s"${prefix}TransactionsUnknownTemplateOrInterface",
    "Subscribing on transaction stream by an unknown template fails",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val packageName = I.TEMPLATE_ID.getPackageId
    val moduleName = I.TEMPLATE_ID.getModuleName
    val unknownTemplate =
      new javaapi.data.Identifier(packageName, moduleName, "TemplateDoesNotExist")
    val unknownInterface =
      new javaapi.data.Identifier(packageName, moduleName, "InterfaceDoesNotExist")
    import ledger.*
    for {
      _ <- create(party, new T1(party, 1))
      txReq <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq(unknownTemplate),
          Seq.empty,
        )
      )
      failure <- transactions(txReq)
        .mustFail("subscribing with an unknown template")
      txReq2 <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq.empty,
          Seq((unknownInterface, true)),
        )
      )
      failure2 <- transactions(txReq2)
        .mustFail("subscribing with an unknown interface")
    } yield {
      assertGrpcErrorRegex(
        failure,
        RequestValidationErrors.NotFound.NoTemplatesForPackageNameAndQualifiedName,
        Some(
          Pattern.compile(
            "The following package-name/template qualified-name pairs do not reference any template-id uploaded on this participant.*TemplateDoesNotExist"
          )
        ),
      )
      assertGrpcErrorRegex(
        failure2,
        RequestValidationErrors.NotFound.NoInterfaceForPackageNameAndQualifiedName,
        Some(
          Pattern.compile(
            "The following package-name/interface qualified-name pairs do not reference any interface-id uploaded on this participant.*InterfaceDoesNotExist"
          )
        ),
      )
    }
  })

  test(
    s"${prefix}TransactionsMultipleParties",
    "Subscribing on transaction stream by multiple parties",
    allocate(Parties(2)),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party1, party2))) =>
    import ledger.*
    for {
      c <- create(party1, new T6(party1, party2))
      txReqWithView <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party1)),
          Seq.empty,
          Seq((I.TEMPLATE_ID, true)),
        )
      )
      party1Iface1transactionsWithView <- transactions(txReqWithView)
      txReqWithoutView <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party1)),
          Seq.empty,
          Seq((I.TEMPLATE_ID, false)),
        )
      )
      party1Iface1transactionsWithoutView <- transactions(txReqWithoutView)
      txReqWithView2 <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party2)),
          Seq.empty,
          Seq((I.TEMPLATE_ID, true)),
        )
      )
      party2Iface1transactionsWithView <- transactions(txReqWithView2)
      txReqWithoutView2 <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party2)),
          Seq.empty,
          Seq((I.TEMPLATE_ID, false)),
        )
      )
      party2Iface1transactionsWithoutView <- transactions(txReqWithoutView2)
      // archive active contract to avoid interference with subsequent tests
      _ <- ledger.exercise(party1, c.exerciseArchive())
    } yield {
      assertEquals(
        party1Iface1transactionsWithView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
        party2Iface1transactionsWithView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
      )

      assertEquals(
        party1Iface1transactionsWithoutView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
        party2Iface1transactionsWithoutView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
      )

      assertEquals(
        party1Iface1transactionsWithView.map(
          updateTransaction(emptyView = true, emptyWitness = true)
        ),
        party2Iface1transactionsWithoutView.map(
          updateTransaction(emptyView = false, emptyWitness = true)
        ),
      )
    }
  })

  test(
    s"${prefix}TransactionsSubscribeBeforeTemplateCreated",
    "Subscribing on transaction stream by interface before template is created",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*

    implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

    for {
      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(Carbonv1TestDar.path))

      txReq <- getTransactionsRequest(
        transactionFormat(
          Some(Seq(party)),
          Seq.empty,
          Seq((carbonv1.carbonv1.I.TEMPLATE_ID, true)),
        )
      ).map(
        // endless stream here as we would like to keep it open until
        // template is uploaded and contract with this template is created
        _.update(
          _.optionalEndInclusive := None
        )
      )
      transactionFuture = transactions(
        take = 1,
        request = txReq,
      )
      _ = assertEquals(transactionFuture.isCompleted, false)

      _ <- ledger.uploadDarFileAndVetOnConnectedSynchronizers(Dars.read(Carbonv2TestDar.path))

      _ = assertEquals(transactionFuture.isCompleted, false)

      contract <- succeedsEventually(
        maxRetryDuration = 10.seconds,
        description = "Topology processing around Dar upload can take a bit of time.",
        delayMechanism = ledger.delayMechanism,
      ) {
        create(party, new carbonv2.carbonv2.T(party, 21))(carbonv2.carbonv2.T.COMPANION)
      }

      transactions <- transactionFuture

    } yield assertSingleContractWithSimpleView(
      transactions = transactions,
      contractIdentifier = carbonv2.carbonv2.T.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      viewIdentifier = carbonv1.carbonv1.I.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      contractId = contract.contractId,
      viewValue = 21,
    )
  })

  private def assertSingleContractWithSimpleView(
      transactions: Vector[Transaction],
      contractIdentifier: Identifier,
      viewIdentifier: Identifier,
      contractId: String,
      viewValue: Long,
  ): Unit = {
    assertLength("transaction should be found", 1, transactions).discard
    val createdEvent = createdEvents(transactions(0)).head
    assertLength("Create event has a view", 1, createdEvent.interfaceViews).discard
    assertEquals(
      "Create event template ID",
      createdEvent.templateId.get.toString,
      contractIdentifier.toString,
    )
    assertEquals("Create event contract ID", createdEvent.contractId, contractId)
    assertViewEquals(createdEvent.interfaceViews, viewIdentifier) { value =>
      assertLength("View has 1 field", 1, value.fields).discard
      assertEquals("View.value", value.fields(0).getValue.getInt64, viewValue)
    }
  }

  private def updateTransaction(
      emptyView: Boolean = false,
      emptyWitness: Boolean = false,
      emptyCreateArguments: Boolean = false,
      emptyContractKey: Boolean = false,
      emptyDetails: Boolean = false,
  )(tx: com.daml.ledger.api.v2.transaction.Transaction): Transaction =
    tx.copy(
      events = tx.events.map { event =>
        event.copy(event = event.event match {
          case created: Event.Created =>
            created.copy(value =
              created.value.copy(
                witnessParties = if (emptyWitness) Seq.empty else created.value.witnessParties,
                interfaceViews =
                  if (emptyView) Seq.empty
                  else if (emptyDetails)
                    created.value.interfaceViews.map(iv =>
                      iv.copy(viewStatus =
                        iv.viewStatus.map(status => status.copy(details = Seq.empty))
                      )
                    )
                  else created.value.interfaceViews,
                contractKey = if (emptyContractKey) None else created.value.contractKey,
                createArguments = if (emptyCreateArguments) None else created.value.createArguments,
              )
            )
          case other => other
        })
      },
      commandId = "",
    )

  private def hideTraceIdFromStatusMessages(
      tx: com.daml.ledger.api.v2.transaction.Transaction
  ): Transaction =
    tx.copy(
      events = tx.events.map { event =>
        event.copy(event = event.event match {
          case created: Event.Created =>
            created.copy(value =
              created.value.copy(
                interfaceViews = created.value.interfaceViews.map(view =>
                  view.copy(
                    viewStatus = view.viewStatus.map(status =>
                      status.copy(message =
                        status.message
                          .replaceFirst("""DAML_FAILURE\(9,.{8}\)""", "DAML_FAILURE(9,0)")
                      )
                    )
                  )
                )
              )
            )
          case other => other
        })
      },
      commandId = "",
    )

  private def assertViewFailed(views: Seq[InterfaceView], interfaceId: Identifier): Unit = {
    val viewSearch = views.find(_.interfaceId.contains(interfaceId))

    val view = assertDefined(viewSearch, "View could not be found")

    val actualInterfaceId = assertDefined(view.interfaceId, "Interface ID is not defined")
    assertEquals("View has correct interface ID", interfaceId, actualInterfaceId)

    val status = assertDefined(view.viewStatus, "Status is not defined")
    assertEquals("Status must be invalid argument", status.code, 9)
  }

  private def assertViewEquals(views: Seq[InterfaceView], interfaceId: Identifier)(
      checkValue: Record => Unit
  ): Unit = {
    val viewSearch = views.find(_.interfaceId.contains(interfaceId))

    val view = assertDefined(
      viewSearch,
      s"View could not be found, there are: ${views.map(_.interfaceId).mkString("[", ",", "]")}",
    )

    val viewCount = views.count(_.interfaceId.contains(interfaceId))
    assertEquals(s"Only one view of interfaceId=$interfaceId must be defined", viewCount, 1)

    val actualInterfaceId = assertDefined(view.interfaceId, "Interface ID is not defined")
    assertEquals("View has correct interface ID", actualInterfaceId, interfaceId)

    val status = assertDefined(view.viewStatus, "Status is not defined")
    assertEquals("Status must be successful", status.code, 0)

    val actualValue = assertDefined(view.viewValue, "Value is not defined")
    checkValue(actualValue)
  }
}
