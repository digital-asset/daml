// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.v2.event.{CreatedEvent, ExercisedEvent}
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.daml.ledger.test.java.model.test.{Dummy, DummyWithParam}
import com.daml.ledger.test.java.semantic.interfaceviews.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}

import scala.concurrent.{ExecutionContext, Future}

// Allows using deprecated Protobuf fields for testing
class TransactionServiceFiltersIT extends LedgerTestSuite {

  test(
    "TSFInterfaceTemplatePlainFilters",
    "Combine plain interface filters with plain template filters",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val filterT =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterTAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterI =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = false
        ),
      )
    val filterIAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = false
        ),
      )

    testFilterCompositions(ledger, party, filterT, filterTAnyParty, filterI, filterIAnyParty)

  })

  test(
    "TSFInterfaceTemplateFiltersWithEventBlobs",
    "Combine plain interface filters with template filters with event blobs",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val filterT =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterTAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterI =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = false
        ),
      )
    val filterIAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = false
        ),
      )

    testFilterCompositions(ledger, party, filterT, filterTAnyParty, filterI, filterIAnyParty)

  })

  test(
    "TSFInterfaceWithEventBlobsTemplatePlainFilters",
    "Combine interface filters with event blobs with plain template filters",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val filterT =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterTAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterI =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = true
        ),
      )
    val filterIAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = true
        ),
      )

    testFilterCompositions(ledger, party, filterT, filterTAnyParty, filterI, filterIAnyParty)

  })

  test(
    "TSFInterfaceWithEventBlobsTemplateFiltersWithEventBlobs",
    "Combine interface filters with event blobs with template filters with event blobs",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val filterT =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterTAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterI =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = true
        ),
      )
    val filterIAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = true
        ),
      )

    testFilterCompositions(ledger, party, filterT, filterTAnyParty, filterI, filterIAnyParty)

  })

  test(
    "TSFTemplateWildcardFiltersWithEventBlobs",
    "Template wildcard filters with event blobs",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val format =
      createEventFormat(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = Seq(),
        wildcardFilterO = Some(WildcardFilter(includeCreatedEventBlob = true)),
      )

    import ledger.*
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      c1 <- create(party, new T5(party, 1))(T5.COMPANION)
      c2 <- create(party, new T6(party, party))(T6.COMPANION)
      c3 <- create(party, new T3(party, 2))(T3.COMPANION)
      c4 <- create(party, new T4(party, 4))(T4.COMPANION)
      txReq <- getTransactionsRequest(
        transactionFormat =
          TransactionFormat(Some(format), transactionShape = TRANSACTION_SHAPE_ACS_DELTA),
        begin = endOffsetAtTestStart,
      )
      txReqLedgerEffects <- getTransactionsRequest(
        transactionFormat =
          TransactionFormat(Some(format), transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS),
        begin = endOffsetAtTestStart,
      )
      txEvents <- transactions(txReq).map(_.flatMap(createdEvents))
      txReqLedgerEffects <- transactions(txReqLedgerEffects).map(_.flatMap(createdEvents))
      currentEnd <- ledger.currentEnd()
      acsEvents <- activeContracts(createActiveContractsRequest(format, currentEnd))

      // archive active contracts to avoid interference with the next tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      eventBlobAssertions(txEvents)
      eventBlobAssertions(txReqLedgerEffects)
      eventBlobAssertions(acsEvents)
    }

  })

  test(
    "TSFExercisedTemplateFilters",
    "Filter exercised events with template filters",
    allocate(TwoParties),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party1, party2))) =>
    val formatT =
      createEventFormat(
        partyO = Some(party1),
        interfaceFilters = Seq(),
        templateFilters = templateFilterForDummy,
      )
    val formatTAnyParty =
      createEventFormat(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = templateFilterForDummy,
      )

    testFilterCompositionsForExercised(ledger, party1, party2, formatT, formatTAnyParty)

  })

  test(
    s"TransactionAcsDeltaWildcardFilters",
    "Create arguments for wildcard filters on acs delta transaction streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T5(party, 1))(T5.COMPANION)
      c2 <- create(party, new T6(party, party))(T6.COMPANION)
      c3 <- create(party, new T3(party, 3))(T3.COMPANION)
      c4 <- create(party, new T4(party, 4))(T4.COMPANION)
      _ <- archive(ledger, party)(c1, c2, c3, c4)
      txReq <- getTransactionsRequest(
        transactionFormat(
          parties = Some(Seq(party)),
          templateIds = Seq.empty,
          interfaceFilters = Seq.empty,
          transactionShape = AcsDelta,
        )
      )
      txs <- transactions(txReq)
      txReqPartyWildcard <- getTransactionsRequest(
        transactionFormat(
          parties = None,
          templateIds = Seq.empty,
          interfaceFilters = Seq.empty,
          transactionShape = AcsDelta,
        )
      )
      txsPartyWildcard <- transactions(txReqPartyWildcard)
      created = txs.flatMap(createdEvents)
      createdPartyWildcard = txsPartyWildcard.flatMap(createdEvents)
    } yield {
      (created ++ createdPartyWildcard).foreach(event =>
        assertEquals(
          s"Create event contract arguments must NOT be empty for $event",
          event.createArguments.isEmpty,
          false,
        )
      )
    }
  })

  test(
    s"TransactionLedgerEffectsWildcardFilters",
    "Create arguments for wildcard filters on ledger effects transaction streams",
    allocate(SingleParty),
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    import ledger.*
    for {
      c1 <- create(party, new T5(party, 1))(T5.COMPANION)
      c2 <- create(party, new T6(party, party))(T6.COMPANION)
      c3 <- create(party, new T3(party, 3))(T3.COMPANION)
      c4 <- create(party, new T4(party, 4))(T4.COMPANION)
      _ <- archive(ledger, party)(c1, c2, c3, c4)
      txReq <- getTransactionsRequest(
        transactionFormat(
          parties = Some(Seq(party)),
          templateIds = Seq.empty,
          interfaceFilters = Seq.empty,
          transactionShape = LedgerEffects,
        )
      )
      txs <- transactions(txReq)
      txReqPartyWildcard <- getTransactionsRequest(
        transactionFormat(
          parties = None,
          templateIds = Seq.empty,
          interfaceFilters = Seq.empty,
          transactionShape = LedgerEffects,
        )
      )
      txsPartyWildcard <- transactions(txReqPartyWildcard)
      created = txs.flatMap(createdEvents)
      createdPartyWildcard = txsPartyWildcard.flatMap(createdEvents)
    } yield {
      (created ++ createdPartyWildcard).foreach(event =>
        assertEquals(
          s"Create event contract arguments must NOT be empty for $event",
          event.createArguments.isEmpty,
          false,
        )
      )
    }
  })

  private def testFilterCompositions(
      ledger: ParticipantTestContext,
      party: Party,
      formatT: EventFormat,
      formatTAnyParty: EventFormat,
      formatI: EventFormat,
      formatIAnyParty: EventFormat,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- testFilterComposition(
        ledger,
        party,
        mergeFormats(formatT, formatI),
      )

      _ <- testFilterComposition(
        ledger,
        party,
        mergeFormats(formatTAnyParty, formatIAnyParty),
      )

      _ <- testFilterComposition(
        ledger,
        party,
        mergeFormats(formatT, formatIAnyParty),
      )

      _ <- testFilterComposition(
        ledger,
        party,
        mergeFormats(formatTAnyParty, formatI),
      )
    } yield ()

  private def testFilterCompositionsForExercised(
      ledger: ParticipantTestContext,
      party1: Party,
      party2: Party,
      formatT: EventFormat,
      formatTAnyParty: EventFormat,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- testFilterCompositionForExercised(
        ledger,
        party1,
        party2,
        formatT,
      )

      _ <- testFilterCompositionForExercised(
        ledger,
        party1,
        party2,
        formatTAnyParty,
      )

    } yield ()

  private def archive(ledger: ParticipantTestContext, party: Party)(
      c1: T5.ContractId,
      c2: T6.ContractId,
      c3: T3.ContractId,
      c4: T4.ContractId,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- ledger.exercise(party, c1.exerciseArchive())
      _ <- ledger.exercise(party, c2.exerciseArchive())
      _ <- ledger.exercise(party, c3.exerciseArchive())
      _ <- ledger.exercise(party, c4.exerciseArchive())
    } yield ()

  private def testFilterComposition(
      ledger: ParticipantTestContext,
      party: Party,
      format: EventFormat,
  )(implicit ec: ExecutionContext): Future[Unit] = {

    import ledger.*
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      c1 <- create(party, new T5(party, 1))(T5.COMPANION)
      c2 <- create(party, new T6(party, party))(T6.COMPANION)
      c3 <- create(party, new T3(party, 2))(T3.COMPANION)
      c4 <- create(party, new T4(party, 4))(T4.COMPANION)
      txReq <- getTransactionsRequest(
        TransactionFormat(
          eventFormat = Some(format),
          transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
        ),
        begin = endOffsetAtTestStart,
      )
      txReqLedgerEffects <- getTransactionsRequest(
        TransactionFormat(
          eventFormat = Some(format),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        ),
        begin = endOffsetAtTestStart,
      )
      txEvents <- transactions(txReq).map(_.flatMap(createdEvents))
      txEventsLedgerEffects <- transactions(txReqLedgerEffects).map(_.flatMap(createdEvents))
      currentEnd <- ledger.currentEnd()
      acsEvents <- activeContracts(createActiveContractsRequest(format, currentEnd))
      // archive active contracts to avoid interference with the next tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      basicAssertions(
        c1.contractId,
        c2.contractId,
        c3.contractId,
        txEvents,
        eventBlobFlagFromInterfaces(format),
        eventBlobFlagFromTemplates(format),
      )
      basicAssertions(
        c1.contractId,
        c2.contractId,
        c3.contractId,
        acsEvents,
        eventBlobFlagFromInterfaces(format),
        eventBlobFlagFromTemplates(format),
      )
      basicAssertions(
        c1.contractId,
        c2.contractId,
        c3.contractId,
        txEventsLedgerEffects,
        eventBlobFlagFromInterfaces(format),
        eventBlobFlagFromTemplates(format),
      )
    }
  }

  private def testFilterCompositionForExercised(
      ledger: ParticipantTestContext,
      party1: Party,
      party2: Party,
      format: EventFormat,
  )(implicit ec: ExecutionContext): Future[Unit] = {

    import ledger.*
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      dummy1 <- create(party1, new Dummy(party1))(Dummy.COMPANION)
      dummy2 <- create(party2, new Dummy(party2))(Dummy.COMPANION)
      dummyWithParam <- create(party1, new DummyWithParam(party1))(DummyWithParam.COMPANION)
      _ <- ledger.exercise(party1, dummy1.exerciseDummyNonConsuming())
      _ <- ledger.exercise(party2, dummy2.exerciseDummyNonConsuming())
      _ <- ledger.exercise(party1, dummyWithParam.exerciseDummyChoice2(""))
      _ <- ledger.exercise(party1, dummy1.exerciseArchive())
      _ <- ledger.exercise(party2, dummy2.exerciseArchive())

      txReq <- getTransactionsRequest(
        TransactionFormat(
          eventFormat = Some(format),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
        ),
        begin = endOffsetAtTestStart,
      )
      txEvents <- transactions(txReq).map(_.flatMap(exercisedEvents))
    } yield {
      assertionsForExercised(
        txEvents,
        wildcardParty = format.filtersForAnyParty.isDefined,
      )
    }
  }

  private def assertionsForExercised(
      exercisedEvents: Vector[ExercisedEvent],
      wildcardParty: Boolean,
  ): Unit = {
    val expectedEventsNum = if (wildcardParty) 4 else 2
    assertLength(
      s"$expectedEventsNum exercised events should have been found",
      expectedEventsNum,
      exercisedEvents,
    ).discard

    for (event <- exercisedEvents) {
      assertEquals(
        "Exercised event of Dummy template ID",
        event.templateId.get,
        Dummy.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
      )
    }
  }

  private def basicAssertions(
      c1: String,
      c2: String,
      c3: String,
      createdEvents: Vector[CreatedEvent],
      expectEventBlobFromInterfaces: Boolean,
      expectEventBlobFromTemplates: Boolean,
  ): Unit = {
    val expectEventBlob = expectEventBlobFromInterfaces || expectEventBlobFromTemplates
    assertLength("3 transactions found", 3, createdEvents).discard

    // T5
    val createdEvent1 = createdEvents(0)
    assertEquals(
      "Create event 1 template ID",
      createdEvent1.templateId.get,
      T5.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
    )
    assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1).discard
    assertLength("Create event 1 has a view", 1, createdEvent1.interfaceViews).discard
    assertEquals(
      "Create event 1 createArguments must NOT be empty",
      createdEvent1.createArguments.isEmpty,
      false,
    )
    assertEquals(
      s"""Create event 1 createdEventBlob must ${if (expectEventBlob) "NOT" else ""} be empty""",
      createdEvent1.createdEventBlob.isEmpty,
      !expectEventBlob,
    )

    // T6
    val createdEvent2 = createdEvents(1)
    assertEquals(
      "Create event 2 template ID",
      createdEvent2.templateId.get,
      T6.TEMPLATE_ID_WITH_PACKAGE_ID.toV1,
    )
    assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2)
    assertLength("Create event 2 has a view", 1, createdEvent2.interfaceViews).discard
    assertEquals(
      "Create event 2 createArguments must NOT be empty",
      createdEvent2.createArguments.isEmpty,
      false,
    )
    assertEquals(
      s"""Create event 2 createdEventBlob must ${if (expectEventBlobFromInterfaces) "NOT"
        else ""} be empty""",
      createdEvent2.createdEventBlob.isEmpty,
      !expectEventBlobFromInterfaces,
    )

    // T3
    val createdEvent3 = createdEvents(2)
    assertEquals(
      "Create event 3 template ID",
      createdEvent3.templateId.get.toString,
      T3.TEMPLATE_ID_WITH_PACKAGE_ID.toV1.toString,
    )
    assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3)
    assertLength("Create event 3 has no view", 0, createdEvent3.interfaceViews).discard
    assertEquals(
      "Create event 3 createArguments must NOT be empty",
      createdEvent3.createArguments.isEmpty,
      false,
    )
    assertEquals(
      s"""Create event 3 createdEventBlob must ${if (expectEventBlobFromTemplates) "NOT"
        else ""} be empty""",
      createdEvent3.createdEventBlob.isEmpty,
      !expectEventBlobFromTemplates,
    )
  }

  private def eventBlobAssertions(
      createdEvents: Vector[CreatedEvent]
  ): Unit = {
    assertLength("4 transactions found", 4, createdEvents).discard

    createdEvents.foreach(createdEvent =>
      assertEquals(
        s"Create event $createdEvent createdEventBlob must NOT be empty",
        createdEvent.createdEventBlob.isEmpty,
        false,
      )
    )

  }

  private def createInterfaceFilter(
      includeCreatedEventBlob: Boolean
  ) =
    Seq(
      new InterfaceFilter(
        interfaceId = Some(I2.TEMPLATE_ID.toV1),
        includeInterfaceView = true,
        includeCreatedEventBlob = includeCreatedEventBlob,
      )
    )

  private def createTemplateFilter(includeCreatedEventBlob: Boolean): Seq[TemplateFilter] =
    Seq(
      new TemplateFilter(
        templateId = Some(T3.TEMPLATE_ID.toV1),
        includeCreatedEventBlob = includeCreatedEventBlob,
      ),
      new TemplateFilter(
        templateId = Some(T5.TEMPLATE_ID.toV1),
        includeCreatedEventBlob = includeCreatedEventBlob,
      ),
    )

  private lazy val templateFilterForDummy: Seq[TemplateFilter] =
    Seq(
      TemplateFilter(
        templateId = Some(Dummy.TEMPLATE_ID.toV1),
        includeCreatedEventBlob = false,
      )
    )

  private def createEventFormat(
      partyO: Option[Party], // party or party-wildcard
      interfaceFilters: Seq[InterfaceFilter],
      templateFilters: Seq[TemplateFilter] = Seq.empty,
      wildcardFilterO: Option[WildcardFilter] = None,
  ): EventFormat = {
    val filters = Filters(
      templateFilters.map(tf => CumulativeFilter(IdentifierFilter.TemplateFilter(tf)))
        ++
          interfaceFilters.map(ifaceF =>
            CumulativeFilter(IdentifierFilter.InterfaceFilter(ifaceF))
          ) ++ (wildcardFilterO match {
            case Some(wildcardFilter) =>
              Seq(CumulativeFilter.defaultInstance.withWildcardFilter(wildcardFilter))
            case None => Seq.empty
          })
    )

    partyO match {
      case Some(party) =>
        EventFormat(
          filtersByParty = Map(party.getValue -> filters),
          filtersForAnyParty = None,
          verbose = true,
        )
      case None =>
        EventFormat(
          filtersByParty = Map.empty,
          filtersForAnyParty = Some(filters),
          verbose = true,
        )
    }
  }

  private def createActiveContractsRequest(eventFormat: EventFormat, activeAtOffset: Long) =
    GetActiveContractsRequest(
      activeAtOffset = activeAtOffset,
      eventFormat = Some(eventFormat),
    )

  private def eventBlobFlagFromInterfaces(format: EventFormat): Boolean =
    extractFlag(format, _.includeCreatedEventBlob)

  private def eventBlobFlagFromTemplates(format: EventFormat): Boolean =
    (for {
      byParty <- format.filtersByParty.headOption.map(_._2)
      templateFilter <- byParty.cumulative.collectFirst(_.identifierFilter match {
        case IdentifierFilter.TemplateFilter(templateF) => templateF
      })
    } yield templateFilter.includeCreatedEventBlob).getOrElse(false) ||
      (for {
        anyParty <- format.filtersForAnyParty
        templateFilter <- anyParty.cumulative.collectFirst(_.identifierFilter match {
          case IdentifierFilter.TemplateFilter(templateF) => templateF
        })
      } yield templateFilter.includeCreatedEventBlob).getOrElse(false)

  private def extractFlag(
      format: EventFormat,
      extractor: InterfaceFilter => Boolean,
  ): Boolean =
    (for {
      byParty <- format.filtersByParty.headOption.map(_._2)
      interfaceFilter <- byParty.cumulative.collectFirst(_.identifierFilter match {
        case IdentifierFilter.InterfaceFilter(interfaceF) => interfaceF
      })
    } yield extractor(interfaceFilter)).getOrElse(false) ||
      (for {
        anyParty <- format.filtersForAnyParty
        interfaceFilter <- anyParty.cumulative.collectFirst(_.identifierFilter match {
          case IdentifierFilter.InterfaceFilter(interfaceF) => interfaceF
        })
      } yield extractor(interfaceFilter)).getOrElse(false)

  private def mergeFormats(
      t1: EventFormat,
      t2: EventFormat,
  ): EventFormat = EventFormat(
    filtersByParty = t1.filtersByParty ++ t2.filtersByParty.map { case (p, f2) =>
      t1.filtersByParty.get(p) match {
        case Some(f1) => p -> mergeFilters(f1, f2)
        case None => p -> f2
      }
    },
    filtersForAnyParty = t1.filtersForAnyParty match {
      case Some(f1) => Some(t2.filtersForAnyParty.fold(f1)(f2 => mergeFilters(f1, f2)))
      case None => t2.filtersForAnyParty
    },
    verbose = t1.verbose || t2.verbose,
  )

  private def mergeFilters(f1: Filters, f2: Filters): Filters =
    Filters(f1.cumulative ++ f2.cumulative)

}
