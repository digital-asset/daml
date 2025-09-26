// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers.*
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.testtool.infrastructure.{LedgerTestSuite, Party}
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.state_service.GetActiveContractsRequest
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  Filters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
  WildcardFilter,
}
import com.daml.ledger.test.java.semantic.interfaceviews.{I2, T3, T4, T5, T6}
import com.digitalasset.canton.discard.Implicits.DiscardOps

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

// Allows using deprecated Protobuf fields for testing
// TODO(i23504) Remove
@nowarn("cat=deprecation")
class LegacyTransactionServiceFiltersIT extends LedgerTestSuite {

  test(
    "TSFInterfaceTemplatePlainFilters",
    "Combine plain interface filters with plain template filters",
    allocate(SingleParty),
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq(party))) =>
    val filterT =
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterTAnyParty =
      createTransactionFilter(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterI =
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = false
        ),
      )
    val filterIAnyParty =
      createTransactionFilter(
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
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterTAnyParty =
      createTransactionFilter(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterI =
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = false
        ),
      )
    val filterIAnyParty =
      createTransactionFilter(
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
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterTAnyParty =
      createTransactionFilter(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = false),
      )
    val filterI =
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = true
        ),
      )
    val filterIAnyParty =
      createTransactionFilter(
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
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterTAnyParty =
      createTransactionFilter(
        partyO = None,
        interfaceFilters = Seq(),
        templateFilters = createTemplateFilter(includeCreatedEventBlob = true),
      )
    val filterI =
      createTransactionFilter(
        partyO = Some(party),
        interfaceFilters = createInterfaceFilter(
          includeCreatedEventBlob = true
        ),
      )
    val filterIAnyParty =
      createTransactionFilter(
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
    val filter =
      createTransactionFilter(
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
      txReq <- getTransactionsRequestLegacy(
        transactionFilter = filter,
        begin = endOffsetAtTestStart,
      )
      txEvents <- transactions(txReq).map(_.flatMap(createdEvents))
      currentEnd <- ledger.currentEnd()
      acsEvents <- activeContracts(createActiveContractsRequest(filter, currentEnd))

      // archive active contracts to avoid interference with the next tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      eventBlobAssertions(txEvents)
      eventBlobAssertions(acsEvents)
    }

  })

  private def testFilterCompositions(
      ledger: ParticipantTestContext,
      party: Party,
      filterT: TransactionFilter,
      filterTAnyParty: TransactionFilter,
      filterI: TransactionFilter,
      filterIAnyParty: TransactionFilter,
  )(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- testFilterComposition(
        ledger,
        party,
        mergeTransactionFilters(filterT, filterI),
      )

      _ <- testFilterComposition(
        ledger,
        party,
        mergeTransactionFilters(filterTAnyParty, filterIAnyParty),
      )

      _ <- testFilterComposition(
        ledger,
        party,
        mergeTransactionFilters(filterT, filterIAnyParty),
      )

      _ <- testFilterComposition(
        ledger,
        party,
        mergeTransactionFilters(filterTAnyParty, filterI),
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
      filter: TransactionFilter,
  )(implicit ec: ExecutionContext): Future[Unit] = {

    import ledger.*
    for {
      endOffsetAtTestStart <- ledger.currentEnd()
      c1 <- create(party, new T5(party, 1))(T5.COMPANION)
      c2 <- create(party, new T6(party, party))(T6.COMPANION)
      c3 <- create(party, new T3(party, 2))(T3.COMPANION)
      c4 <- create(party, new T4(party, 4))(T4.COMPANION)
      txReq <- getTransactionsRequestLegacy(
        transactionFilter = filter,
        begin = endOffsetAtTestStart,
      )
      txEvents <- transactions(txReq).map(_.flatMap(createdEvents))
      currentEnd <- ledger.currentEnd()
      acsEvents <- activeContracts(createActiveContractsRequest(filter, currentEnd))
      // archive active contracts to avoid interference with the next tests
      _ <- archive(ledger, party)(c1, c2, c3, c4)
    } yield {
      basicAssertions(
        c1.contractId,
        c2.contractId,
        c3.contractId,
        txEvents,
        eventBlobFlagFromInterfaces(filter),
        eventBlobFlagFromTemplates(filter),
      )
      basicAssertions(
        c1.contractId,
        c2.contractId,
        c3.contractId,
        acsEvents,
        eventBlobFlagFromInterfaces(filter),
        eventBlobFlagFromTemplates(filter),
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

  private def createTransactionFilter(
      partyO: Option[Party], // party or party-wildcard
      interfaceFilters: Seq[InterfaceFilter],
      templateFilters: Seq[TemplateFilter] = Seq.empty,
      wildcardFilterO: Option[WildcardFilter] = None,
  ): TransactionFilter = {
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
        TransactionFilter(
          filtersByParty = Map(party.getValue -> filters),
          filtersForAnyParty = None,
        )
      case None =>
        TransactionFilter(
          filtersByParty = Map.empty,
          filtersForAnyParty = Some(filters),
        )
    }
  }

  private def createActiveContractsRequest(filter: TransactionFilter, activeAtOffset: Long) =
    new GetActiveContractsRequest(
      filter = Some(filter),
      verbose = true,
      activeAtOffset = activeAtOffset,
      eventFormat = None,
    )

  private def eventBlobFlagFromInterfaces(filter: TransactionFilter): Boolean =
    extractFlag(filter, _.includeCreatedEventBlob)

  private def eventBlobFlagFromTemplates(filter: TransactionFilter): Boolean =
    (for {
      byParty <- filter.filtersByParty.headOption.map(_._2)
      templateFilter <- byParty.cumulative.collectFirst(_.identifierFilter match {
        case IdentifierFilter.TemplateFilter(templateF) => templateF
      })
    } yield templateFilter.includeCreatedEventBlob).getOrElse(false) ||
      (for {
        anyParty <- filter.filtersForAnyParty
        templateFilter <- anyParty.cumulative.collectFirst(_.identifierFilter match {
          case IdentifierFilter.TemplateFilter(templateF) => templateF
        })
      } yield templateFilter.includeCreatedEventBlob).getOrElse(false)

  private def extractFlag(
      filter: TransactionFilter,
      extractor: InterfaceFilter => Boolean,
  ): Boolean =
    (for {
      byParty <- filter.filtersByParty.headOption.map(_._2)
      interfaceFilter <- byParty.cumulative.collectFirst(_.identifierFilter match {
        case IdentifierFilter.InterfaceFilter(interfaceF) => interfaceF
      })
    } yield extractor(interfaceFilter)).getOrElse(false) ||
      (for {
        anyParty <- filter.filtersForAnyParty
        interfaceFilter <- anyParty.cumulative.collectFirst(_.identifierFilter match {
          case IdentifierFilter.InterfaceFilter(interfaceF) => interfaceF
        })
      } yield extractor(interfaceFilter)).getOrElse(false)

  private def mergeTransactionFilters(
      t1: TransactionFilter,
      t2: TransactionFilter,
  ): TransactionFilter = TransactionFilter(
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
  )

  private def mergeFilters(f1: Filters, f2: Filters): Filters =
    Filters(f1.cumulative ++ f2.cumulative)

}
