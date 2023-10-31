// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_15

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.TransactionHelpers._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsRequest
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction_filter.{
  Filters,
  InclusiveFilters,
  InterfaceFilter,
  TemplateFilter,
  TransactionFilter,
}
import com.daml.ledger.api.v1.value.Identifier
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.test.semantic.InterfaceViews._
import scalaz.Tag

import scala.concurrent.{ExecutionContext, Future}

// Allows using deprecated Protobuf fields for testing
@annotation.nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\..*")
class TransactionServiceFiltersIT extends LedgerTestSuite {

  test(
    "TSFInterfaceTemplateIds",
    "Combine plain interface filters with template ids",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterComposition(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = false,
          includeCreateEventPayload = false,
        ),
        templateIds = createTemplateIdFilter,
      ),
    )
  })

  test(
    "TSFInterfaceTemplatePlainFilters",
    "Combine plain interface filters with plain template filters",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterComposition(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = false,
          includeCreateEventPayload = false,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = false),
      ),
    )
  })

  test(
    "TSFInterfaceTemplateFiltersWithPayloads",
    "Combine plain interface filters with template filters with payloads",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterComposition(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = false,
          includeCreateEventPayload = false,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = true),
      ),
    )
  })

  test(
    "TSFInterfaceWithBlobsTemplateIds",
    "Combine interface filters with blobs with template ids",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterComposition(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = true,
          includeCreateEventPayload = false,
        ),
        templateIds = createTemplateIdFilter,
      ),
    )
  })

  test(
    "TSFInterfaceWithBlobsTemplatePlainFilters",
    "Combine interface filters with blobs with plain template filters",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterCompositionFailure(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = true,
          includeCreateEventPayload = false,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = false),
      ),
    )
  })

  test(
    "TSFInterfaceWithBlobsTemplateFiltersWithPayloads",
    "Combine interface filters with blobs with template filters with payloads",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterCompositionFailure(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = true,
          includeCreateEventPayload = false,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = true),
      ),
    )
  })

  test(
    "TSFInterfaceWithPayloadsTemplateIds",
    "Combine interface filters with payloads with template ids",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterCompositionFailure(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = false,
          includeCreateEventPayload = true,
        ),
        templateIds = createTemplateIdFilter,
      ),
    )
  })

  test(
    "TSFInterfaceWithPayloadsTemplatePlainFilters",
    "Combine interface filters with payloads with plain template filters",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterComposition(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = false,
          includeCreateEventPayload = true,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = false),
      ),
    )
  })

  test(
    "TSFInterfaceWithPayloadsTemplateFiltersWithPayloads",
    "Combine interface filters with payloads with template filters with payloads",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterComposition(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = false,
          includeCreateEventPayload = true,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = true),
      ),
    )
  })

  test(
    "TSFInterfaceWithPayloadsAndBlobs",
    "Combine interface filters with payloads and blobs",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterCompositionFailure(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = true,
          includeCreateEventPayload = true,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = true),
      ),
    )
  })

  test(
    "TSFTemplateIdsWithTemplateFilters",
    "Combine template ids with template filters",
    allocate(SingleParty),
    enabled = _.templateFilters,
  )(implicit ec => { case Participants(Participant(ledger, party)) =>
    testFilterCompositionFailure(
      ledger,
      party,
      createTransactionFilter(
        party = party,
        interfaceFilters = createInterfaceFilter(
          includeCreateArgumentsBlob = false,
          includeCreateEventPayload = false,
        ),
        templateFilters = createTemplateFilter(includeCreateEventPayload = true),
        templateIds = createTemplateIdFilter,
      ),
    )
  })

  private def testFilterComposition(
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      filter: TransactionFilter,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    import ledger._
    for {
      c1 <- create(party, T5(party, 1))
      c2 <- create(party, T6(party, party))
      c3 <- create(party, T3(party, 2))
      _ <- create(party, T4(party, 4))
      txEvents <- flatTransactions(getTransactionsRequest(filter)).map(_.flatMap(createdEvents))
      acsEvents <- activeContracts(createActiveContractsRequest(filter)).map(_._2)
    } yield {
      basicAssertions(
        c1.toString,
        c2.toString,
        c3.toString,
        txEvents,
        blobFlag(filter),
        payloadFlagFromInterfaces(filter),
        payloadFlagFromTemplates(filter),
      )
      basicAssertions(
        c1.toString,
        c2.toString,
        c3.toString,
        acsEvents,
        blobFlag(filter),
        payloadFlagFromInterfaces(filter),
        payloadFlagFromTemplates(filter),
      )
    }
  }

  private def testFilterCompositionFailure(
      ledger: ParticipantTestContext,
      party: Primitive.Party,
      filter: TransactionFilter,
  )(implicit ec: ExecutionContext): Future[Unit] = {
    import ledger._
    for {
      _ <- create(party, T5(party, 1))
      _ <- create(party, T6(party, party))
      _ <- create(party, T3(party, 2))
      _ <- create(party, T4(party, 4))
      _ <- flatTransactions(getTransactionsRequest(filter)).mustFail(
        "filter composition unsupported for flat transactions"
      )
      _ <- activeContracts(createActiveContractsRequest(filter)).mustFail(
        "filter composition unsupported for acs"
      )
    } yield ()
  }

  private def basicAssertions(
      c1: String,
      c2: String,
      c3: String,
      createdEvents: Vector[CreatedEvent],
      expectBlob: Boolean,
      expectPayloadFromInterfaces: Boolean,
      expectPayloadFromTemplates: Boolean,
  ): Unit = {
    val expectPayload = expectPayloadFromInterfaces || expectPayloadFromTemplates
    assertLength("3 transactions found", 3, createdEvents)

    // T5
    val createdEvent1 = createdEvents(0)
    assertEquals(
      "Create event 1 template ID",
      createdEvent1.templateId.get.toString,
      Tag.unwrap(T5.id).toString,
    )
    assertEquals("Create event 1 contract ID", createdEvent1.contractId, c1)
    assertLength("Create event 1 has a view", 1, createdEvent1.interfaceViews)
    assertEquals(
      "Create event 1 createArguments must NOT be empty",
      createdEvent1.createArguments.isEmpty,
      false,
    )
    assertEquals(
      s"""Create event 1 createArgumentsBlob must ${if (expectBlob) "NOT" else ""} be empty""",
      createdEvent1.createArgumentsBlob.isEmpty,
      !expectBlob,
    )
    assertEquals(
      s"""Create event 1 createEventPayload must ${if (expectPayload) "NOT" else ""} be empty""",
      createdEvent1.createEventPayload.isEmpty,
      !expectPayload,
    )

    // T6
    val createdEvent2 = createdEvents(1)
    assertEquals(
      "Create event 2 template ID",
      createdEvent2.templateId.get.toString,
      Tag.unwrap(T6.id).toString,
    )
    assertEquals("Create event 2 contract ID", createdEvent2.contractId, c2)
    assertLength("Create event 2 has a view", 1, createdEvent2.interfaceViews)
    assertEquals(
      "Create event 2 createArguments must be empty",
      createdEvent2.createArguments.isEmpty,
      true,
    )
    assertEquals(
      s"""Create event 2 createArgumentsBlob must ${if (expectBlob) "NOT" else ""} be empty""",
      createdEvent2.createArgumentsBlob.isEmpty,
      !expectBlob,
    )
    assertEquals(
      s"""Create event 2 createEventPayload must ${if (expectPayloadFromInterfaces) "NOT"
        else ""} be empty""",
      createdEvent2.createEventPayload.isEmpty,
      !expectPayloadFromInterfaces,
    )

    // T3
    val createdEvent3 = createdEvents(2)
    assertEquals(
      "Create event 3 template ID",
      createdEvent3.templateId.get.toString,
      Tag.unwrap(T3.id).toString,
    )
    assertEquals("Create event 3 contract ID", createdEvent3.contractId, c3)
    assertLength("Create event 3 has no view", 0, createdEvent3.interfaceViews)
    assertEquals(
      "Create event 3 createArguments must not be empty",
      createdEvent3.createArguments.isEmpty,
      false,
    )
    assertEquals(
      s"""Create event 3 createArgumentsBlob must be empty""",
      createdEvent3.createArgumentsBlob.isEmpty,
      true,
    )
    assertEquals(
      s"""Create event 3 createEventPayload must ${if (expectPayloadFromTemplates) "NOT"
        else ""} be empty""",
      createdEvent3.createEventPayload.isEmpty,
      !expectPayloadFromTemplates,
    )
  }

  private def createInterfaceFilter(
      includeCreateArgumentsBlob: Boolean,
      includeCreateEventPayload: Boolean,
  ) = {
    Seq(
      new InterfaceFilter(
        interfaceId = Some(Tag.unwrap(I2.id)),
        includeInterfaceView = true,
        includeCreateArgumentsBlob = includeCreateArgumentsBlob,
        includeCreateEventPayload = includeCreateEventPayload,
      )
    )
  }

  private def createTemplateIdFilter: Seq[Identifier] =
    Seq(Tag.unwrap(T3.id), Tag.unwrap(T5.id))

  private def createTemplateFilter(includeCreateEventPayload: Boolean): Seq[TemplateFilter] =
    Seq(
      new TemplateFilter(
        templateId = Some(Tag.unwrap(T3.id)),
        includeCreateEventPayload = includeCreateEventPayload,
      ),
      new TemplateFilter(
        templateId = Some(Tag.unwrap(T5.id)),
        includeCreateEventPayload = includeCreateEventPayload,
      ),
    )

  private def createTransactionFilter(
      party: Primitive.Party,
      interfaceFilters: Seq[InterfaceFilter],
      templateIds: Seq[Identifier] = Seq.empty,
      templateFilters: Seq[TemplateFilter] = Seq.empty,
  ): TransactionFilter =
    new TransactionFilter(
      filtersByParty = Map(
        party.toString -> new Filters(
          inclusive = Some(
            new InclusiveFilters(
              templateIds = templateIds,
              templateFilters = templateFilters,
              interfaceFilters = interfaceFilters,
            )
          )
        )
      )
    )

  private def createActiveContractsRequest(filter: TransactionFilter) =
    new GetActiveContractsRequest(
      filter = Some(filter),
      verbose = true,
      activeAtOffset = "",
    )

  private def blobFlag(filter: TransactionFilter): Boolean =
    extractFlag(filter, _.includeCreateArgumentsBlob)

  private def payloadFlagFromInterfaces(filter: TransactionFilter): Boolean =
    extractFlag(filter, _.includeCreateEventPayload)

  private def payloadFlagFromTemplates(filter: TransactionFilter): Boolean =
    (for {
      byParty <- filter.filtersByParty.headOption.map(_._2)
      inclusive <- byParty.inclusive
      templateFilter <- inclusive.templateFilters.headOption
    } yield templateFilter.includeCreateEventPayload).getOrElse(false)

  private def extractFlag(
      filter: TransactionFilter,
      extractor: InterfaceFilter => Boolean,
  ): Boolean =
    (for {
      byParty <- filter.filtersByParty.headOption.map(_._2)
      inclusive <- byParty.inclusive
      interfaceFilter <- inclusive.interfaceFilters.headOption
    } yield extractor(interfaceFilter)).getOrElse(false)

}
