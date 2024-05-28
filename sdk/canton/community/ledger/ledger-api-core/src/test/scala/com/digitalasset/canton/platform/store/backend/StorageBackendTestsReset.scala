// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForTreeTx
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsReset extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (reset)"

  import StorageBackendTestValues.*

  it should "start with an empty index" in {
    val identity = executeSql(backend.parameter.ledgerIdentity)
    val end = executeSql(backend.parameter.ledgerEnd)
    val parties = executeSql(backend.party.knownParties(None, 10))
    val stringInterningEntries = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 1000)
    )

    identity shouldBe None
    end shouldBe ParameterStorageBackend.LedgerEnd.beforeBegin
    parties shouldBe empty
    stringInterningEntries shouldBe empty
  }

  it should "not see any data after advancing the ledger end" in {
    advanceLedgerEndToMakeOldDataVisible()
    val parties = executeSql(backend.party.knownParties(None, 10))

    parties shouldBe empty
  }

  it should "reset everything when using resetAll" in {
    val dtos: Vector[DbDto] = Vector(
      // 1: party allocation
      dtoPartyEntry(offset(1)),
      // 2: transaction with create node
      dtoCreate(offset(2), 1L, hashCid("#3")),
      DbDto.IdFilterCreateStakeholder(1L, someTemplateId.toString, someParty.toString),
      dtoCompletion(offset(2)),
      // 3: transaction with exercise node and retroactive divulgence
      dtoExercise(offset(3), 2L, true, hashCid("#3")),
      dtoCompletion(offset(3)),
      // 4: assign event
      dtoAssign(offset(4), 4L, hashCid("#4")),
      DbDto.IdFilterAssignStakeholder(4L, someTemplateId.toString, someParty.toString),
      // 5: unassign event
      dtoUnassign(offset(5), 5L, hashCid("#5")),
      DbDto.IdFilterUnassignStakeholder(5L, someTemplateId.toString, someParty.toString),
      // String interning
      DbDto.StringInterningDto(10, "d|x:abc"),
    )

    // Initialize and insert some data
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(ledgerEnd(4, 3L)))

    // queries
    def identity = executeSql(backend.parameter.ledgerIdentity)

    def end = executeSql(backend.parameter.ledgerEnd)

    def events = {
      executeSql(
        backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
          EventPayloadSourceForTreeTx.Create
        )(List(1L), Set.empty)
      ) ++
        executeSql(
          backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
            EventPayloadSourceForTreeTx.Consuming
          )(List(2L), Set.empty)
        )
    }

    def parties = executeSql(backend.party.knownParties(None, 10))

    def stringInterningEntries = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 1000)
    )

    def filterIds = executeSql(
      backend.event.transactionStreamingQueries.fetchIdsOfCreateEventsForStakeholder(
        stakeholderO = Some(someParty),
        templateIdO = None,
        startExclusive = 0,
        endInclusive = 1000,
        limit = 1000,
      )
    )

    def assignEvents = executeSql(
      backend.event.assignEventBatch(
        eventSequentialIds = List(4),
        allFilterParties = Some(Set.empty),
      )
    )

    def unassignEvents = executeSql(
      backend.event.unassignEventBatch(
        eventSequentialIds = List(5),
        allFilterParties = Some(Set.empty),
      )
    )

    def assignIds = executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0L,
        endInclusive = 1000L,
        1000,
      )
    )

    def unassignIds = executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0L,
        endInclusive = 1000L,
        1000,
      )
    )

    // verify queries indeed returning something
    identity should not be None
    end should not be ParameterStorageBackend.LedgerEnd.beforeBegin
    events.size shouldBe 2
    parties should not be empty
    stringInterningEntries should not be empty
    filterIds should not be empty
    assignEvents should not be empty
    unassignEvents should not be empty
    assignIds should not be empty
    unassignIds should not be empty

    // Reset
    executeSql(backend.reset.resetAll)

    // Check the contents (queries that do not depend on ledger end)
    identity shouldBe None
    end shouldBe ParameterStorageBackend.LedgerEnd.beforeBegin
    events shouldBe empty

    // Check the contents (queries that don't read beyond ledger end)
    advanceLedgerEndToMakeOldDataVisible()

    parties shouldBe empty
    stringInterningEntries shouldBe empty
    filterIds shouldBe empty
    assignEvents shouldBe empty
    unassignEvents shouldBe empty
    assignIds shouldBe empty
    unassignIds shouldBe empty
  }

  // Some queries are protected to never return data beyond the current ledger end.
  // By advancing the ledger end to a large value, we can check whether these
  // queries now find any left-over data not cleaned by reset.
  private def advanceLedgerEndToMakeOldDataVisible(): Unit = {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(updateLedgerEnd(ledgerEnd(10000, 10000)))
    ()
  }
}
