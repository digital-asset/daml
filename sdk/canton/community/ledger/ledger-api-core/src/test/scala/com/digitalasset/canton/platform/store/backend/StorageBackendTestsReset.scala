// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.common.EventIdSource.{
  ActivateStakeholder,
  ActivateWitnesses,
  DeactivateStakeholder,
  DeactivateWitnesses,
  VariousWitnesses,
}
import com.digitalasset.canton.platform.store.backend.common.EventPayloadSourceForUpdatesLedgerEffects
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.PaginationInput
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsReset extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (reset)"

  import StorageBackendTestValues.*

  it should "start with an empty index" in {
    val identity = executeSql(backend.parameter.ledgerIdentity)
    val end = executeSql(backend.parameter.ledgerEnd)
    val parties = executeSql(backend.party.knownParties(None, None, 10))
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
    val parties = executeSql(backend.party.knownParties(None, None, 10))

    parties shouldBe empty
  }

  it should "reset everything when using resetAll" in {
    val dtos: Vector[DbDto] = Vector(
      // 1: party allocation
      Seq(dtoPartyEntry(offset(1))),
      // 2: transaction with create node
      dtosCreate(
        event_offset = 2L,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#3"),
      )(),
      Seq(dtoCompletion(offset(2))),
      // 3: transaction with exercise node and retroactive divulgence
      dtosConsumingExercise(
        event_offset = 3L,
        event_sequential_id = 2L,
      ),
      Seq(dtoCompletion(offset(3))),
      // 4: assign event
      dtosAssign(
        event_offset = 4L,
        event_sequential_id = 3L,
        notPersistedContractId = hashCid("#4"),
      )(),
      // 5: unassign event
      dtosUnassign(
        event_offset = 5L,
        event_sequential_id = 4L,
      ),
      // 6: topology transaction
      Seq(dtoPartyToParticipant(offset = offset(6), eventSequentialId = 5L)),
      // 7: witnessed create
      dtosWitnessedCreate(event_offset = 7L, event_sequential_id = 6L)(),
      // 8: witnessed consuming exercise
      dtosWitnessedExercised(event_offset = 8L, event_sequential_id = 7L),
      // 9: witnessed non-consuming exercise
      dtosWitnessedExercised(event_offset = 9L, event_sequential_id = 8L, consuming = false),
      // String interning
      Seq(DbDto.StringInterningDto(internalId = 10, externalString = "d|x:abc")),
    ).flatten

    // Initialize and insert some data
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(ledgerEnd(10, 10L)))

    // queries
    def identity = executeSql(backend.parameter.ledgerIdentity)

    def end = executeSql(backend.parameter.ledgerEnd)

    def events =
      executeSql(
        backend.event.fetchEventPayloadsLedgerEffects(
          EventPayloadSourceForUpdatesLedgerEffects.Activate
        )(
          eventSequentialIds = IdRange(1L, 10L),
          requestingPartiesForTx = Some(Set.empty),
          requestingPartiesForReassignment = Some(Set.empty),
        )
      ) ++
        executeSql(
          backend.event.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.Deactivate
          )(
            eventSequentialIds = IdRange(1L, 10L),
            requestingPartiesForTx = Some(Set.empty),
            requestingPartiesForReassignment = Some(Set.empty),
          )
        ) ++
        executeSql(
          backend.event.fetchEventPayloadsLedgerEffects(
            EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
          )(
            eventSequentialIds = IdRange(1L, 10L),
            requestingPartiesForTx = Some(Set.empty),
            requestingPartiesForReassignment = Some(Set.empty),
          )
        )

    def parties = executeSql(backend.party.knownParties(None, None, 10))

    def stringInterningEntries = executeSql(
      backend.stringInterning.loadStringInterningEntries(0, 1000)
    )

    val paginationInput = PaginationInput(
      startExclusive = 0L,
      endInclusive = 1000L,
      limit = 1000,
    )

    def activateStakeholderIds = executeSql(
      backend.event.updateStreamingQueries.fetchEventIds(ActivateStakeholder)(
        witnessO = None,
        templateIdO = None,
        eventTypes = Set.empty,
      )(_)(paginationInput)
    )

    def activateWitnessesIds = executeSql(
      backend.event.updateStreamingQueries.fetchEventIds(ActivateWitnesses)(
        witnessO = None,
        templateIdO = None,
        eventTypes = Set.empty,
      )(_)(paginationInput)
    )

    def deactivateStakeholderIds = executeSql(
      backend.event.updateStreamingQueries.fetchEventIds(DeactivateStakeholder)(
        witnessO = None,
        templateIdO = None,
        eventTypes = Set.empty,
      )(_)(paginationInput)
    )

    def deactivateWitnessesIds = executeSql(
      backend.event.updateStreamingQueries.fetchEventIds(DeactivateWitnesses)(
        witnessO = None,
        templateIdO = None,
        eventTypes = Set.empty,
      )(_)(paginationInput)
    )

    def variousWitnessesIds = executeSql(
      backend.event.updateStreamingQueries.fetchEventIds(VariousWitnesses)(
        witnessO = None,
        templateIdO = None,
        eventTypes = Set.empty,
      )(_)(paginationInput)
    )

    // verify queries indeed return something
    identity should not be None
    end should not be ParameterStorageBackend.LedgerEnd.beforeBegin
    events.size shouldBe 7
    parties should not be empty
    stringInterningEntries should not be empty
    activateStakeholderIds should not be empty
    activateWitnessesIds should not be empty
    deactivateStakeholderIds should not be empty
    deactivateWitnessesIds should not be empty
    variousWitnessesIds should not be empty

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
    activateStakeholderIds shouldBe empty
    activateWitnessesIds shouldBe empty
    deactivateStakeholderIds shouldBe empty
    deactivateWitnessesIds shouldBe empty
    variousWitnessesIds shouldBe empty
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
