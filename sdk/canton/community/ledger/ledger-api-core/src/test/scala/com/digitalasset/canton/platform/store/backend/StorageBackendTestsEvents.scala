// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  CommonEventProperties,
  CommonUpdateProperties,
  RawArchivedEvent,
  RawCreatedEventLegacy,
  RawExercisedEvent,
  RawLedgerEffectsEventLegacy,
  RawThinAcsDeltaEvent,
  RawThinAssignEvent,
  RawThinCreatedEvent,
  RawThinLedgerEffectsEvent,
  RawUnassignEvent,
  ReassignmentProperties,
  SequentialIdBatch,
  SynchronizerOffset,
  ThinCreatedEventProperties,
  TransactionProperties,
}
import com.digitalasset.canton.platform.store.backend.common.{
  EventIdSource,
  EventIdSourceLegacy,
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesLedgerEffects,
  EventPayloadSourceForUpdatesLedgerEffectsLegacy,
}
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.{
  IdFilterInput,
  PaginationInput,
  PaginationLastOnlyInput,
}
import com.digitalasset.canton.protocol.TestUpdateId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{
  ChoiceName,
  Identifier,
  NameTypeConRef,
  PackageName,
  Party,
}
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalactic.Equality
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsEvents
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (events)"

  import StorageBackendTestValues.*
  import ScalatestEqualityHelpers.*

  it should "find contracts by party legacy" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreateLegacy(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(1L, someTemplateId, partyObserver1, first_per_sequential_id = false),
      dtoCreateLegacy(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(2L, someTemplateId, partyObserver2, first_per_sequential_id = false),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyObserver1),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyObserver2),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = None,
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
    resultSuperReader should contain theSameElementsAs Vector(1L, 2L)
  }

  it should "find contracts by party and template legacy" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreateLegacy(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(1L, someTemplateId, partyObserver1, first_per_sequential_id = false),
      dtoCreateLegacy(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(2L, someTemplateId, partyObserver2, first_per_sequential_id = false),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = Some(someTemplateId),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyObserver1),
          templateIdO = Some(someTemplateId),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyObserver2),
          templateIdO = Some(someTemplateId),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = None,
          templateIdO = Some(someTemplateId),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
    resultSuperReader should contain theSameElementsAs Vector(1L, 2L)
  }

  it should "not find contracts when the template doesn't match legacy" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")
    val otherTemplate = NameTypeConRef.assertFromString("#pkg-name:Mod:Template2")

    val dtos = Vector(
      dtoCreateLegacy(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(1L, someTemplateId, partyObserver1, first_per_sequential_id = false),
      dtoCreateLegacy(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(2L, someTemplateId, partyObserver2, first_per_sequential_id = false),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = Some(otherTemplate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyObserver1),
          templateIdO = Some(otherTemplate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyObserver2),
          templateIdO = Some(otherTemplate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = None,
          templateIdO = Some(otherTemplate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultSignatory shouldBe empty
    resultObserver1 shouldBe empty
    resultObserver2 shouldBe empty
    resultSuperReader shouldBe empty
  }

  it should "not find contracts when unknown names are used legacy" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver = Ref.Party.assertFromString("observer")
    val partyUnknown = Ref.Party.assertFromString("unknown")
    val unknownTemplate = NameTypeConRef.assertFromString("#unknown:unknown:unknown")

    val dtos = Vector(
      dtoCreateLegacy(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(1L, someTemplateId, partyObserver, first_per_sequential_id = false),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(1), 1L))
    val resultUnknownParty = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyUnknown),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultUnknownTemplate = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = Some(unknownTemplate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultUnknownPartyAndTemplate = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partyUnknown),
          templateIdO = Some(unknownTemplate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultUnknownTemplateSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = None,
          templateIdO = Some(unknownTemplate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultUnknownParty shouldBe empty
    resultUnknownTemplate shouldBe empty
    resultUnknownPartyAndTemplate shouldBe empty
    resultUnknownTemplateSuperReader shouldBe empty
  }

  it should "respect bounds and limits legacy" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtoCreateLegacy(
        offset(1),
        1L,
        hashCid("#1"),
        signatory = partySignatory,
        observer = partyObserver1,
      ),
      dtoCreateFilter(1L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(1L, someTemplateId, partyObserver1, first_per_sequential_id = false),
      dtoCreateLegacy(
        offset(2),
        2L,
        hashCid("#2"),
        signatory = partySignatory,
        observer = partyObserver2,
      ),
      dtoCreateFilter(2L, someTemplateId, partySignatory, first_per_sequential_id = true),
      dtoCreateFilter(2L, someTemplateId, partyObserver2, first_per_sequential_id = false),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val result01L2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 1L,
            limit = 2,
          )
        )
    )
    val result12L2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 1L,
            endInclusive = 2L,
            limit = 2,
          )
        )
    )
    val result02L1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 2L,
            limit = 1,
          )
        )
    )
    val result02L2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIdsLegacy(EventIdSourceLegacy.CreateStakeholder)(
          stakeholderO = Some(partySignatory),
          templateIdO = None,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 2L,
            limit = 2,
          )
        )
    )

    result01L2 should contain theSameElementsAs Vector(1L)
    result12L2 should contain theSameElementsAs Vector(2L)
    result02L1 should contain theSameElementsAs Vector(1L)
    result02L2 should contain theSameElementsAs Vector(1L, 2L)
  }

  it should "find contracts by party" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#1"),
      )(
        stakeholders = Set(partySignatory, partyObserver1)
      ),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        notPersistedContractId = hashCid("#2"),
      )(
        stakeholders = Set(partySignatory, partyObserver2)
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyObserver1),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyObserver2),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = None,
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
    resultSuperReader should contain theSameElementsAs Vector(1L, 2L)
  }

  it should "find contracts by party and by event_type" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#1"),
      )(
        stakeholders = Set(partySignatory, partyObserver1)
      ),
      dtosAssign(
        event_offset = 2,
        event_sequential_id = 2L,
        notPersistedContractId = hashCid("#2"),
      )(
        stakeholders = Set(partySignatory, partyObserver2)
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultCreate = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Create),
        )(_)(
          IdFilterInput(
            startExclusive = 0L,
            endInclusive = 10L,
          )
        )
    )
    val resultAssign = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Assign),
        )(_)(
          IdFilterInput(
            startExclusive = 0L,
            endInclusive = 10L,
          )
        )
    )
    val resultBoth = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Assign, PersistentEventType.Create),
        )(_)(
          IdFilterInput(
            startExclusive = 0L,
            endInclusive = 10L,
          )
        )
    )
    val resultForeign = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.WitnessedCreate),
        )(_)(
          IdFilterInput(
            startExclusive = 0L,
            endInclusive = 10L,
          )
        )
    )
    val resultForeignPaginationInput = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.WitnessedCreate),
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 100,
          )
        )
    )
    val resultBothLast = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Assign, PersistentEventType.Create),
        )(_)(
          PaginationLastOnlyInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 100,
          )
        )
    )
    val resultCreateLast = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set(PersistentEventType.Create),
        )(_)(
          PaginationLastOnlyInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 100,
          )
        )
    )

    resultBoth should contain theSameElementsAs Vector(1L, 2L)
    resultCreate should contain theSameElementsAs Vector(1L)
    resultAssign should contain theSameElementsAs Vector(2L)
    resultForeign should contain theSameElementsAs Vector.empty
    resultForeignPaginationInput should contain theSameElementsAs Vector(1L, 2L)
    resultBothLast should contain theSameElementsAs Vector(2L)
    resultCreateLast should contain theSameElementsAs Vector(2L)
  }

  it should "find contracts by party and template" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#1"),
      )(
        stakeholders = Set(partySignatory, partyObserver1),
        template_id = someTemplateId.toString(),
      ),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        notPersistedContractId = hashCid("#2"),
      )(
        stakeholders = Set(partySignatory, partyObserver2),
        template_id = someTemplateId.toString(),
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = Some(someTemplateId),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyObserver1),
          templateIdO = Some(someTemplateId),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyObserver2),
          templateIdO = Some(someTemplateId),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = None,
          templateIdO = Some(someTemplateId),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultSignatory should contain theSameElementsAs Vector(1L, 2L)
    resultObserver1 should contain theSameElementsAs Vector(1L)
    resultObserver2 should contain theSameElementsAs Vector(2L)
    resultSuperReader should contain theSameElementsAs Vector(1L, 2L)
  }

  it should "not find contracts when the template doesn't match" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")
    val otherTemplate = NameTypeConRef.assertFromString("#pkg-name:Mod:Template2")

    val dtos = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#1"),
      )(
        stakeholders = Set(partySignatory, partyObserver1)
      ),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        notPersistedContractId = hashCid("#2"),
      )(
        stakeholders = Set(partySignatory, partyObserver2)
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val resultSignatory = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = Some(otherTemplate),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyObserver1),
          templateIdO = Some(otherTemplate),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultObserver2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyObserver2),
          templateIdO = Some(otherTemplate),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = None,
          templateIdO = Some(otherTemplate),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultSignatory shouldBe empty
    resultObserver1 shouldBe empty
    resultObserver2 shouldBe empty
    resultSuperReader shouldBe empty
  }

  it should "not find contracts when unknown names are used" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver = Ref.Party.assertFromString("observer")
    val partyUnknown = Ref.Party.assertFromString("unknown")
    val unknownTemplate = NameTypeConRef.assertFromString("#unknown:unknown:unknown")

    val dtos = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#1"),
      )(
        stakeholders = Set(partySignatory, partyObserver)
      )
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(1), 1L))
    val resultUnknownParty = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyUnknown),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultUnknownTemplate = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = Some(unknownTemplate),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultUnknownPartyAndTemplate = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partyUnknown),
          templateIdO = Some(unknownTemplate),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )
    val resultUnknownTemplateSuperReader = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = None,
          templateIdO = Some(unknownTemplate),
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 10L,
            limit = 10,
          )
        )
    )

    resultUnknownParty shouldBe empty
    resultUnknownTemplate shouldBe empty
    resultUnknownPartyAndTemplate shouldBe empty
    resultUnknownTemplateSuperReader shouldBe empty
  }

  it should "respect bounds and limits" in {
    val partySignatory = Ref.Party.assertFromString("signatory")
    val partyObserver1 = Ref.Party.assertFromString("observer1")
    val partyObserver2 = Ref.Party.assertFromString("observer2")

    val dtos = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#1"),
      )(
        stakeholders = Set(partySignatory, partyObserver1)
      ),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        notPersistedContractId = hashCid("#2"),
      )(
        stakeholders = Set(partySignatory, partyObserver2)
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    val result01L2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 1L,
            limit = 2,
          )
        )
    )
    val result12L2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 1L,
            endInclusive = 2L,
            limit = 2,
          )
        )
    )
    val result02L1 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 2L,
            limit = 1,
          )
        )
    )
    val result02L2 = executeSql(
      backend.event.updateStreamingQueries
        .fetchEventIds(EventIdSource.ActivateStakeholder)(
          witnessO = Some(partySignatory),
          templateIdO = None,
          eventTypes = Set.empty,
        )(_)(
          PaginationInput(
            startExclusive = 0L,
            endInclusive = 2L,
            limit = 2,
          )
        )
    )

    result01L2 should contain theSameElementsAs Vector(1L)
    result12L2 should contain theSameElementsAs Vector(2L)
    result02L1 should contain theSameElementsAs Vector(1L)
    result02L2 should contain theSameElementsAs Vector(1L, 2L)
  }

  it should "populate correct maxEventSequentialId based on transaction_meta entries" in {
    val dtos = Vector(
      dtoTransactionMeta(offset(10), 1000, 1099),
      dtoTransactionMeta(offset(15), 1100, 1100),
      dtoTransactionMeta(offset(20), 1101, 1110),
      dtoTransactionMeta(offset(21), 1111, 1115),
      dtoTransactionMeta(offset(1000), 1119, 1120),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dtos, _))
    executeSql(updateLedgerEnd(offset(25), 1115))
    val maxEventSequentialId: Long => Long =
      longOffset =>
        executeSql(
          backend.event.maxEventSequentialId(Some(offset(longOffset)))
        )

    executeSql(backend.event.maxEventSequentialId(None)) shouldBe 999
    maxEventSequentialId(1) shouldBe 999
    maxEventSequentialId(2) shouldBe 999
    maxEventSequentialId(9) shouldBe 999
    maxEventSequentialId(10) shouldBe 1099
    maxEventSequentialId(11) shouldBe 1099
    maxEventSequentialId(14) shouldBe 1099
    maxEventSequentialId(15) shouldBe 1100
    maxEventSequentialId(16) shouldBe 1100
    maxEventSequentialId(19) shouldBe 1100
    maxEventSequentialId(20) shouldBe 1110
    maxEventSequentialId(21) shouldBe 1115
    maxEventSequentialId(22) shouldBe 1115
    maxEventSequentialId(24) shouldBe 1115
    maxEventSequentialId(25) shouldBe 1115
    maxEventSequentialId(26) shouldBe 1115

    executeSql(updateLedgerEnd(offset(20), 1110))
    maxEventSequentialId(20) shouldBe 1110
    maxEventSequentialId(21) shouldBe 1110
  }

  it should "return the correct trace context for create events" in {
    val traceContexts = (1 to 3)
      .flatMap(_ => List(TraceContext.empty, TraceContext.withNewTraceContext("test")(identity)))
      .map(SerializableTraceContext(_).toDamlProto.toByteArray)
    val dbDtos = Vector(
      dtoCreateLegacy(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        traceContext = traceContexts(0),
      ),
      dtoCreateLegacy(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        traceContext = traceContexts(1),
      ),
      dtoExerciseLegacy(
        offset = offset(3),
        eventSequentialId = 3L,
        consuming = false,
        contractId = hashCid("#1"),
        traceContext = traceContexts(2),
      ),
      dtoExerciseLegacy(
        offset = offset(4),
        eventSequentialId = 4L,
        consuming = false,
        contractId = hashCid("#2"),
        traceContext = traceContexts(3),
      ),
      dtoExerciseLegacy(
        offset = offset(5),
        eventSequentialId = 5L,
        consuming = true,
        contractId = hashCid("#1"),
        traceContext = traceContexts(4),
      ),
      dtoExerciseLegacy(
        offset = offset(6),
        eventSequentialId = 6L,
        consuming = true,
        contractId = hashCid("#2"),
        commandId = "command id 6",
        traceContext = traceContexts(5),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    val transactionTrees = executeSql(
      backend.event.fetchEventPayloadsLedgerEffectsLegacy(
        EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create
      )(eventSequentialIds = IdRange(1L, 6L), Some(Set.empty))
    ) ++
      executeSql(
        backend.event.fetchEventPayloadsLedgerEffectsLegacy(
          EventPayloadSourceForUpdatesLedgerEffectsLegacy.NonConsuming
        )(eventSequentialIds = IdRange(1L, 6L), Some(Set.empty))
      ) ++
      executeSql(
        backend.event.fetchEventPayloadsLedgerEffectsLegacy(
          EventPayloadSourceForUpdatesLedgerEffectsLegacy.Consuming
        )(eventSequentialIds = IdRange(1L, 6L), Some(Set.empty))
      )
    for (i <- traceContexts.indices)
      yield transactionTrees(i).traceContext should equal(Some(traceContexts(i)))

  }

  it should "return the correct keys for create events" in {
    val someKey = Some(someSerializedDamlLfValue)
    val someMaintainer = Some("maintainer")
    val someMaintainers = Array("maintainer")
    val dbDtos = Vector(
      dtoCreateLegacy(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        createKey = someKey,
        createKeyMaintainer = someMaintainer,
      ),
      dtoCreateLegacy(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        createKey = None,
        createKeyMaintainer = None,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    val transactionTrees = executeSql(
      backend.event.fetchEventPayloadsLedgerEffectsLegacy(
        EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create
      )(eventSequentialIds = IdRange(1L, 4L), Some(Set.empty))
    )

    def checkKeyAndMaintainersInTrees(
        event: RawLedgerEffectsEventLegacy,
        createKey: Option[Array[Byte]],
        createKeyMaintainers: Array[String],
    ) = event match {
      case created: RawCreatedEventLegacy =>
        created.createKeyValue should equal(createKey)
        created.createKeyMaintainers should equal(createKeyMaintainers.toSet)
      case _ => fail()
    }

    checkKeyAndMaintainersInTrees(transactionTrees(0).event, someKey, someMaintainers)
    checkKeyAndMaintainersInTrees(transactionTrees(1).event, None, Array.empty)

  }

  it should "work properly for SynchronizerOffset queries" in {
    val startRecordTimeSynchronizer = Timestamp.now()
    val startRecordTimeSynchronizer2 = Timestamp.now().addMicros(10000)
    val startPublicationTime = Timestamp.now().addMicros(100000)
    val dbDtos = Vector(
      dtoCompletion(
        offset = offset(1),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(500),
        publicationTime = startPublicationTime.addMicros(500),
      ),
      dtoTransactionMeta(
        offset = offset(3),
        synchronizerId = someSynchronizerId2,
        recordTime = startRecordTimeSynchronizer2.addMicros(500),
        publicationTime = startPublicationTime.addMicros(500),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
      dtoTransactionMeta(
        offset = offset(5),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(1000),
        publicationTime = startPublicationTime.addMicros(1000),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
      dtoCompletion(
        offset = offset(7),
        synchronizerId = someSynchronizerId2,
        recordTime = startRecordTimeSynchronizer2.addMicros(1000),
        publicationTime = startPublicationTime.addMicros(1000),
      ),
      dtoCompletion(
        offset = offset(9),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(2000),
        publicationTime = startPublicationTime.addMicros(1000),
      ),
      dtoTransactionMeta(
        offset = offset(11),
        synchronizerId = someSynchronizerId2,
        recordTime = startRecordTimeSynchronizer2.addMicros(2000),
        publicationTime = startPublicationTime.addMicros(1000),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
      dtoCompletion(
        offset = offset(13),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(3000),
        publicationTime = startPublicationTime.addMicros(2000),
      ),
      dtoTransactionMeta(
        offset = offset(15),
        synchronizerId = someSynchronizerId2,
        recordTime = startRecordTimeSynchronizer2.addMicros(3000),
        publicationTime = startPublicationTime.addMicros(2000),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(
      updateLedgerEnd(offset(12), 2L, CantonTimestamp(startPublicationTime.addMicros(1000)))
    )

    Vector(
      someSynchronizerId -> startRecordTimeSynchronizer -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      someSynchronizerId -> startRecordTimeSynchronizer.addMicros(500) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      someSynchronizerId -> startRecordTimeSynchronizer.addMicros(501) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId -> startRecordTimeSynchronizer.addMicros(1000) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId -> startRecordTimeSynchronizer.addMicros(1500) -> Some(
        SynchronizerOffset(
          offset = offset(9),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId -> startRecordTimeSynchronizer.addMicros(2000) -> Some(
        SynchronizerOffset(
          offset = offset(9),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId -> startRecordTimeSynchronizer.addMicros(2001) -> None,
      someSynchronizerId2 -> startRecordTimeSynchronizer2 -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      someSynchronizerId2 -> startRecordTimeSynchronizer2.addMicros(500) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      someSynchronizerId2 -> startRecordTimeSynchronizer2.addMicros(700) -> Some(
        SynchronizerOffset(
          offset = offset(7),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId2 -> startRecordTimeSynchronizer2.addMicros(1000) -> Some(
        SynchronizerOffset(
          offset = offset(7),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId2 -> startRecordTimeSynchronizer2.addMicros(1001) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId2 -> startRecordTimeSynchronizer2.addMicros(2000) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      someSynchronizerId2 -> startRecordTimeSynchronizer2.addMicros(2001) -> None,
    ).zipWithIndex.foreach {
      case (((synchronizerId, afterOrAtRecordTimeInclusive), expectation), index) =>
        withClue(
          s"test $index firstSynchronizerOffsetAfterOrAt($synchronizerId,$afterOrAtRecordTimeInclusive)"
        ) {
          executeSql(
            backend.event.firstSynchronizerOffsetAfterOrAt(
              synchronizerId = synchronizerId,
              afterOrAtRecordTimeInclusive = afterOrAtRecordTimeInclusive,
            )
          ) shouldBe expectation
        }
    }

    Vector(
      Some(someSynchronizerId) -> offset(1) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      Some(someSynchronizerId) -> offset(2) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      Some(someSynchronizerId) -> offset(4) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      Some(someSynchronizerId) -> offset(5) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId) -> offset(7) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId) -> offset(9) -> Some(
        SynchronizerOffset(
          offset = offset(9),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId) -> offset(10) -> Some(
        SynchronizerOffset(
          offset = offset(9),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId) -> offset(12) -> Some(
        SynchronizerOffset(
          offset = offset(9),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId) -> offset(20) -> Some(
        SynchronizerOffset(
          offset = offset(9),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId2) -> offset(3) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      Some(someSynchronizerId2) -> offset(6) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      Some(someSynchronizerId2) -> offset(7) -> Some(
        SynchronizerOffset(
          offset = offset(7),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId2) -> offset(9) -> Some(
        SynchronizerOffset(
          offset = offset(7),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId2) -> offset(11) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId2) -> offset(12) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      Some(someSynchronizerId2) -> offset(20) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      None -> offset(1) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      None -> offset(2) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      None -> offset(3) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      None -> offset(4) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      None -> offset(5) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      None -> offset(12) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      None -> offset(20) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
    ).zipWithIndex.foreach {
      case (((synchronizerIdO, beforeOrAtOffsetInclusive), expectation), index) =>
        withClue(
          s"test $index lastSynchronizerOffsetBeforeOrAt($synchronizerIdO,$beforeOrAtOffsetInclusive)"
        ) {
          executeSql(
            backend.event.lastSynchronizerOffsetBeforeOrAt(
              synchronizerIdO = synchronizerIdO,
              beforeOrAtOffsetInclusive = beforeOrAtOffsetInclusive,
            )
          ) shouldBe expectation
        }
    }

    Vector(
      offset(1) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      offset(2) -> None,
      offset(3) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      offset(5) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      offset(13) -> None,
      offset(15) -> None,
    ).zipWithIndex.foreach { case ((offset, expectation), index) =>
      withClue(s"test $index synchronizer Offset($offset)") {
        executeSql(
          backend.event.synchronizerOffset(
            offset = offset
          )
        ) shouldBe expectation
      }
    }

    Vector(
      startPublicationTime -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      startPublicationTime.addMicros(500) -> Some(
        SynchronizerOffset(
          offset = offset(1),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      startPublicationTime.addMicros(501) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      startPublicationTime.addMicros(1000) -> Some(
        SynchronizerOffset(
          offset = offset(5),
          synchronizerId = someSynchronizerId,
          recordTime = startRecordTimeSynchronizer.addMicros(1000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      startPublicationTime.addMicros(1001) -> None,
    ).zipWithIndex.foreach { case ((afterOrAtPublicationTimeInclusive, expectation), index) =>
      withClue(
        s"test $index firstSynchronizerOffsetAfterOrAtPublicationTime($afterOrAtPublicationTimeInclusive)"
      ) {
        executeSql(
          backend.event.firstSynchronizerOffsetAfterOrAtPublicationTime(
            afterOrAtPublicationTimeInclusive = afterOrAtPublicationTimeInclusive
          )
        ) shouldBe expectation
      }
    }

    Vector(
      startPublicationTime -> None,
      startPublicationTime.addMicros(499) -> None,
      startPublicationTime.addMicros(500) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      startPublicationTime.addMicros(501) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      startPublicationTime.addMicros(1000) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      startPublicationTime.addMicros(1001) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      startPublicationTime.addMicros(2000) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      startPublicationTime.addMicros(4000) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
    ).zipWithIndex.foreach { case ((beforeOrAtPublicationTimeInclusive, expectation), index) =>
      withClue(
        s"test $index lastSynchronizerOffsetBeforeOrAtPublicationTime($beforeOrAtPublicationTimeInclusive)"
      ) {
        executeSql(
          backend.event.lastSynchronizerOffsetBeforeOrAtPublicationTime(
            beforeOrAtPublicationTimeInclusive = beforeOrAtPublicationTimeInclusive
          )
        ) shouldBe expectation
      }
    }
    Vector(
      startRecordTimeSynchronizer2 -> None,
      startRecordTimeSynchronizer2.addMicros(499) -> None,
      startRecordTimeSynchronizer2.addMicros(500) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      startRecordTimeSynchronizer2.addMicros(501) -> Some(
        SynchronizerOffset(
          offset = offset(3),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(500),
          publicationTime = startPublicationTime.addMicros(500),
        )
      ),
      startRecordTimeSynchronizer2.addMicros(2000) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      startRecordTimeSynchronizer2.addMicros(2001) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      startRecordTimeSynchronizer2.addMicros(2000) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
      // never return an synchronizer offset with an offset greater than the ledger end
      startRecordTimeSynchronizer2.addMicros(4000) -> Some(
        SynchronizerOffset(
          offset = offset(11),
          synchronizerId = someSynchronizerId2,
          recordTime = startRecordTimeSynchronizer2.addMicros(2000),
          publicationTime = startPublicationTime.addMicros(1000),
        )
      ),
    ).zipWithIndex.foreach { case ((beforeOrAtRecordTime, expectation), index) =>
      withClue(
        s"test $index lastSynchronizerOffsetBeforeOrAtRecordTime($beforeOrAtRecordTime)"
      ) {
        executeSql(
          backend.event.lastSynchronizerOffsetBeforeOrAtRecordTime(
            synchronizerId = someSynchronizerId2,
            beforeOrAtRecordTimeInclusive = beforeOrAtRecordTime,
          )
        ) shouldBe expectation
      }
    }
  }

  it should "work with multiple transaction_metadata entries sharing the same record_time - firstSynchronizerOffsetAfterOrAt" in {
    val startRecordTimeSynchronizer = Timestamp.now().addMicros(10000)
    val startPublicationTime = Timestamp.now().addMicros(100000)
    val dbDtos = Vector(
      dtoTransactionMeta(
        offset = offset(3),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(500),
        publicationTime = startPublicationTime.addMicros(500),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
      dtoTransactionMeta(
        offset = offset(7),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(550),
        publicationTime = startPublicationTime.addMicros(700),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
      dtoTransactionMeta(
        offset = offset(9),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(550),
        publicationTime = startPublicationTime.addMicros(800),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
      // insertion is out of order for this entry, for testing result is not reliant on insertion order, but rather on index order (regression for bug #26434)
      dtoTransactionMeta(
        offset = offset(5),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(550),
        publicationTime = startPublicationTime.addMicros(600),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
      dtoTransactionMeta(
        offset = offset(11),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(600),
        publicationTime = startPublicationTime.addMicros(900),
        event_sequential_id_first = 1,
        event_sequential_id_last = 1,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(
      updateLedgerEnd(offset(12), 2L, CantonTimestamp(startPublicationTime.addMicros(1000)))
    )

    executeSql(
      backend.event.firstSynchronizerOffsetAfterOrAt(
        synchronizerId = someSynchronizerId,
        afterOrAtRecordTimeInclusive = startRecordTimeSynchronizer.addMicros(540),
      )
    ).value.offset shouldBe offset(5)
    executeSql(
      backend.event.firstSynchronizerOffsetAfterOrAt(
        synchronizerId = someSynchronizerId,
        afterOrAtRecordTimeInclusive = startRecordTimeSynchronizer.addMicros(550),
      )
    ).value.offset shouldBe offset(5)
  }

  it should "work with multiple completion entries sharing the same record_time - firstSynchronizerOffsetAfterOrAt" in {
    val startRecordTimeSynchronizer = Timestamp.now().addMicros(10000)
    val startPublicationTime = Timestamp.now().addMicros(100000)
    val dbDtos = Vector(
      dtoCompletion(
        offset = offset(3),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(500),
        publicationTime = startPublicationTime.addMicros(500),
      ),
      dtoCompletion(
        offset = offset(7),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(550),
        publicationTime = startPublicationTime.addMicros(700),
      ),
      dtoCompletion(
        offset = offset(9),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(550),
        publicationTime = startPublicationTime.addMicros(800),
      ),
      // insertion is out of order for this entry, for testing result is not reliant on insertion order, but rather on index order (regression for bug #26434)
      dtoCompletion(
        offset = offset(5),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(550),
        publicationTime = startPublicationTime.addMicros(600),
      ),
      dtoCompletion(
        offset = offset(11),
        synchronizerId = someSynchronizerId,
        recordTime = startRecordTimeSynchronizer.addMicros(600),
        publicationTime = startPublicationTime.addMicros(900),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(
      updateLedgerEnd(offset(12), 2L, CantonTimestamp(startPublicationTime.addMicros(1000)))
    )

    executeSql(
      backend.event.firstSynchronizerOffsetAfterOrAt(
        synchronizerId = someSynchronizerId,
        afterOrAtRecordTimeInclusive = startRecordTimeSynchronizer.addMicros(540),
      )
    ).value.offset shouldBe offset(5)
    executeSql(
      backend.event.firstSynchronizerOffsetAfterOrAt(
        synchronizerId = someSynchronizerId,
        afterOrAtRecordTimeInclusive = startRecordTimeSynchronizer.addMicros(550),
      )
    ).value.offset shouldBe offset(5)
  }

  it should "work properly for archivals query legacy" in {
    val dbDtos = Vector(
      dtoExerciseLegacy(
        offset = offset(5),
        eventSequentialId = 14,
        consuming = true,
        contractId = hashCid("#1"),
      ),
      dtoExerciseLegacy(
        offset = offset(5),
        eventSequentialId = 18,
        consuming = true,
        contractId = hashCid("#2"),
      ),
      dtoTransactionMeta(
        offset = offset(5),
        synchronizerId = someSynchronizerId2,
        event_sequential_id_first = 10,
        event_sequential_id_last = 20,
      ),
      dtoExerciseLegacy(
        offset = offset(15),
        eventSequentialId = 118,
        consuming = true,
        contractId = hashCid("#3"),
      ),
      dtoExerciseLegacy(
        offset = offset(15),
        eventSequentialId = 119,
        consuming = true,
        contractId = hashCid("#4"),
      ),
      dtoTransactionMeta(
        offset = offset(15),
        synchronizerId = someSynchronizerId2,
        event_sequential_id_first = 110,
        event_sequential_id_last = 120,
      ),
      dtoExerciseLegacy(
        offset = offset(25),
        eventSequentialId = 211,
        consuming = true,
        contractId = hashCid("#5"),
      ),
      dtoExerciseLegacy(
        offset = offset(25),
        eventSequentialId = 212,
        consuming = false,
        contractId = hashCid("#55"),
      ),
      dtoExerciseLegacy(
        offset = offset(25),
        eventSequentialId = 214,
        consuming = true,
        contractId = hashCid("#6"),
      ),
      dtoTransactionMeta(
        offset = offset(25),
        synchronizerId = someSynchronizerId2,
        event_sequential_id_first = 210,
        event_sequential_id_last = 220,
      ),
      dtoExerciseLegacy(
        offset = offset(35),
        eventSequentialId = 315,
        consuming = true,
        contractId = hashCid("#7"),
      ),
      dtoTransactionMeta(
        offset = offset(35),
        synchronizerId = someSynchronizerId2,
        event_sequential_id_first = 310,
        event_sequential_id_last = 320,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(
      updateLedgerEnd(offset(25), 220L)
    )

    Vector(
      None -> offset(4) -> Set(),
      None -> offset(5) -> Set(
        hashCid("#1"),
        hashCid("#2"),
      ),
      None -> offset(10) -> Set(
        hashCid("#1"),
        hashCid("#2"),
      ),
      None -> offset(15) -> Set(
        hashCid("#1"),
        hashCid("#2"),
        hashCid("#3"),
        hashCid("#4"),
      ),
      None -> offset(25) -> Set(
        hashCid("#1"),
        hashCid("#2"),
        hashCid("#3"),
        hashCid("#4"),
        hashCid("#5"),
        hashCid("#6"),
      ),
      None -> offset(1000) -> Set(
        hashCid("#1"),
        hashCid("#2"),
        hashCid("#3"),
        hashCid("#4"),
        hashCid("#5"),
        hashCid("#6"),
      ),
      Some(offset(4)) -> offset(1000) -> Set(
        hashCid("#1"),
        hashCid("#2"),
        hashCid("#3"),
        hashCid("#4"),
        hashCid("#5"),
        hashCid("#6"),
      ),
      Some(offset(5)) -> offset(1000) -> Set(
        hashCid("#3"),
        hashCid("#4"),
        hashCid("#5"),
        hashCid("#6"),
      ),
      Some(offset(6)) -> offset(1000) -> Set(
        hashCid("#3"),
        hashCid("#4"),
        hashCid("#5"),
        hashCid("#6"),
      ),
      Some(offset(15)) -> offset(1000) -> Set(
        hashCid("#5"),
        hashCid("#6"),
      ),
      Some(offset(15)) -> offset(15) -> Set(
      ),
      Some(offset(6)) -> offset(25) -> Set(
        hashCid("#3"),
        hashCid("#4"),
        hashCid("#5"),
        hashCid("#6"),
      ),
      Some(offset(6)) -> offset(24) -> Set(
        hashCid("#3"),
        hashCid("#4"),
      ),
    ).zipWithIndex.foreach { case (((fromExclusive, toInclusive), expectation), index) =>
      withClue(
        s"test $index archivals($fromExclusive,$toInclusive)"
      ) {
        executeSql(
          backend.event.archivalsLegacy(
            fromExclusive = fromExclusive,
            toInclusive = toInclusive,
          )
        ) shouldBe expectation
      }
    }
  }

  it should "work properly for prunableContract" in {
    val dbDtos = Vector(
      dtosConsumingExercise(
        event_offset = 5,
        event_sequential_id = 14,
        internal_contract_id = Some(1),
      ),
      dtosConsumingExercise(
        event_offset = 5,
        event_sequential_id = 18,
        internal_contract_id = Some(2),
      ),
      Vector(
        dtoTransactionMeta(
          offset = offset(5),
          synchronizerId = someSynchronizerId2,
          event_sequential_id_first = 10,
          event_sequential_id_last = 20,
        )
      ),
      dtosConsumingExercise(
        event_offset = 15,
        event_sequential_id = 118,
        internal_contract_id = Some(3),
      ),
      dtosConsumingExercise(
        event_offset = 15,
        event_sequential_id = 119,
        internal_contract_id = Some(4),
      ),
      Vector(
        dtoTransactionMeta(
          offset = offset(15),
          synchronizerId = someSynchronizerId2,
          event_sequential_id_first = 110,
          event_sequential_id_last = 120,
        )
      ),
      dtosConsumingExercise(
        event_offset = 25,
        event_sequential_id = 211,
        internal_contract_id = Some(5),
      ),
      dtosUnassign(
        event_offset = 25,
        event_sequential_id = 212,
        internal_contract_id = Some(55),
      ),
      dtosConsumingExercise(
        event_offset = 25,
        event_sequential_id = 214,
        internal_contract_id = Some(6),
      ),
      dtosCreate(
        event_offset = 25,
        event_sequential_id = 215,
        internal_contract_id = 61,
      )(),
      dtosAssign(
        event_offset = 25,
        event_sequential_id = 216,
        internal_contract_id = 62,
      )(),
      dtosWitnessedCreate(
        event_offset = 25,
        event_sequential_id = 217,
        internal_contract_id = 63,
      )(),
      dtosWitnessedExercised(
        event_offset = 25,
        event_sequential_id = 218,
        internal_contract_id = Some(64),
      ),
      dtosWitnessedExercised(
        event_offset = 25,
        consuming = false,
        event_sequential_id = 219,
        internal_contract_id = Some(65),
      ),
      Vector(
        dtoTransactionMeta(
          offset = offset(25),
          synchronizerId = someSynchronizerId2,
          event_sequential_id_first = 210,
          event_sequential_id_last = 220,
        )
      ),
      dtosConsumingExercise(
        event_offset = 35,
        event_sequential_id = 315,
        internal_contract_id = Some(7),
      ),
      Vector(
        dtoTransactionMeta(
          offset = offset(35),
          synchronizerId = someSynchronizerId2,
          event_sequential_id_first = 310,
          event_sequential_id_last = 320,
        )
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(
      updateLedgerEnd(offset(25), 220L)
    )

    Vector(
      None -> offset(4) -> Set(),
      None -> offset(5) -> Set(
        1,
        2,
      ),
      None -> offset(10) -> Set(
        1,
        2,
      ),
      None -> offset(15) -> Set(
        1,
        2,
        3,
        4,
      ),
      None -> offset(25) -> Set(
        1, 2, 3, 4, 5, 6, 63,
      ),
      None -> offset(1000) -> Set(
        1, 2, 3, 4, 5, 6, 63,
      ),
      Some(offset(4)) -> offset(1000) -> Set(
        1, 2, 3, 4, 5, 6, 63,
      ),
      Some(offset(5)) -> offset(1000) -> Set(
        3, 4, 5, 6, 63,
      ),
      Some(offset(6)) -> offset(1000) -> Set(
        3, 4, 5, 6, 63,
      ),
      Some(offset(15)) -> offset(1000) -> Set(
        5,
        6,
        63,
      ),
      Some(offset(15)) -> offset(15) -> Set(
      ),
      Some(offset(6)) -> offset(25) -> Set(
        3, 4, 5, 6, 63,
      ),
      Some(offset(6)) -> offset(24) -> Set(
        3,
        4,
      ),
    ).zipWithIndex.foreach { case (((fromExclusive, toInclusive), expectation), index) =>
      withClue(
        s"test $index archivals($fromExclusive,$toInclusive)"
      ) {
        executeSql(
          backend.event.prunableContracts(
            fromExclusive = fromExclusive,
            toInclusive = toInclusive,
          )
        ) shouldBe expectation
      }
    }
  }

  it should "fetch correctly AcsDelta and LedgerEffects Raw events" in {
    implicit val eq: Equality[RawThinAcsDeltaEvent] = caseClassArrayEq
    implicit val eq2: Equality[RawThinLedgerEffectsEvent] = caseClassArrayEq

    val dbDtos = Vector(
      dtosCreate(event_sequential_id = 1L)(),
      dtosAssign(event_sequential_id = 2L)(),
      dtosConsumingExercise(event_sequential_id = 3L),
      dtosUnassign(event_sequential_id = 4L),
      dtosWitnessedCreate(event_sequential_id = 5L)(),
      dtosWitnessedExercised(event_sequential_id = 6L),
      dtosWitnessedExercised(event_sequential_id = 7L, consuming = false),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(
      updateLedgerEnd(offset(10000), 10000L)
    )

    executeSql(
      backend.event.fetchEventPayloadsAcsDelta(EventPayloadSourceForUpdatesAcsDelta.Activate)(
        eventSequentialIds = SequentialIdBatch.IdRange(0, 100),
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
      )
    ).toList should contain theSameElementsInOrderAs List(
      RawThinCreatedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 1L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set.empty,
          internalContractId = 10L,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 0L,
          acsDelta = true,
        ),
      ),
      RawThinAssignEvent(
        reassignmentProperties = ReassignmentProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 2L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          reassignmentId = "0012345678",
          submitter = Some("submitter1"),
          reassignmentCounter = 345,
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set.empty,
          internalContractId = 10L,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 345,
          acsDelta = true,
        ),
        sourceSynchronizerId = someSynchronizerId2.toProtoPrimitive,
      ),
    )
    executeSql(
      backend.event.fetchEventPayloadsAcsDelta(EventPayloadSourceForUpdatesAcsDelta.Deactivate)(
        eventSequentialIds = SequentialIdBatch.IdRange(0, 100),
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
      )
    ).toList should contain theSameElementsInOrderAs List(
      RawArchivedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 3L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        filteredStakeholderParties = Set("stakeholder1"),
        ledgerEffectiveTime = Timestamp.assertFromLong(123456),
      ),
      RawUnassignEvent(
        reassignmentProperties = ReassignmentProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 4L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          reassignmentId = "0012345678",
          submitter = Some("submitter1"),
          reassignmentCounter = 345,
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        filteredStakeholderParties = Set("stakeholder1"),
        assignmentExclusivity = Some(Timestamp.assertFromLong(111333)),
        targetSynchronizerId = someSynchronizerId2.toProtoPrimitive,
      ),
    )

    executeSql(
      backend.event.fetchEventPayloadsLedgerEffects(
        EventPayloadSourceForUpdatesLedgerEffects.Activate
      )(
        eventSequentialIds = SequentialIdBatch.IdRange(0, 100),
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
      )
    ).toList should contain theSameElementsInOrderAs List(
      RawThinCreatedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 1L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set("witness1"),
          internalContractId = 10L,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 0L,
          acsDelta = true,
        ),
      ),
      RawThinAssignEvent(
        reassignmentProperties = ReassignmentProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 2L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          reassignmentId = "0012345678",
          submitter = Some("submitter1"),
          reassignmentCounter = 345,
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set.empty,
          internalContractId = 10L,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 345,
          acsDelta = true,
        ),
        sourceSynchronizerId = someSynchronizerId2.toProtoPrimitive,
      ),
    )
    executeSql(
      backend.event.fetchEventPayloadsLedgerEffects(
        EventPayloadSourceForUpdatesLedgerEffects.Deactivate
      )(
        eventSequentialIds = SequentialIdBatch.IdRange(0, 100),
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
      )
    ).toList should contain theSameElementsInOrderAs List(
      RawExercisedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 3L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        exerciseConsuming = true,
        exerciseChoice = ChoiceName.assertFromString("choice"),
        exerciseChoiceInterface = Option(Identifier.assertFromString("in:ter:face")),
        exerciseArgument = Array(1, 2, 3),
        exerciseArgumentCompression = Some(1),
        exerciseResult = Some(Array(2, 3, 4)),
        exerciseResultCompression = Some(2),
        exerciseActors = Set("actor1", "actor2"),
        exerciseLastDescendantNodeId = 3,
        filteredAdditionalWitnessParties = Set("witness1"),
        filteredStakeholderParties = Set("stakeholder1"),
        ledgerEffectiveTime = Timestamp.assertFromLong(123456),
        acsDelta = true,
      ),
      RawUnassignEvent(
        reassignmentProperties = ReassignmentProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 4L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          reassignmentId = "0012345678",
          submitter = Some("submitter1"),
          reassignmentCounter = 345,
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        filteredStakeholderParties = Set("stakeholder1"),
        assignmentExclusivity = Some(Timestamp.assertFromLong(111333)),
        targetSynchronizerId = someSynchronizerId2.toProtoPrimitive,
      ),
    )
    executeSql(
      backend.event.fetchEventPayloadsLedgerEffects(
        EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
      )(
        eventSequentialIds = SequentialIdBatch.IdRange(0, 100),
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
      )
    ).toList should contain theSameElementsInOrderAs List(
      RawThinCreatedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 5L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set("witness1"),
          internalContractId = 10L,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 0L,
          acsDelta = false,
        ),
      ),
      RawExercisedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 6L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        exerciseConsuming = true,
        exerciseChoice = ChoiceName.assertFromString("choice"),
        exerciseChoiceInterface = Option(Identifier.assertFromString("in:ter:face")),
        exerciseArgument = Array(1, 2, 3),
        exerciseArgumentCompression = Some(1),
        exerciseResult = Some(Array(2, 3, 4)),
        exerciseResultCompression = Some(2),
        exerciseActors = Set("actor1", "actor2"),
        exerciseLastDescendantNodeId = 3,
        filteredAdditionalWitnessParties = Set("witness1"),
        filteredStakeholderParties = Set.empty,
        ledgerEffectiveTime = Timestamp.assertFromLong(123456),
        acsDelta = false,
      ),
      RawExercisedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 7L,
            offset = 10L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        exerciseConsuming = false,
        exerciseChoice = ChoiceName.assertFromString("choice"),
        exerciseChoiceInterface = Option(Identifier.assertFromString("in:ter:face")),
        exerciseArgument = Array(1, 2, 3),
        exerciseArgumentCompression = Some(1),
        exerciseResult = Some(Array(2, 3, 4)),
        exerciseResultCompression = Some(2),
        exerciseActors = Set("actor1", "actor2"),
        exerciseLastDescendantNodeId = 3,
        filteredAdditionalWitnessParties = Set("witness1"),
        filteredStakeholderParties = Set.empty,
        ledgerEffectiveTime = Timestamp.assertFromLong(123456),
        acsDelta = false,
      ),
    )
  }

  it should "fetch correctly EventQueryService results" in {
    implicit val eq: Equality[RawThinCreatedEvent] = caseClassArrayEq
    implicit val eq2: Equality[RawArchivedEvent] = caseClassArrayEq

    val createOnlyInternalContractId = 10L
    val createAndArchiveInternalContractId = 11L
    val transientInternalContractId = 12L
    val divulgedInternalContractId = 13L

    val dbDtos = Vector(
      dtosCreate(
        event_sequential_id = 1L,
        internal_contract_id = createOnlyInternalContractId,
        event_offset = 116,
      )(),
      dtosCreate(
        event_sequential_id = 2L,
        internal_contract_id = createAndArchiveInternalContractId,
        event_offset = 136,
      )(),
      dtosConsumingExercise(
        event_sequential_id = 3L,
        internal_contract_id = Some(createAndArchiveInternalContractId),
        event_offset = 146,
      ),
      dtosWitnessedCreate(
        event_sequential_id = 4L,
        internal_contract_id = transientInternalContractId,
        event_offset = 156,
      )(),
      dtosWitnessedExercised(
        event_sequential_id = 5L,
        internal_contract_id = Some(transientInternalContractId),
        event_offset = 156,
      ),
      dtosWitnessedCreate(
        event_sequential_id = 6L,
        internal_contract_id = divulgedInternalContractId,
        event_offset = 160,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(
      updateLedgerEnd(offset(10000), 10000L)
    )

    val createOnly = executeSql(
      backend.event.eventReaderQueries.fetchContractIdEvents(
        internalContractId = createOnlyInternalContractId,
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
        endEventSequentialId = 10000L,
      )
    )
    createOnly._1.value should equal(
      RawThinCreatedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 1L,
            offset = 116L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set.empty,
          internalContractId = createOnlyInternalContractId,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 0L,
          acsDelta = true,
        ),
      )
    )
    createOnly._2 shouldBe None

    val createAndArchiveOnly = executeSql(
      backend.event.eventReaderQueries.fetchContractIdEvents(
        internalContractId = createAndArchiveInternalContractId,
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
        endEventSequentialId = 10000L,
      )
    )
    createAndArchiveOnly._1.value should equal(
      RawThinCreatedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 2L,
            offset = 136L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set.empty,
          internalContractId = createAndArchiveInternalContractId,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 0L,
          acsDelta = true,
        ),
      )
    )
    createAndArchiveOnly._2.value should equal(
      RawArchivedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 3L,
            offset = 146L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        filteredStakeholderParties = Set("stakeholder1"),
        ledgerEffectiveTime = Timestamp.assertFromLong(123456),
      )
    )

    val transient = executeSql(
      backend.event.eventReaderQueries.fetchContractIdEvents(
        internalContractId = transientInternalContractId,
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
        endEventSequentialId = 10000L,
      )
    )
    transient._1.value should equal(
      RawThinCreatedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 4L,
            offset = 156L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        thinCreatedEventProperties = ThinCreatedEventProperties(
          representativePackageId = "representativepackage",
          filteredAdditionalWitnessParties = Set("witness1"),
          internalContractId = transientInternalContractId,
          requestingParties = Some(Set("witness1", "stakeholder1", "submitter1", "actor1")),
          reassignmentCounter = 0L,
          acsDelta = false,
        ),
      )
    )
    transient._2.value should equal(
      RawArchivedEvent(
        transactionProperties = TransactionProperties(
          commonEventProperties = CommonEventProperties(
            eventSequentialId = 5L,
            offset = 156L,
            nodeId = 15,
            workflowId = Some("workflow-id"),
            synchronizerId = someSynchronizerId.toProtoPrimitive,
          ),
          commonUpdateProperties = CommonUpdateProperties(
            updateId = TestUpdateId("update").toHexString,
            commandId = Some("command-id"),
            traceContext = serializableTraceContext,
            recordTime = Timestamp.assertFromLong(100L),
          ),
          externalTransactionHash = Some(someExternalTransactionHashBinary),
        ),
        contractId = hashCid("c1"),
        templateId = Identifier
          .assertFromString("package:pl:ate")
          .toFullIdentifier(PackageName.assertFromString("tem")),
        filteredStakeholderParties = Set.empty,
        ledgerEffectiveTime = Timestamp.assertFromLong(123456),
      )
    )

    val divulged = executeSql(
      backend.event.eventReaderQueries.fetchContractIdEvents(
        internalContractId = divulgedInternalContractId,
        requestingParties =
          Some(Set("witness1", "stakeholder1", "submitter1", "actor1").map(Party.assertFromString)),
        endEventSequentialId = 10000L,
      )
    )
    divulged shouldBe (None -> None)
  }
}
