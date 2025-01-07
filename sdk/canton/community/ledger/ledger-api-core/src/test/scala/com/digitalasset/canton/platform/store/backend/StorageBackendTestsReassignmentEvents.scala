// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawCreatedEvent,
  UnassignProperties,
}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Ref, Time}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsReassignmentEvents
    extends Matchers
    with OptionValues
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*
  import DbDtoEq.*

  private val emptyTraceContext =
    SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray

  behavior of "event id fetching from filter tables"

  it should "return the correct event ids for assign event stakeholder" in {
    val dbDtos = Vector(
      DbDto.IdFilterAssignStakeholder(1, someTemplateId.toString, someParty),
      DbDto.IdFilterAssignStakeholder(1, someTemplateId.toString, someParty2),
      DbDto.IdFilterAssignStakeholder(2, someTemplateId2.toString, someParty),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1, 2)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty2),
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = None,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1, 1, 2)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 1,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = Some(someTemplateId2),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(2)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 1,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = None,
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 1,
        limit = 10,
      )
    ) shouldBe Vector(1, 1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholderO = None,
        templateId = Some(someTemplateId2),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(2)

  }

  it should "return the correct event ids for unassign event stakeholder" in {
    val dbDtos = Vector(
      DbDto.IdFilterUnassignStakeholder(1, someTemplateId.toString, someParty),
      DbDto.IdFilterUnassignStakeholder(1, someTemplateId.toString, someParty2),
      DbDto.IdFilterUnassignStakeholder(2, someTemplateId2.toString, someParty),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1, 2)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty2),
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = None,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1, 1, 2)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 1,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1, 2)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = Some(someTemplateId2),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(2)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = None,
        templateId = Some(someTemplateId2),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(2)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = Some(someParty),
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 1,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = None,
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 1,
        limit = 10,
      )
    ) shouldBe Vector(1, 1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholderO = None,
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 1,
        limit = 1,
      )
    ) shouldBe Vector(1)
  }

  behavior of "event batch fetching"

  it should "return the correct assign events" in {
    val dbDtos = Vector(
      dtoAssign(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        commandId = "command id 1",
      ),
      dtoAssign(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    val result = executeSql(
      backend.event.assignEventBatch(
        eventSequentialIds = List(1L, 2L),
        allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
      )
    )

    result
      .map(original =>
        original.copy(
          event = original.event.copy(
            rawCreatedEvent = rawCreatedEventHasExpectedCreateArgumentAndDriverMetadata(
              original.event.rawCreatedEvent,
              someSerializedDamlLfValue,
              someDriverMetadataBytes,
            )
          ),
          traceContext = hasSameTraceContext(original.traceContext, Some(emptyTraceContext)),
          eventSequentialId = 0L,
        )
      ) shouldBe (
      Vector(
        Entry(
          commandId = Some("command id 1"),
          workflowId = Some("workflow_id"),
          offset = 1,
          traceContext = Some(emptyTraceContext),
          recordTime = someTime,
          updateId = offset(1).toDecimalString,
          eventSequentialId = 0L,
          ledgerEffectiveTime = Timestamp.MinValue,
          synchronizerId = "x::targetdomain",
          event = EventStorageBackend.RawAssignEvent(
            sourceSynchronizerId = "x::sourcedomain",
            targetSynchronizerId = "x::targetdomain",
            unassignId = "123456789",
            submitter = Option(someParty),
            reassignmentCounter = 1000L,
            rawCreatedEvent = RawCreatedEvent(
              updateId = offset(1).toDecimalString,
              offset = 1,
              nodeIndex = 0,
              contractId = hashCid("#1").coid,
              templateId = someTemplateId,
              packageName = somePackageName,
              packageVersion = Some(somePackageVersion),
              witnessParties = Set("signatory"),
              signatories = Set("signatory"),
              observers = Set("observer"),
              createArgument = someSerializedDamlLfValue,
              createArgumentCompression = Some(123),
              createKeyValue = None,
              createKeyMaintainers = Set.empty,
              createKeyValueCompression = Some(456),
              ledgerEffectiveTime = someTime,
              createKeyHash = None,
              driverMetadata = someDriverMetadataBytes,
            ),
          ),
        ),
        Entry(
          commandId = Some("command id 2"),
          workflowId = Some("workflow_id"),
          offset = 2,
          traceContext = Some(emptyTraceContext),
          recordTime = someTime,
          updateId = offset(2).toDecimalString,
          eventSequentialId = 0L,
          ledgerEffectiveTime = Timestamp.MinValue,
          synchronizerId = "x::targetdomain",
          event = EventStorageBackend.RawAssignEvent(
            sourceSynchronizerId = "x::sourcedomain",
            targetSynchronizerId = "x::targetdomain",
            unassignId = "123456789",
            submitter = Option(someParty),
            reassignmentCounter = 1000L,
            rawCreatedEvent = RawCreatedEvent(
              updateId = offset(2).toDecimalString,
              offset = 2,
              nodeIndex = 0,
              contractId = hashCid("#2").coid,
              templateId = someTemplateId,
              packageName = somePackageName,
              packageVersion = Some(somePackageVersion),
              witnessParties = Set("signatory"),
              signatories = Set("signatory"),
              observers = Set("observer"),
              createArgument = someSerializedDamlLfValue,
              createArgumentCompression = Some(123),
              createKeyValue = None,
              createKeyMaintainers = Set.empty,
              createKeyValueCompression = Some(456),
              ledgerEffectiveTime = someTime,
              createKeyHash = None,
              driverMetadata = someDriverMetadataBytes,
            ),
          ),
        ),
      )
    )
  }

  it should "return the correct unassign events" in {
    val dbDtos = Vector(
      dtoUnassign(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        commandId = "command id 1",
      ),
      dtoUnassign(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    executeSql(
      backend.event.unassignEventBatch(
        eventSequentialIds = List(1L, 2L),
        allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
      )
    ).map(original =>
      original.copy(
        traceContext = hasSameTraceContext(original.traceContext, Some(emptyTraceContext)),
        eventSequentialId = 0L,
      )
    ) shouldBe Vector(
      Entry(
        commandId = Some("command id 1"),
        workflowId = Some("workflow_id"),
        offset = 1,
        traceContext = Some(emptyTraceContext),
        recordTime = someTime,
        updateId = offset(1).toDecimalString,
        eventSequentialId = 0L,
        ledgerEffectiveTime = Timestamp.MinValue,
        synchronizerId = "x::sourcedomain",
        event = EventStorageBackend.RawUnassignEvent(
          sourceSynchronizerId = "x::sourcedomain",
          targetSynchronizerId = "x::targetdomain",
          unassignId = "123456789",
          submitter = Option(someParty),
          reassignmentCounter = 1000L,
          contractId = hashCid("#1").coid,
          templateId = someTemplateId,
          packageName = somePackageName,
          witnessParties = Set("signatory"),
          assignmentExclusivity = Some(Time.Timestamp.assertFromLong(11111)),
        ),
      ),
      Entry(
        commandId = Some("command id 2"),
        workflowId = Some("workflow_id"),
        offset = 2,
        traceContext = Some(emptyTraceContext),
        recordTime = someTime,
        updateId = offset(2).toDecimalString,
        eventSequentialId = 0L,
        ledgerEffectiveTime = Timestamp.MinValue,
        synchronizerId = "x::sourcedomain",
        event = EventStorageBackend.RawUnassignEvent(
          sourceSynchronizerId = "x::sourcedomain",
          targetSynchronizerId = "x::targetdomain",
          unassignId = "123456789",
          submitter = Option(someParty),
          reassignmentCounter = 1000L,
          contractId = hashCid("#2").coid,
          templateId = someTemplateId,
          packageName = somePackageName,
          witnessParties = Set("signatory"),
          assignmentExclusivity = Some(Time.Timestamp.assertFromLong(11111)),
        ),
      ),
    )
  }

  it should "return the correct trace context for assign events" in {
    TraceContext.withNewTraceContext { aTraceContext =>
      val serializableTraceContext = SerializableTraceContext(aTraceContext).toDamlProto.toByteArray
      val dbDtos = Vector(
        dtoAssign(
          offset = offset(1),
          eventSequentialId = 1L,
          contractId = hashCid("#1"),
          commandId = "command id 1",
          traceContext = emptyTraceContext,
        ),
        dtoAssign(
          offset = offset(2),
          eventSequentialId = 2L,
          contractId = hashCid("#2"),
          commandId = "command id 2",
          traceContext = serializableTraceContext,
        ),
      )

      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
      executeSql(ingest(dbDtos, _))
      executeSql(updateLedgerEnd(offset(2), 2L))

      val assignments = executeSql(
        backend.event.assignEventBatch(
          eventSequentialIds = List(1L, 2L),
          allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
        )
      )
      assignments.head.traceContext should equal(None)
      assignments(1).traceContext should equal(Some(serializableTraceContext))
    }
  }

  it should "return the correct trace context for unassign events" in {
    TraceContext.withNewTraceContext { aTraceContext =>
      val serializableTraceContext = SerializableTraceContext(aTraceContext).toDamlProto.toByteArray
      val dbDtos = Vector(
        dtoUnassign(
          offset = offset(1),
          eventSequentialId = 1L,
          contractId = hashCid("#1"),
          commandId = "command id 1",
          traceContext = emptyTraceContext,
        ),
        dtoUnassign(
          offset = offset(2),
          eventSequentialId = 2L,
          contractId = hashCid("#2"),
          commandId = "command id 2",
          traceContext = serializableTraceContext,
        ),
      )

      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
      executeSql(ingest(dbDtos, _))
      executeSql(updateLedgerEnd(offset(2), 2L))

      val unassignments = executeSql(
        backend.event.unassignEventBatch(
          eventSequentialIds = List(1L, 2L),
          allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
        )
      )
      unassignments.head.traceContext should equal(None)
      unassignments(1).traceContext should equal(Some(serializableTraceContext))
    }
  }

  behavior of "active contract batch lookup for contracts"

  it should "return the correct active contracts from create events, and only if not archived/unassigned" in {
    val dbDtos = Vector(
      dtoCreate(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        commandId = "command id 1",
        synchronizerId = "x::domain1",
        driverMetadata = someDriverMetadataBytes,
      ),
      dtoCreate(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
        synchronizerId = "x::domain1",
        driverMetadata = someDriverMetadataBytes,
      ),
      dtoExercise(
        offset = offset(3),
        eventSequentialId = 3L,
        consuming = true,
        contractId = hashCid("#2"),
        synchronizerId = "x::domain2",
      ),
      dtoUnassign(
        offset = offset(4),
        eventSequentialId = 4L,
        contractId = hashCid("#2"),
        sourceSynchronizerId = "x::domain2",
      ),
      dtoExercise(
        offset = offset(10),
        eventSequentialId = 10L,
        consuming = true,
        contractId = hashCid("#2"),
        synchronizerId = "x::domain1",
      ),
      dtoUnassign(
        offset = offset(11),
        eventSequentialId = 11L,
        contractId = hashCid("#1"),
        sourceSynchronizerId = "x::domain1",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(11), 11L))

    executeSql(
      backend.event.activeContractCreateEventBatch(
        eventSequentialIds = List(1, 2),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 6,
      )
    ).map(activeContract =>
      activeContract.copy(rawCreatedEvent =
        rawCreatedEventHasExpectedCreateArgumentAndDriverMetadata(
          activeContract.rawCreatedEvent,
          someSerializedDamlLfValue,
          someDriverMetadataBytes,
        )
      )
    ) shouldBe Vector(
      EventStorageBackend.RawActiveContract(
        workflowId = Some("workflow_id"),
        synchronizerId = "x::domain1",
        reassignmentCounter = 0L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(1).toDecimalString,
          offset = 1,
          nodeIndex = 0,
          contractId = hashCid("#1").coid,
          templateId = someTemplateId,
          packageName = somePackageName,
          packageVersion = Some(somePackageVersion),
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          createArgument = someSerializedDamlLfValue,
          createArgumentCompression = None,
          createKeyValue = None,
          createKeyMaintainers = Set.empty,
          createKeyValueCompression = None,
          ledgerEffectiveTime = someTime,
          createKeyHash = None,
          driverMetadata = someDriverMetadataBytes,
        ),
        eventSequentialId = 1L,
      ),
      EventStorageBackend.RawActiveContract(
        workflowId = Some("workflow_id"),
        synchronizerId = "x::domain1",
        reassignmentCounter = 0L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(2).toDecimalString,
          offset = 2,
          nodeIndex = 0,
          contractId = hashCid("#2").coid,
          templateId = someTemplateId,
          packageName = somePackageName,
          packageVersion = Some(somePackageVersion),
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          createArgument = someSerializedDamlLfValue,
          createArgumentCompression = None,
          createKeyValue = None,
          createKeyMaintainers = Set.empty,
          createKeyValueCompression = None,
          ledgerEffectiveTime = someTime,
          createKeyHash = None,
          driverMetadata = someDriverMetadataBytes,
        ),
        eventSequentialId = 2L,
      ),
    )

    // same query as first to double check equality predicate
    executeSql(
      backend.event.activeContractCreateEventBatch(
        eventSequentialIds = List(1, 2),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 6,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(1L, 2L).map(x => offset(x).toDecimalString)

    // archive in the same domain renders it inactive
    executeSql(
      backend.event.activeContractCreateEventBatch(
        eventSequentialIds = List(1, 2),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 10,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(1L).map(x => offset(x).toDecimalString)

    // unassignment in the same domain renders it inactive
    executeSql(
      backend.event.activeContractCreateEventBatch(
        eventSequentialIds = List(1, 2),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 11,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe Nil
  }

  it should "return the correct active contracts from assign events, and only if not archived/unassigned" in {
    val dbDtos = Vector(
      dtoUnassign(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        sourceSynchronizerId = "x::domain1",
      ),
      dtoAssign(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#1"),
        commandId = "command id 1",
        targetSynchronizerId = "x::domain1",
      ),
      dtoAssign(
        offset = offset(3),
        eventSequentialId = 3L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
        targetSynchronizerId = "x::domain1",
      ),
      dtoExercise(
        offset = offset(4),
        eventSequentialId = 4L,
        consuming = true,
        contractId = hashCid("#2"),
        synchronizerId = "x::domain2",
      ),
      dtoUnassign(
        offset = offset(5),
        eventSequentialId = 5L,
        contractId = hashCid("#2"),
        sourceSynchronizerId = "x::domain2",
      ),
      dtoExercise(
        offset = offset(10),
        eventSequentialId = 10L,
        consuming = true,
        contractId = hashCid("#2"),
        synchronizerId = "x::domain1",
      ),
      dtoUnassign(
        offset = offset(11),
        eventSequentialId = 11L,
        contractId = hashCid("#1"),
        sourceSynchronizerId = "x::domain1",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(11), 11L))

    executeSql(
      backend.event.activeContractAssignEventBatch(
        eventSequentialIds = List(2, 3),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 6,
      )
    ).map(activeContract =>
      activeContract.copy(rawCreatedEvent =
        rawCreatedEventHasExpectedCreateArgumentAndDriverMetadata(
          activeContract.rawCreatedEvent,
          someSerializedDamlLfValue,
          someDriverMetadataBytes,
        )
      )
    ) shouldBe Vector(
      EventStorageBackend.RawActiveContract(
        workflowId = Some("workflow_id"),
        synchronizerId = "x::domain1",
        reassignmentCounter = 1000L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(2).toDecimalString,
          offset = 2,
          nodeIndex = 0,
          contractId = hashCid("#1").coid,
          templateId = someTemplateId,
          packageName = somePackageName,
          packageVersion = Some(somePackageVersion),
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          createArgument = someSerializedDamlLfValue,
          createArgumentCompression = Some(123),
          createKeyValue = None,
          createKeyMaintainers = Set.empty,
          createKeyValueCompression = Some(456),
          ledgerEffectiveTime = someTime,
          createKeyHash = None,
          driverMetadata = someDriverMetadataBytes,
        ),
        eventSequentialId = 2L,
      ),
      EventStorageBackend.RawActiveContract(
        workflowId = Some("workflow_id"),
        synchronizerId = "x::domain1",
        reassignmentCounter = 1000L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(3).toDecimalString,
          offset = 3,
          nodeIndex = 0,
          contractId = hashCid("#2").coid,
          templateId = someTemplateId,
          packageName = somePackageName,
          packageVersion = Some(somePackageVersion),
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          createArgument = someSerializedDamlLfValue,
          createArgumentCompression = Some(123),
          createKeyValue = None,
          createKeyMaintainers = Set.empty,
          createKeyValueCompression = Some(456),
          ledgerEffectiveTime = someTime,
          createKeyHash = None,
          driverMetadata = someDriverMetadataBytes,
        ),
        eventSequentialId = 3L,
      ),
    )

    // same query as first to double check equality predicate
    executeSql(
      backend.event.activeContractAssignEventBatch(
        eventSequentialIds = List(2, 3),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 6,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(2L, 3L).map(x => offset(x).toDecimalString)

    // archive in the same domain renders it inactive
    executeSql(
      backend.event.activeContractAssignEventBatch(
        eventSequentialIds = List(2, 3),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 10,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(2L).map(x => offset(x).toDecimalString)

    // unassignment in the same domain renders it inactive
    executeSql(
      backend.event.activeContractAssignEventBatch(
        eventSequentialIds = List(2, 3),
        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
        endInclusive = 11,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe Nil
  }

  behavior of "incomplete lookup related event_sequential_id lookup queries"

  it should "return the correct sequence of event sequential IDs" in {
    def toDbValues(tuple: (Int, Int, Long)): UnassignProperties =
      tuple match {
        case (i: Int, synchronizerId: Int, seqId: Long) =>
          UnassignProperties(
            contractId = hashCid(s"#$i").coid,
            synchronizerId = s"x::domain$synchronizerId",
            sequentialId = seqId,
          )
      }

    val dbDtos = Vector(
      dtoCreate(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        commandId = "command id 1",
        synchronizerId = "x::domain1",
        driverMetadata = someDriverMetadataBytes,
      ),
      dtoCreate(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
        synchronizerId = "x::domain1",
        driverMetadata = someDriverMetadataBytes,
      ),
      dtoExercise(
        offset = offset(3),
        eventSequentialId = 3L,
        consuming = true,
        contractId = hashCid("#2"),
        synchronizerId = "x::domain2",
      ),
      dtoUnassign(
        offset = offset(4),
        eventSequentialId = 4L,
        contractId = hashCid("#2"),
        sourceSynchronizerId = "x::domain2",
        targetSynchronizerId = "x::domain1",
      ),
      dtoAssign(
        offset = offset(5),
        eventSequentialId = 5L,
        contractId = hashCid("#2"),
        sourceSynchronizerId = "x::domain2",
        targetSynchronizerId = "x::domain1",
      ),
      dtoAssign(
        offset = offset(6),
        eventSequentialId = 6L,
        contractId = hashCid("#2"),
        sourceSynchronizerId = "x::domain3",
        targetSynchronizerId = "x::domain4",
      ),
      dtoExercise(
        offset = offset(10),
        eventSequentialId = 10L,
        consuming = true,
        contractId = hashCid("#2"),
        synchronizerId = "x::domain1",
      ),
      dtoUnassign(
        offset = offset(11),
        eventSequentialId = 11L,
        contractId = hashCid("#1"),
        sourceSynchronizerId = "x::domain1",
      ),
      dtoUnassign(
        offset = offset(12),
        eventSequentialId = 12L,
        contractId = hashCid("#1"),
        targetSynchronizerId = "x::domain2",
      ),
      dtoAssign(
        offset = offset(13),
        eventSequentialId = 13L,
        contractId = hashCid("#2"),
        targetSynchronizerId = "x::domain2",
      ),
      dtoUnassign(
        offset = offset(14),
        eventSequentialId = 14L,
        contractId = hashCid("#2"),
        sourceSynchronizerId = "x::domain2",
      ),
      dtoAssign(
        offset = offset(15),
        eventSequentialId = 15L,
        contractId = hashCid("#2"),
        targetSynchronizerId = "x::domain2",
      ),
      dtoCreate(
        offset = offset(16),
        eventSequentialId = 16L,
        contractId = hashCid("#3"),
        synchronizerId = "x::domain4",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(16), 16L))

    executeSql(
      backend.event.lookupAssignSequentialIdByOffset(
        List(
          1L,
          5L,
          6L,
          7L,
        )
      )
    ) shouldBe Vector(5L, 6L)
    executeSql(
      backend.event.lookupUnassignSequentialIdByOffset(
        List(
          1L, 4L, 6L, 7L, 11L,
        )
      )
    ) shouldBe Vector(4L, 11L)
    executeSql(
      backend.event.lookupAssignSequentialIdBy(
        List(
          // (contractId, synchronizerId, sequentialId)
          (1, 2, 16L), // not found
          (2, 1, 15L), // found at 5
          (3, 2, 16L), // not found
        ).map(toDbValues)
      )
    ) shouldBe Map((2, 1, 15L) -> 5L)
      .map { case (tuple, id) => (toDbValues(tuple), id) }

    // check that the last assign event is preferred over the earlier
    executeSql(
      backend.event.lookupAssignSequentialIdBy(
        List(
          // (contractId, synchronizerId, sequentialId)
          (1, 2, 16L), // not found
          (2, 1, 15L), // found at 5
          (2, 2, 16L), // last found at 15
          (3, 2, 16L), // not found
        ).map(toDbValues)
      )
    ) shouldBe
      Map(
        (2, 1, 15L) -> 5L,
        (2, 2, 16L) -> 15L,
      ).map { case (tuple, id) => (toDbValues(tuple), id) }
    // check that sequential id is taken into account
    executeSql(
      backend.event.lookupAssignSequentialIdBy(
        List(
          // (contractId, synchronizerId, sequentialId)
          (2, 2, 15L), // last <15 found at 13
          (2, 2, 16L), // last found at 15
        ).map(toDbValues)
      )
    ) shouldBe
      Map(
        (2, 2, 15L) -> 13L,
        (2, 2, 16L) -> 15L,
      ).map { case (tuple, id) => (toDbValues(tuple), id) }

    executeSql(
      // test that we will not find the create event if we use the correct contract id but a wrong synchronizer id
      backend.event.lookupCreateSequentialIdByContractId(
        List(
          1, // found at 1
          2, // found at 2
        ).map(coid => hashCid(s"#$coid").coid)
      )
    ) shouldBe Vector(1L, 2L)
  }

  def rawCreatedEventHasExpectedCreateArgumentAndDriverMetadata(
      rawCreatedEvent: RawCreatedEvent,
      createArgument: Array[Byte],
      driverMetadata: Array[Byte],
  ): RawCreatedEvent = {
    rawCreatedEvent.createArgument.toList shouldBe createArgument.toList
    rawCreatedEvent.driverMetadata.toList shouldBe driverMetadata.toList
    rawCreatedEvent.copy(
      createArgument = createArgument,
      driverMetadata = driverMetadata,
    )
  }

  def hasSameTraceContext(
      actual: Option[Array[Byte]],
      expected: Option[Array[Byte]],
  ): Option[Array[Byte]] = {
    actual should equal(expected)
    expected
  }

}
