// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.{Ref, Time}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawCreatedEvent
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
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
        stakeholder = someParty,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1, 2)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholder = someParty2,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 1,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = Some(someTemplateId2),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(2)

    executeSql(
      backend.event.fetchAssignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 1,
        limit = 10,
      )
    ) shouldBe Vector(1)
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
        stakeholder = someParty,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1, 2)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholder = someParty2,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = None,
        startExclusive = 0,
        endInclusive = 2,
        limit = 1,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(1)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = Some(someTemplateId2),
        startExclusive = 0,
        endInclusive = 2,
        limit = 10,
      )
    ) shouldBe Vector(2)

    executeSql(
      backend.event.fetchUnassignEventIdsForStakeholder(
        stakeholder = someParty,
        templateId = Some(someTemplateId),
        startExclusive = 0,
        endInclusive = 1,
        limit = 10,
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
        allFilterParties = Set(Ref.Party.assertFromString("signatory"), someParty),
      )
    )

    result
      .map(original =>
        original.copy(
          rawCreatedEvent = rawCreatedEventHasExpectedCreateAgumentAndDriverMetadata(
            original.rawCreatedEvent,
            someSerializedDamlLfValue,
            someDriverMetadataBytes,
          ),
          traceContext = hasSameTraceContext(original.traceContext, Some(emptyTraceContext)),
        )
      ) shouldBe (
      Vector(
        EventStorageBackend.RawAssignEvent(
          commandId = Some("command id 1"),
          workflowId = Some("workflow_id"),
          offset = offset(1).toHexString,
          sourceDomainId = "x::sourcedomain",
          targetDomainId = "x::targetdomain",
          unassignId = "123456789",
          submitter = Option(someParty),
          reassignmentCounter = 1000L,
          rawCreatedEvent = RawCreatedEvent(
            updateId = offset(1).toHexString,
            contractId = hashCid("#1").coid,
            templateId = someTemplateId,
            packageName = None,
            witnessParties = Set("signatory"),
            signatories = Set("signatory"),
            observers = Set("observer"),
            agreementText = Some("agreement"),
            createArgument = someSerializedDamlLfValue,
            createArgumentCompression = Some(123),
            createKeyValue = None,
            createKeyMaintainers = Set.empty,
            createKeyValueCompression = Some(456),
            ledgerEffectiveTime = someTime,
            createKeyHash = None,
            driverMetadata = someDriverMetadataBytes,
          ),
          traceContext = Some(emptyTraceContext),
        ),
        EventStorageBackend.RawAssignEvent(
          commandId = Some("command id 2"),
          workflowId = Some("workflow_id"),
          offset = offset(2).toHexString,
          sourceDomainId = "x::sourcedomain",
          targetDomainId = "x::targetdomain",
          unassignId = "123456789",
          submitter = Option(someParty),
          reassignmentCounter = 1000L,
          rawCreatedEvent = RawCreatedEvent(
            updateId = offset(2).toHexString,
            contractId = hashCid("#2").coid,
            templateId = someTemplateId,
            packageName = None,
            witnessParties = Set("signatory"),
            signatories = Set("signatory"),
            observers = Set("observer"),
            agreementText = Some("agreement"),
            createArgument = someSerializedDamlLfValue,
            createArgumentCompression = Some(123),
            createKeyValue = None,
            createKeyMaintainers = Set.empty,
            createKeyValueCompression = Some(456),
            ledgerEffectiveTime = someTime,
            createKeyHash = None,
            driverMetadata = someDriverMetadataBytes,
          ),
          traceContext = Some(emptyTraceContext),
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
        allFilterParties = Set(Ref.Party.assertFromString("signatory"), someParty),
      )
    ).map(original =>
      original.copy(traceContext =
        hasSameTraceContext(original.traceContext, Some(emptyTraceContext))
      )
    ) shouldBe Vector(
      EventStorageBackend.RawUnassignEvent(
        commandId = Some("command id 1"),
        workflowId = Some("workflow_id"),
        offset = offset(1).toHexString,
        sourceDomainId = "x::sourcedomain",
        targetDomainId = "x::targetdomain",
        unassignId = "123456789",
        submitter = Option(someParty),
        reassignmentCounter = 1000L,
        contractId = hashCid("#1").coid,
        templateId = someTemplateId,
        updateId = offset(1).toHexString,
        witnessParties = Set("signatory"),
        assignmentExclusivity = Some(Time.Timestamp.assertFromLong(11111)),
        traceContext = Some(emptyTraceContext),
      ),
      EventStorageBackend.RawUnassignEvent(
        commandId = Some("command id 2"),
        workflowId = Some("workflow_id"),
        offset = offset(2).toHexString,
        sourceDomainId = "x::sourcedomain",
        targetDomainId = "x::targetdomain",
        unassignId = "123456789",
        submitter = Option(someParty),
        reassignmentCounter = 1000L,
        contractId = hashCid("#2").coid,
        templateId = someTemplateId,
        updateId = offset(2).toHexString,
        witnessParties = Set("signatory"),
        assignmentExclusivity = Some(Time.Timestamp.assertFromLong(11111)),
        traceContext = Some(emptyTraceContext),
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
          allFilterParties = Set(Ref.Party.assertFromString("signatory"), someParty),
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
          allFilterParties = Set(Ref.Party.assertFromString("signatory"), someParty),
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
        domainId = Some("x::domain1"),
        driverMetadata = Some(someDriverMetadataBytes),
      ),
      dtoCreate(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
        domainId = Some("x::domain1"),
        driverMetadata = Some(someDriverMetadataBytes),
      ),
      dtoExercise(
        offset = offset(3),
        eventSequentialId = 3L,
        consuming = true,
        contractId = hashCid("#2"),
        domainId = Some("x::domain2"),
      ),
      dtoUnassign(
        offset = offset(4),
        eventSequentialId = 4L,
        contractId = hashCid("#2"),
        sourceDomainId = "x::domain2",
      ),
      dtoExercise(
        offset = offset(10),
        eventSequentialId = 10L,
        consuming = true,
        contractId = hashCid("#2"),
        domainId = Some("x::domain1"),
      ),
      dtoUnassign(
        offset = offset(11),
        eventSequentialId = 11L,
        contractId = hashCid("#1"),
        sourceDomainId = "x::domain1",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(11), 11L))

    executeSql(
      backend.event.activeContractCreateEventBatchV2(
        eventSequentialIds = List(1, 2),
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
        endInclusive = 6,
      )
    ).map(activeContract =>
      activeContract.copy(rawCreatedEvent =
        rawCreatedEventHasExpectedCreateAgumentAndDriverMetadata(
          activeContract.rawCreatedEvent,
          someSerializedDamlLfValue,
          someDriverMetadataBytes,
        )
      )
    ) shouldBe Vector(
      EventStorageBackend.RawActiveContract(
        workflowId = Some("workflow_id"),
        domainId = "x::domain1",
        reassignmentCounter = 0L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(1).toHexString,
          contractId = hashCid("#1").coid,
          templateId = someTemplateId,
          packageName = None,
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          agreementText = None,
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
        domainId = "x::domain1",
        reassignmentCounter = 0L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(2).toHexString,
          contractId = hashCid("#2").coid,
          templateId = someTemplateId,
          packageName = None,
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          agreementText = None,
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
      backend.event.activeContractCreateEventBatchV2(
        eventSequentialIds = List(1, 2),
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
        endInclusive = 6,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(1L, 2L).map(x => offset(x).toHexString)

    // archive in the same domain renders it inactive
    executeSql(
      backend.event.activeContractCreateEventBatchV2(
        eventSequentialIds = List(1, 2),
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
        endInclusive = 10,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(1L).map(x => offset(x).toHexString)

    // unassignment in the same domain renders it inactive
    executeSql(
      backend.event.activeContractCreateEventBatchV2(
        eventSequentialIds = List(1, 2),
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
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
        sourceDomainId = "x::domain1",
      ),
      dtoAssign(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#1"),
        commandId = "command id 1",
        targetDomainId = "x::domain1",
      ),
      dtoAssign(
        offset = offset(3),
        eventSequentialId = 3L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
        targetDomainId = "x::domain1",
      ),
      dtoExercise(
        offset = offset(4),
        eventSequentialId = 4L,
        consuming = true,
        contractId = hashCid("#2"),
        domainId = Some("x::domain2"),
      ),
      dtoUnassign(
        offset = offset(5),
        eventSequentialId = 5L,
        contractId = hashCid("#2"),
        sourceDomainId = "x::domain2",
      ),
      dtoExercise(
        offset = offset(10),
        eventSequentialId = 10L,
        consuming = true,
        contractId = hashCid("#2"),
        domainId = Some("x::domain1"),
      ),
      dtoUnassign(
        offset = offset(11),
        eventSequentialId = 11L,
        contractId = hashCid("#1"),
        sourceDomainId = "x::domain1",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(11), 11L))

    executeSql(
      backend.event.activeContractAssignEventBatch(
        eventSequentialIds = List(2, 3),
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
        endInclusive = 6,
      )
    ).map(activeContract =>
      activeContract.copy(rawCreatedEvent =
        rawCreatedEventHasExpectedCreateAgumentAndDriverMetadata(
          activeContract.rawCreatedEvent,
          someSerializedDamlLfValue,
          someDriverMetadataBytes,
        )
      )
    ) shouldBe Vector(
      EventStorageBackend.RawActiveContract(
        workflowId = Some("workflow_id"),
        domainId = "x::domain1",
        reassignmentCounter = 1000L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(2).toHexString,
          contractId = hashCid("#1").coid,
          templateId = someTemplateId,
          packageName = None,
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          agreementText = Some("agreement"),
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
        domainId = "x::domain1",
        reassignmentCounter = 1000L,
        rawCreatedEvent = RawCreatedEvent(
          updateId = offset(3).toHexString,
          contractId = hashCid("#2").coid,
          templateId = someTemplateId,
          packageName = None,
          witnessParties = Set("observer"),
          signatories = Set("signatory"),
          observers = Set("observer"),
          agreementText = Some("agreement"),
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
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
        endInclusive = 6,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(2L, 3L).map(x => offset(x).toHexString)

    // archive in the same domain renders it inactive
    executeSql(
      backend.event.activeContractAssignEventBatch(
        eventSequentialIds = List(2, 3),
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
        endInclusive = 10,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe List(2L).map(x => offset(x).toHexString)

    // unassignment in the same domain renders it inactive
    executeSql(
      backend.event.activeContractAssignEventBatch(
        eventSequentialIds = List(2, 3),
        allFilterParties = Set(Ref.Party.assertFromString("observer")),
        endInclusive = 11,
      )
    ).map(_.rawCreatedEvent.updateId) shouldBe Nil
  }

  behavior of "incomplete lookup related event_sequential_id lookup queries"

  it should "return the correct sequence of event sequential IDs" in {
    val dbDtos = Vector(
      dtoCreate(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        commandId = "command id 1",
        domainId = Some("x::domain1"),
        driverMetadata = Some(someDriverMetadataBytes),
      ),
      dtoCreate(
        offset = offset(2),
        eventSequentialId = 2L,
        contractId = hashCid("#2"),
        commandId = "command id 2",
        domainId = Some("x::domain1"),
        driverMetadata = Some(someDriverMetadataBytes),
      ),
      dtoExercise(
        offset = offset(3),
        eventSequentialId = 3L,
        consuming = true,
        contractId = hashCid("#2"),
        domainId = Some("x::domain2"),
      ),
      dtoUnassign(
        offset = offset(4),
        eventSequentialId = 4L,
        contractId = hashCid("#2"),
        sourceDomainId = "x::domain2",
      ),
      dtoAssign(
        offset = offset(5),
        eventSequentialId = 5L,
        contractId = hashCid("#2"),
        sourceDomainId = "x::domain2",
      ),
      dtoAssign(
        offset = offset(6),
        eventSequentialId = 6L,
        contractId = hashCid("#2"),
        sourceDomainId = "x::domain3",
      ),
      dtoExercise(
        offset = offset(10),
        eventSequentialId = 10L,
        consuming = true,
        contractId = hashCid("#2"),
        domainId = Some("x::domain1"),
      ),
      dtoUnassign(
        offset = offset(11),
        eventSequentialId = 11L,
        contractId = hashCid("#1"),
        sourceDomainId = "x::domain1",
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(dbDtos, _))
    executeSql(updateLedgerEnd(offset(11), 11L))

    executeSql(
      backend.event.lookupAssignSequentialIdByOffset(
        List(
          1L,
          5L,
          6L,
          7L,
        ).map(offset).map(_.toHexString)
      )
    ) shouldBe Vector(5L, 6L)
    executeSql(
      backend.event.lookupUnassignSequentialIdByOffset(
        List(
          1L, 4L, 6L, 7L, 11L,
        ).map(offset).map(_.toHexString)
      )
    ) shouldBe Vector(4L, 11L)
    executeSql(
      backend.event.lookupAssignSequentialIdByContractId(
        List(
          1,
          2,
          3,
        ).map(i => hashCid(s"#$i").coid)
      )
    ) shouldBe Vector(5L)
    executeSql(
      backend.event.lookupCreateSequentialIdByContractId(
        List(
          1,
          2,
          3,
        ).map(i => hashCid(s"#$i").coid)
      )
    ) shouldBe Vector(1L, 2L)
  }

  def rawCreatedEventHasExpectedCreateAgumentAndDriverMetadata(
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
