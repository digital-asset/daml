// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

private[backend] trait StorageBackendTestsReassignmentEvents
    extends Matchers
    with OptionValues
    with StorageBackendSpec {
  this: AnyFlatSpec =>

// TODO(i28539) analyse if additional unit tests needed, and implement them with the new schema
//  import StorageBackendTestValues.*
//  import ScalatestEqualityHelpers.eqOptArray
//
//  implicit val dbDtoEq: Equality[DbDto] = ScalatestEqualityHelpers.DbDtoEq
//
//  private val emptyTraceContext =
//    SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray
//
//  behavior of "event id fetching from filter tables"
//
//  it should "return the correct event ids for assign event stakeholder" in {
//    val dbDtos = Vector(
//      DbDto.IdFilterAssignStakeholder(
//        1,
//        someTemplateId.toString,
//        someParty,
//        first_per_sequential_id = true,
//      ),
//      DbDto.IdFilterAssignStakeholder(
//        1,
//        someTemplateId.toString,
//        someParty2,
//        first_per_sequential_id = false,
//      ),
//      DbDto.IdFilterAssignStakeholder(
//        2,
//        someTemplateId2.toString,
//        someParty,
//        first_per_sequential_id = true,
//      ),
//    )
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(2), 2L))
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1, 2)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty2),
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = None,
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1, 2)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 1,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = Some(someTemplateId),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = Some(someTemplateId2),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(2)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = Some(someTemplateId),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 1,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = None,
//        templateId = Some(someTemplateId),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 1,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchAssignEventIdsForStakeholderLegacy(
//        stakeholderO = None,
//        templateId = Some(someTemplateId2),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(2)
//
//  }
//
//  it should "return the correct event ids for unassign event stakeholder" in {
//    val dbDtos = Vector(
//      DbDto.IdFilterUnassignStakeholder(
//        1,
//        someTemplateId.toString,
//        someParty,
//        first_per_sequential_id = true,
//      ),
//      DbDto.IdFilterUnassignStakeholder(
//        1,
//        someTemplateId.toString,
//        someParty2,
//        first_per_sequential_id = false,
//      ),
//      DbDto.IdFilterUnassignStakeholder(
//        2,
//        someTemplateId2.toString,
//        someParty,
//        first_per_sequential_id = true,
//      ),
//    )
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(2), 2L))
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1, 2)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty2),
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = None,
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1, 2)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 1,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = Some(someTemplateId),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = None,
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1, 2)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = Some(someTemplateId2),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(2)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = None,
//        templateId = Some(someTemplateId2),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 2,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(2)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = Some(someParty),
//        templateId = Some(someTemplateId),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 1,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = None,
//        templateId = Some(someTemplateId),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 1,
//          limit = 10,
//        )
//      )
//    ) shouldBe Vector(1)
//
//    executeSql(
//      backend.event.fetchUnassignEventIdsForStakeholderLegacy(
//        stakeholderO = None,
//        templateId = Some(someTemplateId),
//      )(_)(
//        PaginationInput(
//          startExclusive = 0,
//          endInclusive = 1,
//          limit = 1,
//        )
//      )
//    ) shouldBe Vector(1)
//  }
//
//  behavior of "event batch fetching"
//
//  it should "return the correct assign events" in {
//    val dbDtos = Vector(
//      dtoAssignLegacy(
//        offset = offset(1),
//        eventSequentialId = 1L,
//        contractId = hashCid("#1"),
//        commandId = "command id 1",
//        nodeId = 24,
//        internalContractId = 42L,
//      ),
//      dtoAssignLegacy(
//        offset = offset(2),
//        eventSequentialId = 2L,
//        contractId = hashCid("#2"),
//        commandId = "command id 2",
//        nodeId = 42,
//        internalContractId = 43L,
//      ),
//    )
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(2), 2L))
//
//    val result = executeSql(
//      backend.event.assignEventBatchLegacy(
//        eventSequentialIds = Ids(List(1L, 2L)),
//        allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
//      )
//    ).map(sanitize)
//
//    result shouldBe (
//      Vector(
//        Entry(
//          commandId = Some("command id 1"),
//          workflowId = Some("workflow_id"),
//          offset = 1,
//          nodeId = 24,
//          traceContext = Some(emptyTraceContext),
//          recordTime = someTime,
//          updateId = TestUpdateId(offset(1).toDecimalString).toHexString,
//          eventSequentialId = 0L,
//          ledgerEffectiveTime = None,
//          synchronizerId = "x::targetsynchronizer",
//          event = EventStorageBackend.RawAssignEventLegacy(
//            sourceSynchronizerId = "x::sourcesynchronizer",
//            targetSynchronizerId = "x::targetsynchronizer",
//            reassignmentId = "0012345678",
//            submitter = Option(someParty),
//            reassignmentCounter = 1000L,
//            rawCreatedEvent = RawCreatedEventLegacy(
//              contractId = hashCid("#1"),
//              templateId = someTemplateIdFull,
//              witnessParties = Set("signatory"),
//              flatEventWitnesses = Set("signatory"),
//              signatories = Set("signatory"),
//              observers = Set("observer"),
//              createArgument = someSerializedDamlLfValue,
//              createArgumentCompression = Some(123),
//              createKeyValue = None,
//              createKeyMaintainers = Set.empty,
//              createKeyValueCompression = Some(456),
//              ledgerEffectiveTime = someTime,
//              createKeyHash = None,
//              authenticationData = someAuthenticationDataBytes,
//              representativePackageId = someTemplateIdFull.pkgId,
//              internalContractId = 42L,
//            ),
//          ),
//          externalTransactionHash = None,
//        ),
//        Entry(
//          commandId = Some("command id 2"),
//          workflowId = Some("workflow_id"),
//          offset = 2,
//          nodeId = 42,
//          traceContext = Some(emptyTraceContext),
//          recordTime = someTime,
//          updateId = TestUpdateId(offset(2).toDecimalString).toHexString,
//          eventSequentialId = 0L,
//          ledgerEffectiveTime = None,
//          synchronizerId = "x::targetsynchronizer",
//          event = EventStorageBackend.RawAssignEventLegacy(
//            sourceSynchronizerId = "x::sourcesynchronizer",
//            targetSynchronizerId = "x::targetsynchronizer",
//            reassignmentId = "0012345678",
//            submitter = Option(someParty),
//            reassignmentCounter = 1000L,
//            rawCreatedEvent = RawCreatedEventLegacy(
//              contractId = hashCid("#2"),
//              templateId = someTemplateIdFull,
//              witnessParties = Set("signatory"),
//              flatEventWitnesses = Set("signatory"),
//              signatories = Set("signatory"),
//              observers = Set("observer"),
//              createArgument = someSerializedDamlLfValue,
//              createArgumentCompression = Some(123),
//              createKeyValue = None,
//              createKeyMaintainers = Set.empty,
//              createKeyValueCompression = Some(456),
//              ledgerEffectiveTime = someTime,
//              createKeyHash = None,
//              authenticationData = someAuthenticationDataBytes,
//              representativePackageId = someTemplateIdFull.pkgId,
//              internalContractId = 43L,
//            ),
//          ),
//          externalTransactionHash = None,
//        ),
//      )
//    )
//
//    val resultRange = executeSql(
//      backend.event.assignEventBatchLegacy(
//        eventSequentialIds = IdRange(1L, 2L),
//        allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
//      )
//    )
//    resultRange.map(sanitize) shouldBe result
//
//  }
//
//  it should "return the correct unassign events" in {
//    val dbDtos = Vector(
//      dtoUnassignLegacy(
//        offset = offset(1),
//        eventSequentialId = 1L,
//        contractId = hashCid("#1"),
//        commandId = "command id 1",
//        nodeId = 24,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(2),
//        eventSequentialId = 2L,
//        contractId = hashCid("#2"),
//        commandId = "command id 2",
//        nodeId = 42,
//      ),
//    )
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(2), 2L))
//
//    val result = executeSql(
//      backend.event.unassignEventBatchLegacy(
//        eventSequentialIds = Ids(List(1L, 2L)),
//        allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
//      )
//    ).map(original =>
//      original.copy(
//        traceContext = hasSameTraceContext(original.traceContext, Some(emptyTraceContext)),
//        eventSequentialId = 0L,
//      )
//    )
//    result shouldBe Vector(
//      Entry(
//        commandId = Some("command id 1"),
//        workflowId = Some("workflow_id"),
//        offset = 1,
//        nodeId = 24,
//        traceContext = Some(emptyTraceContext),
//        recordTime = someTime,
//        updateId = TestUpdateId(offset(1).toDecimalString).toHexString,
//        eventSequentialId = 0L,
//        ledgerEffectiveTime = None,
//        synchronizerId = "x::sourcesynchronizer",
//        event = EventStorageBackend.RawUnassignEventLegacy(
//          sourceSynchronizerId = "x::sourcesynchronizer",
//          targetSynchronizerId = "x::targetsynchronizer",
//          reassignmentId = "0012345678",
//          submitter = Option(someParty),
//          reassignmentCounter = 1000L,
//          contractId = hashCid("#1"),
//          templateId = someTemplateIdFull,
//          witnessParties = Set("signatory"),
//          assignmentExclusivity = Some(Time.Timestamp.assertFromLong(11111)),
//        ),
//        externalTransactionHash = None,
//      ),
//      Entry(
//        commandId = Some("command id 2"),
//        workflowId = Some("workflow_id"),
//        offset = 2,
//        nodeId = 42,
//        traceContext = Some(emptyTraceContext),
//        recordTime = someTime,
//        updateId = TestUpdateId(offset(2).toDecimalString).toHexString,
//        eventSequentialId = 0L,
//        ledgerEffectiveTime = None,
//        synchronizerId = "x::sourcesynchronizer",
//        event = EventStorageBackend.RawUnassignEventLegacy(
//          sourceSynchronizerId = "x::sourcesynchronizer",
//          targetSynchronizerId = "x::targetsynchronizer",
//          reassignmentId = "0012345678",
//          submitter = Option(someParty),
//          reassignmentCounter = 1000L,
//          contractId = hashCid("#2"),
//          templateId = someTemplateIdFull,
//          witnessParties = Set("signatory"),
//          assignmentExclusivity = Some(Time.Timestamp.assertFromLong(11111)),
//        ),
//        externalTransactionHash = None,
//      ),
//    )
//
//    val resultRange = executeSql(
//      backend.event.unassignEventBatchLegacy(
//        eventSequentialIds = IdRange(1L, 2L),
//        allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
//      )
//    ).map(original =>
//      original.copy(
//        traceContext = hasSameTraceContext(original.traceContext, Some(emptyTraceContext)),
//        eventSequentialId = 0L,
//      )
//    )
//    resultRange shouldBe result
//
//  }
//
//  it should "return the correct trace context for assign events" in {
//    TraceContext.withNewTraceContext("test") { aTraceContext =>
//      val serializableTraceContext = SerializableTraceContext(aTraceContext).toDamlProto.toByteArray
//      val dbDtos = Vector(
//        dtoAssignLegacy(
//          offset = offset(1),
//          eventSequentialId = 1L,
//          contractId = hashCid("#1"),
//          commandId = "command id 1",
//          traceContext = emptyTraceContext,
//        ),
//        dtoAssignLegacy(
//          offset = offset(2),
//          eventSequentialId = 2L,
//          contractId = hashCid("#2"),
//          commandId = "command id 2",
//          traceContext = serializableTraceContext,
//        ),
//      )
//
//      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//      executeSql(ingest(dbDtos, _))
//      executeSql(updateLedgerEnd(offset(2), 2L))
//
//      val assignments = executeSql(
//        backend.event.assignEventBatchLegacy(
//          eventSequentialIds = Ids(List(1L, 2L)),
//          allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
//        )
//      )
//      assignments.head.traceContext should equal(None)
//      assignments(1).traceContext should equal(Some(serializableTraceContext))
//    }
//  }
//
//  it should "return the correct trace context for unassign events" in {
//    TraceContext.withNewTraceContext("test") { aTraceContext =>
//      val serializableTraceContext = SerializableTraceContext(aTraceContext).toDamlProto.toByteArray
//      val dbDtos = Vector(
//        dtoUnassignLegacy(
//          offset = offset(1),
//          eventSequentialId = 1L,
//          contractId = hashCid("#1"),
//          commandId = "command id 1",
//          traceContext = emptyTraceContext,
//        ),
//        dtoUnassignLegacy(
//          offset = offset(2),
//          eventSequentialId = 2L,
//          contractId = hashCid("#2"),
//          commandId = "command id 2",
//          traceContext = serializableTraceContext,
//        ),
//      )
//
//      executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//      executeSql(ingest(dbDtos, _))
//      executeSql(updateLedgerEnd(offset(2), 2L))
//
//      val unassignments = executeSql(
//        backend.event.unassignEventBatchLegacy(
//          eventSequentialIds = Ids(List(1L, 2L)),
//          allFilterParties = Some(Set(Ref.Party.assertFromString("signatory"), someParty)),
//        )
//      )
//      unassignments.head.traceContext should equal(None)
//      unassignments(1).traceContext should equal(Some(serializableTraceContext))
//    }
//  }
//
//  behavior of "active contract batch lookup for contracts"
//
//  it should "return the correct active contracts from create events, and only if not archived/unassigned legacy" in {
//    val dbDtos = Vector(
//      dtoCreateLegacy(
//        offset = offset(1),
//        eventSequentialId = 1L,
//        contractId = hashCid("#1"),
//        commandId = "command id 1",
//        synchronizerId = someSynchronizerId,
//        authenticationData = someAuthenticationDataBytes,
//        internalContractId = 42L,
//      ),
//      dtoCreateLegacy(
//        offset = offset(2),
//        eventSequentialId = 2L,
//        contractId = hashCid("#2"),
//        commandId = "command id 2",
//        synchronizerId = someSynchronizerId,
//        authenticationData = someAuthenticationDataBytes,
//        representativePackageId = someRepresentativePackageId,
//        internalContractId = 43L,
//      ),
//      dtoExerciseLegacy(
//        offset = offset(3),
//        eventSequentialId = 3L,
//        consuming = true,
//        contractId = hashCid("#2"),
//        synchronizerId = someSynchronizerId2,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(4),
//        eventSequentialId = 4L,
//        contractId = hashCid("#2"),
//        sourceSynchronizerId = someSynchronizerId2,
//      ),
//      dtoExerciseLegacy(
//        offset = offset(10),
//        eventSequentialId = 10L,
//        consuming = true,
//        contractId = hashCid("#2"),
//        synchronizerId = someSynchronizerId,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(11),
//        eventSequentialId = 11L,
//        contractId = hashCid("#1"),
//        sourceSynchronizerId = someSynchronizerId,
//      ),
//    )
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(11), 11L))
//
//    executeSql(
//      backend.event.activeContractCreateEventBatchLegacy(
//        eventSequentialIds = List(1, 2),
//        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
//        endInclusive = 6,
//      )
//    ).map(activeContract =>
//      activeContract.copy(rawCreatedEvent =
//        rawCreatedEventHasExpectedCreateArgumentAndAuthenticationData(
//          activeContract.rawCreatedEvent,
//          someSerializedDamlLfValue,
//          someAuthenticationDataBytes,
//        )
//      )
//    ) shouldBe Vector(
//      EventStorageBackend.RawActiveContractLegacy(
//        workflowId = Some("workflow_id"),
//        synchronizerId = someSynchronizerId.toProtoPrimitive,
//        reassignmentCounter = 0L,
//        offset = 1,
//        rawCreatedEvent = RawCreatedEventLegacy(
//          contractId = hashCid("#1"),
//          templateId = someTemplateIdFull,
//          witnessParties = Set("observer"),
//          flatEventWitnesses = Set("observer"),
//          signatories = Set("signatory"),
//          observers = Set("observer"),
//          createArgument = someSerializedDamlLfValue,
//          createArgumentCompression = None,
//          createKeyValue = None,
//          createKeyMaintainers = Set.empty,
//          createKeyValueCompression = None,
//          ledgerEffectiveTime = someTime,
//          createKeyHash = None,
//          authenticationData = someAuthenticationDataBytes,
//          representativePackageId = someTemplateIdFull.pkgId,
//          internalContractId = 42L,
//        ),
//        eventSequentialId = 1L,
//        nodeId = 0,
//      ),
//      EventStorageBackend.RawActiveContractLegacy(
//        workflowId = Some("workflow_id"),
//        synchronizerId = someSynchronizerId.toProtoPrimitive,
//        reassignmentCounter = 0L,
//        offset = 2,
//        nodeId = 0,
//        rawCreatedEvent = RawCreatedEventLegacy(
//          contractId = hashCid("#2"),
//          templateId = someTemplateIdFull,
//          witnessParties = Set("observer"),
//          flatEventWitnesses = Set("observer"),
//          signatories = Set("signatory"),
//          observers = Set("observer"),
//          createArgument = someSerializedDamlLfValue,
//          createArgumentCompression = None,
//          createKeyValue = None,
//          createKeyMaintainers = Set.empty,
//          createKeyValueCompression = None,
//          ledgerEffectiveTime = someTime,
//          createKeyHash = None,
//          authenticationData = someAuthenticationDataBytes,
//          representativePackageId = someRepresentativePackageId,
//          internalContractId = 43L,
//        ),
//        eventSequentialId = 2L,
//      ),
//    )
//  }
//
//  it should "return the correct active contracts from create events" in {
//    val dbDtos = Vector(
//      dtosCreate(
//        event_offset = 1,
//        event_sequential_id = 1L,
//        notPersistedContractId = hashCid("#1"),
//        command_id = Some("command id 1"),
//        synchronizer_id = someSynchronizerId,
//      )(),
//      dtosAssign(
//        event_offset = 2,
//        event_sequential_id = 2L,
//        notPersistedContractId = hashCid("#2"),
//        command_id = Some("command id 2"),
//      )(),
//    ).flatten
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(11), 11L))
//
//    executeSql(
//      backend.event.activeContractBatch(
//        eventSequentialIds = List(1, 2),
//        allFilterParties = Some(Set(Ref.Party.assertFromString("stakeholder1"))),
//        endInclusive = 6,
//      )
//    ) should contain theSameElementsInOrderAs Vector(
//      EventStorageBackend.RawThinActiveContract(
//        commonEventProperties = CommonEventProperties(
//          eventSequentialId = 1L,
//          offset = 1,
//          nodeId = 15,
//          workflowId = Some("workflow-id"),
//          synchronizerId = someSynchronizerId.toProtoPrimitive,
//        ),
//        thinCreatedEventProperties = ThinCreatedEventProperties(
//          representativePackageId = Ref.PackageId.assertFromString("representativepackage"),
//          filteredAdditionalWitnessParties = Set.empty,
//          internalContractId = 10L,
//          requestingParties = Some(Set("stakeholder1")),
//          reassignmentCounter = 0L,
//          acsDeltaForParticipant = true,
//        ),
//      ),
//      EventStorageBackend.RawThinActiveContract(
//        commonEventProperties = CommonEventProperties(
//          eventSequentialId = 2L,
//          offset = 2,
//          nodeId = 15,
//          workflowId = Some("workflow-id"),
//          synchronizerId = someSynchronizerId.toProtoPrimitive,
//        ),
//        thinCreatedEventProperties = ThinCreatedEventProperties(
//          representativePackageId = Ref.PackageId.assertFromString("representativepackage"),
//          filteredAdditionalWitnessParties = Set.empty,
//          internalContractId = 10L,
//          requestingParties = Some(Set("stakeholder1")),
//          reassignmentCounter = 345L,
//          acsDeltaForParticipant = true,
//        ),
//      ),
//    )
//  }
//
//  it should "return the correct active contracts from assign events, and only if not archived/unassigned" in {
//    val dbDtos = Vector(
//      dtoUnassignLegacy(
//        offset = offset(1),
//        eventSequentialId = 1L,
//        contractId = hashCid("#1"),
//        sourceSynchronizerId = someSynchronizerId,
//      ),
//      dtoAssignLegacy(
//        offset = offset(2),
//        eventSequentialId = 2L,
//        contractId = hashCid("#1"),
//        commandId = "command id 1",
//        targetSynchronizerId = someSynchronizerId,
//        internalContractId = 42L,
//      ),
//      dtoAssignLegacy(
//        offset = offset(3),
//        eventSequentialId = 3L,
//        contractId = hashCid("#2"),
//        commandId = "command id 2",
//        targetSynchronizerId = someSynchronizerId,
//        internalContractId = 43L,
//      ),
//      dtoExerciseLegacy(
//        offset = offset(4),
//        eventSequentialId = 4L,
//        consuming = true,
//        contractId = hashCid("#2"),
//        synchronizerId = someSynchronizerId2,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(5),
//        eventSequentialId = 5L,
//        contractId = hashCid("#2"),
//        sourceSynchronizerId = someSynchronizerId2,
//      ),
//      dtoExerciseLegacy(
//        offset = offset(10),
//        eventSequentialId = 10L,
//        consuming = true,
//        contractId = hashCid("#2"),
//        synchronizerId = someSynchronizerId,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(11),
//        eventSequentialId = 11L,
//        contractId = hashCid("#1"),
//        sourceSynchronizerId = someSynchronizerId,
//      ),
//    )
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(11), 11L))
//
//    executeSql(
//      backend.event.activeContractAssignEventBatchLegacy(
//        eventSequentialIds = List(2, 3),
//        allFilterParties = Some(Set(Ref.Party.assertFromString("observer"))),
//        endInclusive = 6,
//      )
//    ).map(activeContract =>
//      activeContract.copy(rawCreatedEvent =
//        rawCreatedEventHasExpectedCreateArgumentAndAuthenticationData(
//          activeContract.rawCreatedEvent,
//          someSerializedDamlLfValue,
//          someAuthenticationDataBytes,
//        )
//      )
//    ) shouldBe Vector(
//      EventStorageBackend.RawActiveContractLegacy(
//        workflowId = Some("workflow_id"),
//        synchronizerId = someSynchronizerId.toProtoPrimitive,
//        reassignmentCounter = 1000L,
//        offset = 2,
//        nodeId = 0,
//        rawCreatedEvent = RawCreatedEventLegacy(
//          contractId = hashCid("#1"),
//          templateId = someTemplateIdFull,
//          witnessParties = Set("observer"),
//          flatEventWitnesses = Set("observer"),
//          signatories = Set("signatory"),
//          observers = Set("observer"),
//          createArgument = someSerializedDamlLfValue,
//          createArgumentCompression = Some(123),
//          createKeyValue = None,
//          createKeyMaintainers = Set.empty,
//          createKeyValueCompression = Some(456),
//          ledgerEffectiveTime = someTime,
//          createKeyHash = None,
//          authenticationData = someAuthenticationDataBytes,
//          representativePackageId = someTemplateIdFull.pkgId,
//          internalContractId = 42L,
//        ),
//        eventSequentialId = 2L,
//      ),
//      EventStorageBackend.RawActiveContractLegacy(
//        workflowId = Some("workflow_id"),
//        synchronizerId = someSynchronizerId.toProtoPrimitive,
//        reassignmentCounter = 1000L,
//        offset = 3,
//        nodeId = 0,
//        rawCreatedEvent = RawCreatedEventLegacy(
//          contractId = hashCid("#2"),
//          templateId = someTemplateIdFull,
//          witnessParties = Set("observer"),
//          flatEventWitnesses = Set("observer"),
//          signatories = Set("signatory"),
//          observers = Set("observer"),
//          createArgument = someSerializedDamlLfValue,
//          createArgumentCompression = Some(123),
//          createKeyValue = None,
//          createKeyMaintainers = Set.empty,
//          createKeyValueCompression = Some(456),
//          ledgerEffectiveTime = someTime,
//          createKeyHash = None,
//          authenticationData = someAuthenticationDataBytes,
//          representativePackageId = someTemplateIdFull.pkgId,
//          internalContractId = 43L,
//        ),
//        eventSequentialId = 3L,
//      ),
//    )
//  }
//
//  behavior of "incomplete lookup related event_sequential_id lookup queries"
//
//  it should "return the correct sequence of event sequential IDs" in {
//    def toDbValues(tuple: (Int, Int, Long)): UnassignProperties =
//      tuple match {
//        case (i: Int, synchronizerId: Int, seqId: Long) =>
//          UnassignProperties(
//            contractId = hashCid(s"#$i"),
//            synchronizerId = s"x::synchronizer$synchronizerId",
//            sequentialId = seqId,
//          )
//      }
//
//    val synchronizerId1 = SynchronizerId.tryFromString("x::synchronizer1")
//    val synchronizerId2 = SynchronizerId.tryFromString("x::synchronizer2")
//    val synchronizerId3 = SynchronizerId.tryFromString("x::synchronizer3")
//    val synchronizerId4 = SynchronizerId.tryFromString("x::synchronizer4")
//
//    val dbDtos = Vector(
//      dtoCreateLegacy(
//        offset = offset(1),
//        eventSequentialId = 1L,
//        contractId = hashCid("#1"),
//        commandId = "command id 1",
//        synchronizerId = synchronizerId1,
//        authenticationData = someAuthenticationDataBytes,
//      ),
//      dtoCreateLegacy(
//        offset = offset(2),
//        eventSequentialId = 2L,
//        contractId = hashCid("#2"),
//        commandId = "command id 2",
//        synchronizerId = synchronizerId1,
//        authenticationData = someAuthenticationDataBytes,
//      ),
//      dtoExerciseLegacy(
//        offset = offset(3),
//        eventSequentialId = 3L,
//        consuming = true,
//        contractId = hashCid("#2"),
//        synchronizerId = synchronizerId2,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(4),
//        eventSequentialId = 4L,
//        contractId = hashCid("#2"),
//        sourceSynchronizerId = synchronizerId2,
//        targetSynchronizerId = synchronizerId1,
//      ),
//      dtoAssignLegacy(
//        offset = offset(5),
//        eventSequentialId = 5L,
//        contractId = hashCid("#2"),
//        sourceSynchronizerId = synchronizerId2,
//        targetSynchronizerId = synchronizerId1,
//      ),
//      dtoAssignLegacy(
//        offset = offset(6),
//        eventSequentialId = 6L,
//        contractId = hashCid("#2"),
//        sourceSynchronizerId = synchronizerId3,
//        targetSynchronizerId = synchronizerId4,
//      ),
//      dtoExerciseLegacy(
//        offset = offset(10),
//        eventSequentialId = 10L,
//        consuming = true,
//        contractId = hashCid("#2"),
//        synchronizerId = synchronizerId1,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(11),
//        eventSequentialId = 11L,
//        contractId = hashCid("#1"),
//        sourceSynchronizerId = synchronizerId1,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(12),
//        eventSequentialId = 12L,
//        contractId = hashCid("#1"),
//        targetSynchronizerId = synchronizerId2,
//      ),
//      dtoAssignLegacy(
//        offset = offset(13),
//        eventSequentialId = 13L,
//        contractId = hashCid("#2"),
//        targetSynchronizerId = synchronizerId2,
//      ),
//      dtoUnassignLegacy(
//        offset = offset(14),
//        eventSequentialId = 14L,
//        contractId = hashCid("#2"),
//        sourceSynchronizerId = synchronizerId2,
//      ),
//      dtoAssignLegacy(
//        offset = offset(15),
//        eventSequentialId = 15L,
//        contractId = hashCid("#2"),
//        targetSynchronizerId = synchronizerId2,
//      ),
//      dtoCreateLegacy(
//        offset = offset(16),
//        eventSequentialId = 16L,
//        contractId = hashCid("#3"),
//        synchronizerId = synchronizerId4,
//      ),
//    )
//
//    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
//    executeSql(ingest(dbDtos, _))
//    executeSql(updateLedgerEnd(offset(16), 16L))
//
//    executeSql(
//      backend.event.lookupAssignSequentialIdByOffsetLegacy(
//        List(
//          1L,
//          5L,
//          6L,
//          7L,
//        )
//      )
//    ) shouldBe Vector(5L, 6L)
//    executeSql(
//      backend.event.lookupUnassignSequentialIdByOffsetLegacy(
//        List(
//          1L, 4L, 6L, 7L, 11L,
//        )
//      )
//    ) shouldBe Vector(4L, 11L)
//    executeSql(
//      backend.event.lookupAssignSequentialIdByLegacy(
//        List(
//          // (contractId, synchronizerId, sequentialId)
//          (1, 2, 16L), // not found
//          (2, 1, 15L), // found at 5
//          (3, 2, 16L), // not found
//        ).map(toDbValues)
//      )
//    ) shouldBe Map((2, 1, 15L) -> 5L)
//      .map { case (tuple, id) => (toDbValues(tuple), id) }
//
//    // check that the last assign event is preferred over the earlier
//    executeSql(
//      backend.event.lookupAssignSequentialIdByLegacy(
//        List(
//          // (contractId, synchronizerId, sequentialId)
//          (1, 2, 16L), // not found
//          (2, 1, 15L), // found at 5
//          (2, 2, 16L), // last found at 15
//          (3, 2, 16L), // not found
//        ).map(toDbValues)
//      )
//    ) shouldBe
//      Map(
//        (2, 1, 15L) -> 5L,
//        (2, 2, 16L) -> 15L,
//      ).map { case (tuple, id) => (toDbValues(tuple), id) }
//    // check that sequential id is taken into account
//    executeSql(
//      backend.event.lookupAssignSequentialIdByLegacy(
//        List(
//          // (contractId, synchronizerId, sequentialId)
//          (2, 2, 15L), // last <15 found at 13
//          (2, 2, 16L), // last found at 15
//        ).map(toDbValues)
//      )
//    ) shouldBe
//      Map(
//        (2, 2, 15L) -> 13L,
//        (2, 2, 16L) -> 15L,
//      ).map { case (tuple, id) => (toDbValues(tuple), id) }
//
//    executeSql(
//      // test that we will not find the create event if we use the correct contract id but a wrong synchronizer id
//      backend.event.lookupCreateSequentialIdByContractIdLegacy(
//        List(
//          1, // found at 1
//          2, // found at 2
//        ).map(coid => hashCid(s"#$coid"))
//      )
//    ) shouldBe Vector(1L, 2L)
//  }
//
//  def rawCreatedEventHasExpectedCreateArgumentAndAuthenticationData(
//      rawCreatedEvent: RawCreatedEventLegacy,
//      createArgument: Array[Byte],
//      authenticationData: Array[Byte],
//  ): RawCreatedEventLegacy = {
//    rawCreatedEvent.createArgument.toList shouldBe createArgument.toList
//    rawCreatedEvent.authenticationData.toList shouldBe authenticationData.toList
//    rawCreatedEvent.copy(
//      createArgument = createArgument,
//      authenticationData = authenticationData,
//    )
//  }
//
//  private def sanitize(
//      original: Entry[EventStorageBackend.RawAssignEventLegacy]
//  ): Entry[EventStorageBackend.RawAssignEventLegacy] =
//    original.copy(
//      event = original.event.copy(
//        rawCreatedEvent = rawCreatedEventHasExpectedCreateArgumentAndAuthenticationData(
//          original.event.rawCreatedEvent,
//          someSerializedDamlLfValue,
//          someAuthenticationDataBytes,
//        )
//      ),
//      traceContext = hasSameTraceContext(original.traceContext, Some(emptyTraceContext)),
//      eventSequentialId = 0L,
//    )
//
//  def hasSameTraceContext(
//      actual: Option[Array[Byte]],
//      expected: Option[Array[Byte]],
//  ): Option[Array[Byte]] = {
//    actual shouldEqual expected
//    expected
//  }
//
}
