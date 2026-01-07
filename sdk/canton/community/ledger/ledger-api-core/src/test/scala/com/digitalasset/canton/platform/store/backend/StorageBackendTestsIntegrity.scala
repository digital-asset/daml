// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.SqlParser.scalar
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.{
  Added,
  ChangedTo,
  Revoked,
}
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.platform.store.backend.DbDto.{
  EventActivate,
  EventDeactivate,
  EventVariousWitnessed,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

private[backend] trait StorageBackendTestsIntegrity extends Matchers with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  private val time1 = Timestamp.now()
  private val time2 = time1.addMicros(10)
  private val time3 = time2.addMicros(10)
  private val time4 = time3.addMicros(10)
  private val time5 = time4.addMicros(10)
  private val time6 = time5.addMicros(10)
  private val time7 = time6.addMicros(10)

  behavior of "IntegrityStorageBackend"

  it should "find duplicate event ids" in {
    val updates = Vector(
      dtosCreate(event_offset = 7, event_sequential_id = 7L)(),
      dtosCreate(event_offset = 7, event_sequential_id = 7L)(), // duplicate id
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(7), 7L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))

    // Error message should contain the duplicate event sequential id
    failure.getMessage should include("7")
  }

  it should "find duplicate event ids with different offsets" in {
    val updates = Vector(
      dtosCreate(event_offset = 6, event_sequential_id = 7L)(),
      dtosCreate(event_offset = 7, event_sequential_id = 7L)(), // duplicate id
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(7), 7L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))

    // Error message should contain the duplicate event sequential id
    failure.getMessage should include("7")
  }

  it should "find non-consecutive event ids" in {
    val updates = Vector(
      dtosCreate(event_offset = 1, event_sequential_id = 1L)(),
      dtosCreate(event_offset = 3, event_sequential_id = 3L)(), // non-consecutive id
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(3), 3L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))

    failure.getMessage should include("consecutive")

  }

  it should "not find non-consecutive event ids if those gaps are before the pruning offset" in {
    val internalContractId = insertParContracts() // making sure internal_contract_id = 1L exists
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(),
      dtosCreate(
        event_offset = 3,
        event_sequential_id = 3L,
        internal_contract_id = 1,
      )(), // non-consecutive id but after pruning offset
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 4L,
        internal_contract_id = internalContractId,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(2)))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(4), 4L))
    executeSql(backend.integrity.verifyIntegrity())
  }

  it should "detect monotonicity violation of record times for one synchronizer in activate table" in {
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        synchronizer_id = someSynchronizerId,
        record_time = time5.micros,
      )(),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        synchronizer_id = someSynchronizerId2,
        record_time = time1.micros,
      )(),
      dtosCreate(
        event_offset = 3,
        event_sequential_id = 3L,
        synchronizer_id = someSynchronizerId,
        record_time = time7.micros,
      )(),
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 4L,
        synchronizer_id = someSynchronizerId2,
        record_time = time3.micros,
      )(),
      dtosCreate(
        event_offset = 5,
        event_sequential_id = 5L,
        synchronizer_id = someSynchronizerId,
        record_time = time6.micros,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one synchronizer: offsets Offset(3),Offset(5)"
    )
  }

  it should "detect monotonicity violation of record times for one synchronizer in deactivate table" in {
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        synchronizer_id = someSynchronizerId,
        record_time = time5.micros,
      )(),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        synchronizer_id = someSynchronizerId2,
        record_time = time1.micros,
      )(),
      dtosConsumingExercise(
        event_offset = 3,
        event_sequential_id = 3L,
        synchronizer_id = someSynchronizerId,
        record_time = time7.micros,
      ),
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 4L,
        synchronizer_id = someSynchronizerId2,
        record_time = time3.micros,
      )(),
      dtosCreate(
        event_offset = 5,
        event_sequential_id = 5L,
        synchronizer_id = someSynchronizerId,
        record_time = time6.micros,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one synchronizer: offsets Offset(3),Offset(5)"
    )
  }

  it should "detect monotonicity violation of record times for one synchronizer in various witnessed table" in {
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        synchronizer_id = someSynchronizerId,
        record_time = time5.micros,
      )(),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        synchronizer_id = someSynchronizerId2,
        record_time = time1.micros,
      )(),
      dtosWitnessedExercised(
        event_offset = 3,
        event_sequential_id = 3L,
        consuming = false,
        synchronizer_id = someSynchronizerId,
        record_time = time7.micros,
      ),
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 4L,
        synchronizer_id = someSynchronizerId2,
        record_time = time3.micros,
      )(),
      dtosCreate(
        event_offset = 5,
        event_sequential_id = 5L,
        synchronizer_id = someSynchronizerId,
        record_time = time6.micros,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one synchronizer: offsets Offset(3),Offset(5)"
    )
  }

  it should "detect monotonicity violation of record times for one synchronizer in completions table" in {
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        synchronizer_id = someSynchronizerId,
        record_time = time5.micros,
      )(),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        synchronizer_id = someSynchronizerId2,
        record_time = time1.micros,
      )(),
      Seq(
        dtoCompletion(
          offset(3),
          synchronizerId = someSynchronizerId,
          recordTime = time7,
        )
      ),
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 3L,
        synchronizer_id = someSynchronizerId2,
        record_time = time3.micros,
      )(),
      dtosCreate(
        event_offset = 5,
        event_sequential_id = 4L,
        synchronizer_id = someSynchronizerId,
        record_time = time6.micros,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one synchronizer: offsets Offset(3),Offset(5)"
    )
  }

  it should "detect monotonicity violation of record times for one synchronizer in completions table, if it is a timely-reject going backwards" in {
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        synchronizer_id = someSynchronizerId,
        record_time = time5.micros,
      )(),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        synchronizer_id = someSynchronizerId2,
        record_time = time1.micros,
      )(),
      Seq(
        dtoCompletion(
          offset(3),
          synchronizerId = someSynchronizerId,
          recordTime = time7,
          messageUuid = Some("message uuid"),
        )
      ),
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 3L,
        synchronizer_id = someSynchronizerId2,
        record_time = time3.micros,
      )(),
      dtosCreate(
        event_offset = 5,
        event_sequential_id = 4L,
        synchronizer_id = someSynchronizerId,
        record_time = time6.micros,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one synchronizer: offsets Offset(3),Offset(5)"
    )
  }

  it should "detect monotonicity violation of record times for one synchronizer in party to participant table" in {
    val updates = Vector(
      dtoPartyToParticipant(
        offset(1),
        1L,
        someParty,
        someParticipantId.toString,
        Added(AuthorizationLevel.Submission),
        synchronizerId = someSynchronizerId,
        recordTime = time5,
      ),
      dtoPartyToParticipant(
        offset(2),
        2L,
        someParty,
        someParticipantId.toString,
        ChangedTo(AuthorizationLevel.Confirmation),
        synchronizerId = someSynchronizerId2,
        recordTime = time1,
      ),
      dtoPartyToParticipant(
        offset(3),
        3L,
        someParty,
        someParticipantId.toString,
        ChangedTo(AuthorizationLevel.Observation),
        synchronizerId = someSynchronizerId,
        recordTime = time7,
      ),
      dtoPartyToParticipant(
        offset(4),
        4L,
        someParty,
        someParticipantId.toString,
        Revoked,
        synchronizerId = someSynchronizerId2,
        recordTime = time3,
      ),
      dtoPartyToParticipant(
        offset(5),
        5L,
        someParty,
        someParticipantId.toString,
        Added(AuthorizationLevel.Submission),
        synchronizerId = someSynchronizerId,
        recordTime = time6,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 5L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "occurrence of decreasing record time found within one synchronizer: offsets Offset(3),Offset(5)"
    )
  }

  it should "detect duplicated update ids" in {
    val updates = Vector(
      dtoTransactionMeta(
        offset(1),
        1L,
        4L,
        udpateId = Some(updateIdArrayFromOffset(offset(1))),
      ),
      dtoTransactionMeta(
        offset(2),
        1L,
        4L,
        udpateId = Some(updateIdArrayFromOffset(offset(2))),
      ),
      dtoTransactionMeta(
        offset(3),
        1L,
        4L,
        udpateId = Some(updateIdArrayFromOffset(offset(2))),
      ),
      dtoTransactionMeta(
        offset(4),
        1L,
        4L,
        udpateId = Some(updateIdArrayFromOffset(offset(4))),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    val hashForOffset2 = updateIdFromOffset(offset(2)).toHexString
    failure.getMessage should include(
      s"occurrence of duplicate update ID [$hashForOffset2] found for offsets Offset(2), Offset(3)"
    )
  }

  it should "detect duplicated completion offsets" in {
    val updates = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset(2)
      ),
      dtoCompletion(
        offset(2)
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "occurrence of duplicate offset found for lapi_command_completions: for offset Offset(2) 2 rows found"
    )
  }

  it should "detect same completion entries for different offsets" in {
    val updates = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset(2),
        commandId = "commandid",
        submissionId = Some("submissionid"),
        updateId = Some(updateIdArrayFromOffset(offset(2))),
      ),
      dtoCompletion(
        offset(3),
        commandId = "commandid",
        submissionId = Some("submissionid"),
        updateId = Some(updateIdArrayFromOffset(offset(2))),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "duplicate entries found in lapi_command_completions at offsets (first 10 shown) List(Offset(2), Offset(3))"
    )
  }

  it should "detect completion entries with the same messageUuid for different offsets" in {
    val messageUuid = Some(UUID.randomUUID().toString)
    val updates = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset(2),
        commandId = "commandid1",
        submissionId = Some("submissionid1"),
        updateId = Some(updateIdArrayFromOffset(offset(2))),
        messageUuid = messageUuid,
      ),
      dtoCompletion(
        offset(3),
        commandId = "commandid",
        submissionId = Some("submissionid"),
        updateId = Some(updateIdArrayFromOffset(offset(3))),
        messageUuid = messageUuid,
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    val failure =
      intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "duplicate entries found by messageUuid in lapi_command_completions at offsets (first 10 shown) List(Offset(2), Offset(3))"
    )
  }

  it should "not detect same completion entries for different offsets, if synchronizer id differs" in {
    val updates = Vector(
      dtoCompletion(
        offset(1)
      ),
      dtoCompletion(
        offset(2),
        commandId = "commandid",
        submissionId = Some("submissionid"),
        updateId = Some(updateIdArrayFromOffset(offset(2))),
      ),
      dtoCompletion(
        offset(3),
        commandId = "commandid",
        submissionId = Some("submissionid"),
        updateId = Some(updateIdArrayFromOffset(offset(2))),
        synchronizerId = SynchronizerId.tryFromString("x::othersynchronizerid"),
      ),
    )

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 4L))
    executeSql(backend.integrity.verifyIntegrity())
  }

  it should "not find errors beyond the ledger end" in {
    val internalContractId = insertParContracts()
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        internal_contract_id = internalContractId,
      )(),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        internal_contract_id = internalContractId,
      )(),
      dtosCreate(
        event_offset = 7,
        event_sequential_id = 7L,
        internal_contract_id = internalContractId,
      )(), // beyond the ledger end
      dtosCreate(
        event_offset = 7,
        event_sequential_id = 7L,
        internal_contract_id = internalContractId,
      )(), // duplicate id (beyond ledger end)
      dtosCreate(
        event_offset = 9,
        event_sequential_id = 9L,
        internal_contract_id = internalContractId,
      )(), // non-consecutive id (beyond ledger end)
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(2), 2L))
    executeSql(backend.integrity.verifyIntegrity())

    // Succeeds if verifyIntegrity() doesn't throw
    succeed
  }

  private def prepareMissingReferencedParContracts(): Unit = {
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L,
        notPersistedContractId = hashCid("#1"),
        internal_contract_id = 1L,
      )(
        stakeholders = Set(someParty)
      ),
      dtosConsumingExercise(
        event_offset = 2,
        event_sequential_id = 2L,
        deactivated_event_sequential_id = Some(1L),
        internal_contract_id = Some(2L),
      ),
      dtosWitnessedExercised(
        event_offset = 3,
        event_sequential_id = 3L,
        internal_contract_id = Some(3L),
        consuming = false,
      ),
      // events above are under par_pruning_operation.started_up_to_inclusive, no not checked
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 4L,
        notPersistedContractId = hashCid("#1"),
        internal_contract_id = 4L,
      )(
        stakeholders = Set(someParty)
      ),
      // event above still under par_pruning_operation.started_up_to_inclusive, but has a deactivation event above
      dtosCreate(
        event_offset = 11,
        event_sequential_id = 5L,
        notPersistedContractId = hashCid("#1"),
        internal_contract_id = 5L,
      )(
        stakeholders = Set(someParty)
      ),
      dtosConsumingExercise(
        event_offset = 12,
        event_sequential_id = 6L,
        deactivated_event_sequential_id = Some(1L),
        internal_contract_id = Some(6L),
      ),
      dtosWitnessedExercised(
        event_offset = 13,
        event_sequential_id = 7L,
        internal_contract_id = Some(7L),
        consuming = false,
      ),
      dtosConsumingExercise(
        event_offset = 14,
        event_sequential_id = 8L,
        deactivated_event_sequential_id = Some(4L),
        internal_contract_id = Some(4L),
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(14), 8L))
  }

  it should "find missing referenced par_contracts" in {
    prepareMissingReferencedParContracts()
    insertPruningOperationUpTo(10L)
    val failure = intercept[RuntimeException](executeSql(backend.integrity.verifyIntegrity()))
    failure.getMessage should include(
      "some internal_contract_id-s in events tables are not present in par_contracts (first 10 shown with offsets) [(4,4), (4,14), (5,11), (6,12), (7,13)]"
    )
  }

  it should "not report error for missing referenced par_contracts when inMemory" in {
    prepareMissingReferencedParContracts()
    insertPruningOperationUpTo(0L)
    executeSql(backend.integrity.verifyIntegrity(inMemoryCantonStore = true))
    succeed
  }

  it should "find stray deactivations" in {
    val updates = Vector(
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L,
        notPersistedContractId = hashCid("#2"),
        internal_contract_id = 1L,
      )(
        stakeholders = Set(someParty)
      ),
      dtosConsumingExercise( // correct deactivation of #2
        event_offset = 3,
        event_sequential_id = 3L,
        deactivated_event_sequential_id = Some(2L),
        internal_contract_id = Some(1L),
      ),
      dtosConsumingExercise( // unknown deactivated_event_sequential_id
        event_offset = 4,
        event_sequential_id = 4L,
        deactivated_event_sequential_id = Some(1L),
        internal_contract_id = Some(1L),
      ),
      dtosConsumingExercise( // deactivated_event_sequential_id is greater than event_sequential_id
        event_offset = 5,
        event_sequential_id = 5L,
        deactivated_event_sequential_id = Some(6L),
        internal_contract_id = Some(1L),
      ),
      dtosConsumingExercise( // deactivated_event_sequential_id is NULL - not reported
        event_offset = 6,
        event_sequential_id = 6L,
        deactivated_event_sequential_id = None,
      ),
      dtosCreate(
        event_offset = 7,
        event_sequential_id = 7L,
        notPersistedContractId = hashCid("#7"),
        internal_contract_id = 2L,
      )(
        stakeholders = Set(someParty)
      ),
      dtosConsumingExercise( // a deactivation after ledger end, should be ignored
        event_offset = 100,
        event_sequential_id = 100L,
        deactivated_event_sequential_id = Some(8L),
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(7), 7L))

    // using inMemoryCantonStore = true to skip the par_contracts check
    val failure =
      intercept[RuntimeException](
        executeSql(backend.integrity.verifyIntegrity(inMemoryCantonStore = true))
      )
    failure.getMessage should include(
      "some deactivation events do not have a preceding activation event, deactivated_event_sequential_id-s with offsets (first 10 shown) [(1,4), (6,5)]"
    )
  }

  it should "report leftover witnessed events after pruning" in {
    val updates = Vector(
      dtosWitnessedExercised(
        event_offset = 2,
        event_sequential_id = 2L,
        consuming = false,
        internal_contract_id = None,
      ),
      dtosWitnessedExercised(
        event_offset = 5,
        event_sequential_id = 3L,
        consuming = false,
        internal_contract_id = None,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(5), 3L))

    // using inMemoryCantonStore = true to skip the par_contracts check
    val failure = intercept[RuntimeException](
      executeSql(backend.integrity.verifyIntegrity(inMemoryCantonStore = true))
    )

    failure.getMessage should include(
      "some events in various_witnessed have not been pruned, offsets (first 10 shown) [2]"
    )
  }

  it should "report leftover activate events after pruning" in {
    val updates = Vector(
      dtosCreate(
        event_offset = 1,
        event_sequential_id = 1L, // deactivated by 3L
      )(),
      dtosCreate(
        event_offset = 2,
        event_sequential_id = 2L, // not deactivated
      )(),
      dtosConsumingExercise(
        event_offset = 3,
        event_sequential_id = 3L,
        deactivated_event_sequential_id = Some(1L),
      ),
      // incomplete reassignment 1
      dtosCreate(
        event_offset = 4,
        event_sequential_id = 4L, // deactivated by 5L
      )(),
      dtosUnassign(
        event_offset = 5,
        event_sequential_id = 5L,
        deactivated_event_sequential_id = Some(4L),
      ),
      // incomplete reassignment 2
      dtosAssign(
        event_offset = 6,
        event_sequential_id = 6L, // deactivated by 7L
      )(),
      dtosConsumingExercise(
        event_offset = 7,
        event_sequential_id = 7L,
        deactivated_event_sequential_id = Some(6L),
      ),
      // incomplete reassignment 3
      dtosAssign(
        event_offset = 8,
        event_sequential_id = 8L, // deactivated by 9L
      )(),
      dtosUnassign(
        event_offset = 9,
        event_sequential_id = 9L,
        deactivated_event_sequential_id = Some(8L),
      ),
      // after pruning offset
      dtosCreate(
        event_offset = 10,
        event_sequential_id = 10L,
      )(),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(9)))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(10), 10L))

    // using inMemoryCantonStore = true to skip the par_contracts check
    val failure = intercept[RuntimeException](
      executeSql(backend.integrity.verifyIntegrity(inMemoryCantonStore = true))
    )

    failure.getMessage should include(
      "some events in activate have not been pruned, offsets (first 10 shown) [1]"
    )
  }

  it should "report leftover deactivate events after pruning" in {
    val updates = Vector(
      dtosConsumingExercise(
        event_offset = 2,
        event_sequential_id = 2L,
        deactivated_event_sequential_id = None,
      ),
      dtosConsumingExercise(
        event_offset = 5,
        event_sequential_id = 3L,
        deactivated_event_sequential_id = None,
      ),
    ).flatten

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(backend.parameter.updatePrunedUptoInclusive(offset(3)))
    executeSql(ingest(updates, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    // using inMemoryCantonStore = true to skip the par_contracts check
    val failure = intercept[RuntimeException](
      executeSql(backend.integrity.verifyIntegrity(inMemoryCantonStore = true))
    )

    failure.getMessage should include(
      "some events in deactivate have not been pruned, offsets (first 10 shown) [2]"
    )
  }

  private def insertParContracts(): Long =
    executeSql(
      SQL"INSERT INTO par_contracts (contract_id, instance, package_id, template_id) VALUES (${"c".getBytes}, ${"d".getBytes}, 'pid', 'tid')"
        .executeInsert(scalar[Long].single)(_)
    )

  def insertPruningOperationUpTo[A](offset: Long): Unit =
    executeSql(
      SQL"INSERT INTO par_pruning_operation (name, started_up_to_inclusive) VALUES ('n', $offset)"
        .execute()(_)
    )

  private def performMissingMandatoryFieldCheck(updates: Seq[DbDto]) = {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(updates.toVector, _))
    executeSql(updateLedgerEnd(offset(2), 2L))

    // using inMemoryCantonStore = true to skip the par_contracts check
    val failure =
      intercept[RuntimeException](
        executeSql(backend.integrity.verifyIntegrity(inMemoryCantonStore = true))
      )
    failure.getMessage should include(
      "some events are missing mandatory fields, event_sequential_ids, offsets (first 10 shown) [(3,3)]"
    )
  }

  private def checkMissingAssignField(f: String)(c: EventActivate => EventActivate) =
    it should s"find missing mandatory Assign fields: $f" in {
      performMissingMandatoryFieldCheck(
        dtosAssign(event_offset = 3, event_sequential_id = 3L)(
          stakeholders = Set(someParty)
        ).map {
          case t: EventActivate => c(t)
          case o => o
        }
      )
    }

  private def checkMissingConsumingExerciseField(f: String)(c: EventDeactivate => EventDeactivate) =
    it should s"find missing mandatory Consuming Exercise fields: $f" in {
      performMissingMandatoryFieldCheck(
        dtosCreate(event_sequential_id = 2L)(Set(someParty)) ++
          dtosConsumingExercise(
            event_offset = 3,
            event_sequential_id = 3L,
            deactivated_event_sequential_id = Some(2L),
          ).map {
            case t: EventDeactivate => c(t)
            case o => o
          }
      )
    }

  private def checkUnassign(f: String)(c: EventDeactivate => EventDeactivate) =
    it should s"find missing mandatory Unassign fields: $f" in {
      performMissingMandatoryFieldCheck(
        dtosCreate(event_sequential_id = 2L)(Set(someParty)) ++
          dtosUnassign(
            event_offset = 3,
            event_sequential_id = 3L,
            deactivated_event_sequential_id = Some(2L),
          ).map {
            case t: EventDeactivate => c(t)
            case o => o
          }
      )
    }

  private def checkNonConsumingExercise(
      f: String
  )(c: EventVariousWitnessed => EventVariousWitnessed) =
    it should s"find missing mandatory NonConsuming Exercise fields: $f" in {
      performMissingMandatoryFieldCheck(
        dtosWitnessedExercised(event_offset = 3, event_sequential_id = 3L, consuming = false).map {
          case t: EventVariousWitnessed => c(t)
          case o => o
        }
      )
    }

  private def checkMissingWitnessedCreateField(
      f: String
  )(c: EventVariousWitnessed => EventVariousWitnessed) =
    it should s"find missing mandatory Witnessed Create fields: $f" in {
      performMissingMandatoryFieldCheck(
        dtosWitnessedCreate(event_offset = 3, event_sequential_id = 3L)().map {
          case t: EventVariousWitnessed => c(t)
          case o => o
        }
      )
    }

  private def checkMissingWitnessedConsumingExerciseField(
      f: String
  )(c: EventVariousWitnessed => EventVariousWitnessed) =
    it should s"find missing mandatory Witnessed Consuming Exercise fields: $f" in {
      performMissingMandatoryFieldCheck(
        dtosWitnessedExercised(event_offset = 3, event_sequential_id = 3L, consuming = true).map {
          case t: EventVariousWitnessed => c(t)
          case o => o
        }
      )
    }

  checkMissingAssignField("source_synchronizer_id")(_.copy(source_synchronizer_id = None))
  checkMissingAssignField("reassignment_counter")(_.copy(reassignment_counter = None))
  checkMissingAssignField("reassignment_id")(_.copy(reassignment_id = None))
  checkMissingConsumingExerciseField("additional_witnesses")(_.copy(additional_witnesses = None))
  checkMissingConsumingExerciseField("exercise_choice")(_.copy(exercise_choice = None))
  checkMissingConsumingExerciseField("exercise_argument")(_.copy(exercise_argument = None))
  checkMissingConsumingExerciseField("exercise_result")(_.copy(exercise_result = None))
  checkMissingConsumingExerciseField("exercise_actors")(_.copy(exercise_actors = None))
  checkMissingConsumingExerciseField("ledger_effective_time")(_.copy(ledger_effective_time = None))
  checkUnassign("reassignment_id")(_.copy(reassignment_id = None))
  checkUnassign("target_synchronizer_id")(_.copy(target_synchronizer_id = None))
  checkUnassign("reassignment_counter")(_.copy(reassignment_counter = None))
  checkNonConsumingExercise("consuming")(_.copy(consuming = None))
  checkNonConsumingExercise("exercise_choice")(_.copy(exercise_choice = None))
  checkNonConsumingExercise("exercise_argument")(_.copy(exercise_argument = None))
  checkNonConsumingExercise("exercise_result")(_.copy(exercise_result = None))
  checkNonConsumingExercise("exercise_actors")(_.copy(exercise_actors = None))
  checkNonConsumingExercise("contract_id")(_.copy(contract_id = None))
  checkNonConsumingExercise("template_id")(_.copy(template_id = None))
  checkNonConsumingExercise("package_id")(_.copy(package_id = None))
  checkMissingWitnessedCreateField("representative_package_id")(
    _.copy(representative_package_id = None)
  )
  checkMissingWitnessedCreateField("internal_contract_id")(_.copy(internal_contract_id = None))
  checkMissingWitnessedConsumingExerciseField("consuming")(_.copy(consuming = None))
  checkMissingWitnessedConsumingExerciseField("exercise_choice")(_.copy(exercise_choice = None))
  checkMissingWitnessedConsumingExerciseField("exercise_argument")(_.copy(exercise_argument = None))
  checkMissingWitnessedConsumingExerciseField("exercise_result")(_.copy(exercise_result = None))
  checkMissingWitnessedConsumingExerciseField("exercise_actors")(_.copy(exercise_actors = None))
  checkMissingWitnessedConsumingExerciseField("contract_id")(_.copy(contract_id = None))
  checkMissingWitnessedConsumingExerciseField("template_id")(_.copy(template_id = None))
  checkMissingWitnessedConsumingExerciseField("package_id")(_.copy(package_id = None))

}
