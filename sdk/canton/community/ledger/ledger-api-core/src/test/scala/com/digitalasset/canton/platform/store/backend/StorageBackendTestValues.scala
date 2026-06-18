// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash as CantonHash, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.Added
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.{
  AuthorizationEvent,
  AuthorizationLevel,
}
import com.digitalasset.canton.platform.store.backend.Conversions.{
  authorizationEventInt,
  participantPermissionInt,
}
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao
import com.digitalasset.canton.protocol.{ReassignmentId, TestUpdateId, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, NameTypeConRef, NameTypeConRefConverter}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString
import org.scalatest.OptionValues

import java.time.Instant
import java.util.UUID

/** Except where specified, values should be treated as opaque
  */
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
private[store] object StorageBackendTestValues extends OptionValues {

  def hashCid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  /** Produces offsets that are ordered the same as the input value */
  def offset(x: Long): Offset = Offset.tryFromLong(x)
  def ledgerEnd(o: Long, e: Long): ParameterStorageBackend.LedgerEnd =
    ParameterStorageBackend.LedgerEnd(offset(o), e, 0, CantonTimestamp.now())
  def updateIdFromOffset(x: Offset): UpdateId = TestUpdateId(x.toDecimalString)
  def updateIdArrayFromOffset(x: Offset): Array[Byte] = updateIdFromOffset(
    x
  ).toProtoPrimitive.toByteArray

  def timestampFromInstant(i: Instant): Timestamp = Timestamp.assertFromInstant(i)
  val someTime: Timestamp = timestampFromInstant(Instant.now())

  val someParticipantId: ParticipantId = ParticipantId(
    Ref.ParticipantId.assertFromString("participant")
  )
  val somePackageId: Ref.PackageId = Ref.PackageId.assertFromString("pkg")
  val someTemplateId: NameTypeConRef = NameTypeConRef.assertFromString("#pkg-name:Mod:Template")
  val someInterfaceId: Identifier = Identifier.assertFromString("0abc:Mod:Template")
  val someTemplateIdFull: Ref.FullIdentifier = someTemplateId.toFullIdentifier(somePackageId)
  val someRepresentativePackageId: Ref.PackageId =
    Ref.PackageId.assertFromString("representative-pkg")
  val someTemplateId2: NameTypeConRef = NameTypeConRef.assertFromString("#pkg-name:Mod:Template2")
  val someIdentityParams: ParameterStorageBackend.IdentityParams =
    ParameterStorageBackend.IdentityParams(someParticipantId)
  val someParty: Ref.Party = Ref.Party.assertFromString("party")
  val someParty2: Ref.Party = Ref.Party.assertFromString("party2")
  val someParty3: Ref.Party = Ref.Party.assertFromString("party3")
  val someUserId: Ref.UserId = Ref.UserId.assertFromString("user_id")
  val someSubmissionId: Ref.SubmissionId = Ref.SubmissionId.assertFromString("submission_id")
  val someAuthenticationData: Bytes = Bytes.assertFromString("00abcd")
  val someAuthenticationDataBytes: Array[Byte] = someAuthenticationData.toByteArray

  val someArchive: DamlLf.Archive = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build

  val someSerializedDamlLfValue: Array[Byte] = Array.empty[Byte]
  val someSynchronizerId: SynchronizerId = SynchronizerId.tryFromString("x::sourcesynchronizer")
  val someSynchronizerId2: SynchronizerId = SynchronizerId.tryFromString("x::targetsynchronizer")

  val testTraceContext = TraceContext.withNewTraceContext("test trace context")(identity)
  val serializableTraceContext: Array[Byte] =
    SerializableTraceContext(testTraceContext).toDamlProto.toByteArray
  val someExternalTransactionHash: CantonHash =
    CantonHash
      .digest(HashPurpose.PreparedSubmission, ByteString.copyFromUtf8("mock_hash"), Sha256)
  val someExternalTransactionHashBinary: Array[Byte] =
    someExternalTransactionHash.getCryptographicEvidence.toByteArray
  val reassignmentId: Array[Byte] =
    ReassignmentId.create("0012345678").toOption.get.toBytes.toByteArray

  def dtoPartyEntry(
      offset: Offset,
      party: String = someParty,
      isLocal: Boolean = true,
      reject: Boolean = false,
  ): DbDto.PartyEntry =
    DbDto.PartyEntry(
      ledger_offset = offset.unwrap,
      recorded_at = someTime.micros,
      submission_id = Some("submission_id"),
      party = Some(party),
      typ = if (reject) JdbcLedgerDao.rejectType else JdbcLedgerDao.acceptType,
      rejection_reason = Option.when(reject)("some rejection reason"),
      is_local = Some(isLocal),
    )

  def dtosCreate(
      // update related columns
      event_offset: Long = 10L,
      update_id: Array[Byte] = TestUpdateId("update").toProtoPrimitive.toByteArray,
      workflow_id: Option[String] = Some("workflow-id"),
      command_id: Option[String] = Some("command-id"),
      submitters: Option[Set[String]] = Some(Set("submitter1", "submitter2")),
      record_time: Long = 100L,
      synchronizer_id: SynchronizerId = someSynchronizerId,
      trace_context: Array[Byte] = serializableTraceContext,
      external_transaction_hash: Option[Array[Byte]] = Some(someExternalTransactionHashBinary),

      // event related columns
      event_sequential_id: Long = 500L,
      node_id: Int = 15,
      additional_witnesses: Set[String] = Set("witness1", "witness2"),
      representative_package_id: String = "representativepackage",

      // contract related columns
      notPersistedContractId: ContractId = hashCid("c1"),
      internal_contract_id: Long = 10,
      create_key_hash: Option[String] = Some("keyhash"),
  )(
      stakeholders: Set[String] = Set("stakeholder1", "stakeholder2"),
      template_id: String = "tem:pl:ate",
  ): Seq[DbDto] = DbDto
    .createDbDtos(
      event_offset = event_offset,
      update_id = update_id,
      workflow_id = workflow_id,
      command_id = command_id,
      submitters = submitters,
      record_time = record_time,
      synchronizer_id = synchronizer_id,
      trace_context = trace_context,
      external_transaction_hash = external_transaction_hash,
      event_sequential_id = event_sequential_id,
      node_id = node_id,
      additional_witnesses = additional_witnesses,
      representative_package_id = representative_package_id,
      notPersistedContractId = notPersistedContractId,
      internal_contract_id = internal_contract_id,
      create_key_hash = create_key_hash,
    )(
      stakeholders = stakeholders,
      template_id = template_id,
    )
    .toSeq

  def dtosAssign(
      // update related columns
      event_offset: Long = 10L,
      update_id: Array[Byte] = TestUpdateId("update").toProtoPrimitive.toByteArray,
      workflow_id: Option[String] = Some("workflow-id"),
      command_id: Option[String] = Some("command-id"),
      submitter: Option[String] = Some("submitter1"),
      record_time: Long = 100L,
      synchronizer_id: SynchronizerId = someSynchronizerId,
      trace_context: Array[Byte] = serializableTraceContext,

      // event related columns
      event_sequential_id: Long = 500L,
      node_id: Int = 15,
      source_synchronizer_id: SynchronizerId = someSynchronizerId2,
      reassignment_counter: Long = 345,
      reassignment_id: Array[Byte] = reassignmentId,
      representative_package_id: String = "representativepackage",

      // contract related columns
      notPersistedContractId: ContractId = hashCid("c1"),
      internal_contract_id: Long = 10,
  )(
      stakeholders: Set[String] = Set("stakeholder1", "stakeholder2"),
      template_id: String = "tem:pl:ate",
  ): Seq[DbDto] = DbDto
    .assignDbDtos(
      event_offset = event_offset,
      update_id = update_id,
      workflow_id = workflow_id,
      command_id = command_id,
      submitter = submitter,
      record_time = record_time,
      synchronizer_id = synchronizer_id,
      trace_context = trace_context,
      event_sequential_id = event_sequential_id,
      node_id = node_id,
      source_synchronizer_id = source_synchronizer_id,
      reassignment_counter = reassignment_counter,
      reassignment_id = reassignment_id,
      representative_package_id = representative_package_id,
      notPersistedContractId = notPersistedContractId,
      internal_contract_id = internal_contract_id,
    )(
      stakeholders = stakeholders,
      template_id = template_id,
    )
    .toSeq

  def dtosConsumingExercise(
      // update related columns
      event_offset: Long = 10L,
      update_id: Array[Byte] = TestUpdateId("update").toProtoPrimitive.toByteArray,
      workflow_id: Option[String] = Some("workflow-id"),
      command_id: Option[String] = Some("command-id"),
      submitters: Option[Set[String]] = Some(Set("submitter1", "submitter2")),
      record_time: Long = 100L,
      synchronizer_id: SynchronizerId = someSynchronizerId,
      trace_context: Array[Byte] = serializableTraceContext,
      external_transaction_hash: Option[Array[Byte]] = Some(someExternalTransactionHashBinary),

      // event related columns
      event_sequential_id: Long = 500L,
      node_id: Int = 15,
      deactivated_event_sequential_id: Option[Long] = Some(2L),
      additional_witnesses: Set[String] = Set("witness1", "witness2"),
      exercise_choice: String = "choice",
      exercise_choice_interface_id: Option[String] = Some("in:ter:face"),
      exercise_argument: Array[Byte] = Array(1, 2, 3),
      exercise_result: Option[Array[Byte]] = Some(Array(2, 3, 4)),
      exercise_actors: Set[String] = Set("actor1", "actor2"),
      exercise_last_descendant_node_id: Int = 3,
      exercise_argument_compression: Option[Int] = Some(1),
      exercise_result_compression: Option[Int] = Some(2),

      // contract related columns
      contract_id: ContractId = hashCid("c1"),
      internal_contract_id: Option[Long] = Some(10),
      template_id: String = "#tem:pl:ate",
      package_id: String = "package",
      stakeholders: Set[String] = Set("stakeholder1", "stakeholder2"),
      ledger_effective_time: Long = 123456,
  ): Seq[DbDto] = DbDto
    .consumingExerciseDbDtos(
      event_offset = event_offset,
      update_id = update_id,
      workflow_id = workflow_id,
      command_id = command_id,
      submitters = submitters,
      record_time = record_time,
      synchronizer_id = synchronizer_id,
      trace_context = trace_context,
      external_transaction_hash = external_transaction_hash,
      event_sequential_id = event_sequential_id,
      node_id = node_id,
      deactivated_event_sequential_id = deactivated_event_sequential_id,
      additional_witnesses = additional_witnesses,
      exercise_choice = exercise_choice,
      exercise_choice_interface_id = exercise_choice_interface_id,
      exercise_argument = exercise_argument,
      exercise_result = exercise_result,
      exercise_actors = exercise_actors,
      exercise_last_descendant_node_id = exercise_last_descendant_node_id,
      exercise_argument_compression = exercise_argument_compression,
      exercise_result_compression = exercise_result_compression,
      contract_id = contract_id,
      internal_contract_id = internal_contract_id,
      template_id = template_id,
      package_id = package_id,
      stakeholders = stakeholders,
      ledger_effective_time = ledger_effective_time,
    )
    .toSeq

  def dtosUnassign(
      // update related columns
      event_offset: Long = 10L,
      update_id: Array[Byte] = TestUpdateId("update").toProtoPrimitive.toByteArray,
      workflow_id: Option[String] = Some("workflow-id"),
      command_id: Option[String] = Some("command-id"),
      submitter: Option[String] = Some("submitter1"),
      record_time: Long = 100L,
      synchronizer_id: SynchronizerId = someSynchronizerId,
      trace_context: Array[Byte] = serializableTraceContext,

      // event related columns
      event_sequential_id: Long = 500L,
      node_id: Int = 15,
      deactivated_event_sequential_id: Option[Long] = Some(67),
      reassignment_id: Array[Byte] = reassignmentId,
      assignment_exclusivity: Option[Long] = Some(111333),
      target_synchronizer_id: SynchronizerId = someSynchronizerId2,
      reassignment_counter: Long = 345,

      // contract related columns
      contract_id: ContractId = hashCid("c1"),
      internal_contract_id: Option[Long] = Some(10),
      template_id: String = "#tem:pl:ate",
      package_id: String = "package",
      stakeholders: Set[String] = Set("stakeholder1", "stakeholder2"),
  ): Seq[DbDto] = DbDto
    .unassignDbDtos(
      event_offset = event_offset,
      update_id = update_id,
      workflow_id = workflow_id,
      command_id = command_id,
      submitter = submitter,
      record_time = record_time,
      synchronizer_id = synchronizer_id,
      trace_context = trace_context,
      event_sequential_id = event_sequential_id,
      node_id = node_id,
      deactivated_event_sequential_id = deactivated_event_sequential_id,
      reassignment_id = reassignment_id,
      assignment_exclusivity = assignment_exclusivity,
      target_synchronizer_id = target_synchronizer_id,
      reassignment_counter = reassignment_counter,
      contract_id = contract_id,
      internal_contract_id = internal_contract_id,
      template_id = template_id,
      package_id = package_id,
      stakeholders = stakeholders,
    )
    .toSeq

  def dtosWitnessedCreate(
      // update related columns
      event_offset: Long = 10L,
      update_id: Array[Byte] = TestUpdateId("update").toProtoPrimitive.toByteArray,
      workflow_id: Option[String] = Some("workflow-id"),
      command_id: Option[String] = Some("command-id"),
      submitters: Option[Set[String]] = Some(Set("submitter1", "submitter2")),
      record_time: Long = 100L,
      synchronizer_id: SynchronizerId = someSynchronizerId,
      trace_context: Array[Byte] = serializableTraceContext,
      external_transaction_hash: Option[Array[Byte]] = Some(someExternalTransactionHashBinary),

      // event related columns
      event_sequential_id: Long = 500L,
      node_id: Int = 15,
      additional_witnesses: Set[String] = Set("witness1", "witness2"),
      representative_package_id: String = "representativepackage",

      // contract related columns
      internal_contract_id: Long = 10,
  )(template_id: String = "tem:pl:ate"): Seq[DbDto] = DbDto
    .witnessedCreateDbDtos(
      event_offset = event_offset,
      update_id = update_id,
      workflow_id = workflow_id,
      command_id = command_id,
      submitters = submitters,
      record_time = record_time,
      synchronizer_id = synchronizer_id,
      trace_context = trace_context,
      external_transaction_hash = external_transaction_hash,
      event_sequential_id = event_sequential_id,
      node_id = node_id,
      additional_witnesses = additional_witnesses,
      representative_package_id = representative_package_id,
      internal_contract_id = internal_contract_id,
    )(
      template_id = template_id
    )
    .toSeq

  def dtosWitnessedExercised(
      // update related columns
      event_offset: Long = 10L,
      update_id: Array[Byte] = TestUpdateId("update").toProtoPrimitive.toByteArray,
      workflow_id: Option[String] = Some("workflow-id"),
      command_id: Option[String] = Some("command-id"),
      submitters: Option[Set[String]] = Some(Set("submitter1", "submitter2")),
      record_time: Long = 100L,
      synchronizer_id: SynchronizerId = someSynchronizerId,
      trace_context: Array[Byte] = serializableTraceContext,
      external_transaction_hash: Option[Array[Byte]] = Some(someExternalTransactionHashBinary),

      // event related columns
      event_sequential_id: Long = 500L,
      node_id: Int = 15,
      additional_witnesses: Set[String] = Set("witness1", "witness2"),
      consuming: Boolean = true,
      exercise_choice: String = "choice",
      exercise_choice_interface_id: Option[String] = Some("in:ter:face"),
      exercise_argument: Array[Byte] = Array(1, 2, 3),
      exercise_result: Option[Array[Byte]] = Some(Array(2, 3, 4)),
      exercise_actors: Set[String] = Set("actor1", "actor2"),
      exercise_last_descendant_node_id: Int = 3,
      exercise_argument_compression: Option[Int] = Some(1),
      exercise_result_compression: Option[Int] = Some(2),

      // contract related columns
      contract_id: ContractId = hashCid("c1"),
      internal_contract_id: Option[Long] = Some(10),
      template_id: String = "#tem:pl:ate",
      package_id: String = "package",
      ledger_effective_time: Long = 123456,
  ): Seq[DbDto] = DbDto
    .witnessedExercisedDbDtos(
      event_offset = event_offset,
      update_id = update_id,
      workflow_id = workflow_id,
      command_id = command_id,
      submitters = submitters,
      record_time = record_time,
      synchronizer_id = synchronizer_id,
      trace_context = trace_context,
      external_transaction_hash = external_transaction_hash,
      event_sequential_id = event_sequential_id,
      node_id = node_id,
      additional_witnesses = additional_witnesses,
      consuming = consuming,
      exercise_choice = exercise_choice,
      exercise_choice_interface_id = exercise_choice_interface_id,
      exercise_argument = exercise_argument,
      exercise_result = exercise_result,
      exercise_actors = exercise_actors,
      exercise_last_descendant_node_id = exercise_last_descendant_node_id,
      exercise_argument_compression = exercise_argument_compression,
      exercise_result_compression = exercise_result_compression,
      contract_id = contract_id,
      internal_contract_id = internal_contract_id,
      template_id = template_id,
      package_id = package_id,
      ledger_effective_time = ledger_effective_time,
    )
    .toSeq

  def dtoPartyToParticipant(
      offset: Offset,
      eventSequentialId: Long,
      party: String = someParty,
      participant: String = someParticipantId.toString,
      authorizationEvent: AuthorizationEvent = Added(AuthorizationLevel.Submission),
      synchronizerId: SynchronizerId = someSynchronizerId,
      recordTime: Timestamp = someTime,
      traceContext: Array[Byte] = serializableTraceContext,
  ): DbDto.EventPartyToParticipant = {
    val updateId = updateIdArrayFromOffset(offset)
    DbDto.EventPartyToParticipant(
      event_sequential_id = eventSequentialId,
      event_offset = offset.unwrap,
      update_id = updateId,
      party_id = party,
      participant_id = participant,
      participant_permission = participantPermissionInt(authorizationEvent),
      participant_authorization_event = authorizationEventInt(authorizationEvent),
      synchronizer_id = synchronizerId,
      record_time = recordTime.micros,
      trace_context = traceContext,
    )
  }

  def dtoCompletion(
      offset: Offset,
      submitters: Set[String] = Set("signatory"),
      commandId: String = UUID.randomUUID().toString,
      userId: String = someUserId,
      submissionId: Option[String] = Some(UUID.randomUUID().toString),
      deduplicationOffset: Option[Long] = None,
      deduplicationDurationSeconds: Option[Long] = None,
      deduplicationDurationNanos: Option[Int] = None,
      synchronizerId: SynchronizerId = someSynchronizerId,
      traceContext: Array[Byte] = serializableTraceContext,
      recordTime: Timestamp = someTime,
      messageUuid: Option[String] = None,
      updateId: Option[Array[Byte]] = Some(new Array[Byte](0)),
      publicationTime: Timestamp = someTime,
      isTransaction: Boolean = true,
  ): DbDto.CommandCompletion =
    DbDto.CommandCompletion(
      completion_offset = offset.unwrap,
      record_time = recordTime.micros,
      publication_time = publicationTime.micros,
      user_id = userId,
      submitters = submitters,
      command_id = commandId,
      update_id = updateId.filter(_.isEmpty).map(_ => updateIdArrayFromOffset(offset)),
      rejection_status_code = None,
      rejection_status_message = None,
      rejection_status_details = None,
      submission_id = submissionId,
      deduplication_offset = deduplicationOffset,
      deduplication_duration_seconds = deduplicationDurationSeconds,
      deduplication_duration_nanos = deduplicationDurationNanos,
      synchronizer_id = synchronizerId,
      message_uuid = messageUuid,
      is_transaction = isTransaction,
      trace_context = traceContext,
    )

  def dtoTransactionMeta(
      offset: Offset,
      event_sequential_id_first: Long,
      event_sequential_id_last: Long,
      recordTime: Timestamp = someTime,
      udpateId: Option[Array[Byte]] = None,
      synchronizerId: SynchronizerId = someSynchronizerId,
      publicationTime: Timestamp = someTime,
  ): DbDto.TransactionMeta = DbDto.TransactionMeta(
    update_id = udpateId.getOrElse(updateIdArrayFromOffset(offset)),
    event_offset = offset.unwrap,
    publication_time = publicationTime.micros,
    record_time = recordTime.micros,
    synchronizer_id = synchronizerId,
    event_sequential_id_first = event_sequential_id_first,
    event_sequential_id_last = event_sequential_id_last,
  )

  def dtoInterning(
      internal: Int,
      external: String,
  ): DbDto.StringInterningDto = DbDto.StringInterningDto(
    internalId = internal,
    externalString = external,
  )

  def dtoUpdateId(dto: DbDto): UpdateId =
    dto match {
      case _ => sys.error(s"$dto does not have a transaction id")
    }

  def dtoEventSeqId(dto: DbDto): Long =
    dto match {
      case e: DbDto.EventActivate => e.event_sequential_id
      case e: DbDto.EventDeactivate => e.event_sequential_id
      case e: DbDto.EventVariousWitnessed => e.event_sequential_id
      case e: DbDto.IdFilterDbDto => e.idFilter.event_sequential_id
      case _ => sys.error(s"$dto does not have a event sequential id")
    }

  def dtoOffset(dto: DbDto): Long =
    dto match {
      case _ => sys.error(s"$dto does not have a offset id")
    }

  def dtoUserId(dto: DbDto): Ref.UserId =
    dto match {
      case e: DbDto.CommandCompletion => Ref.UserId.assertFromString(e.user_id)
      case _ => sys.error(s"$dto does not have an user id")
    }

  def metaFromSingle(dbDto: DbDto): DbDto.TransactionMeta = DbDto.TransactionMeta(
    update_id = dtoUpdateId(dbDto).toProtoPrimitive.toByteArray,
    event_offset = dtoOffset(dbDto),
    publication_time = someTime.micros,
    record_time = someTime.micros,
    synchronizer_id = someSynchronizerId,
    event_sequential_id_first = dtoEventSeqId(dbDto),
    event_sequential_id_last = dtoEventSeqId(dbDto),
  )

  def meta(
      // update related columns
      event_offset: Long = 10L,
      update_id: Array[Byte] = TestUpdateId("update").toProtoPrimitive.toByteArray,
      workflow_id: Option[String] = Some("workflow-id"),
      command_id: Option[String] = Some("command-id"),
      submitters: Option[Set[String]] = Some(Set("submitter1")),
      record_time: Long = 100L,
      synchronizer_id: SynchronizerId = someSynchronizerId,
      trace_context: Array[Byte] = serializableTraceContext,
      external_transaction_hash: Option[Array[Byte]] = Some(someExternalTransactionHashBinary),
      // meta related columns
      publication_time: Long = 1000,
  )(dbDtosInOrder: Seq[DbDto]): Vector[DbDto] =
    dbDtosInOrder
      .map {
        case dto: DbDto.EventActivate =>
          dto.copy(
            event_offset = event_offset,
            update_id = update_id,
            workflow_id = workflow_id,
            command_id = command_id,
            submitters = submitters,
            record_time = record_time,
            synchronizer_id = synchronizer_id,
            trace_context = trace_context,
            external_transaction_hash = external_transaction_hash,
          )
        case dto: DbDto.EventDeactivate =>
          dto.copy(
            event_offset = event_offset,
            update_id = update_id,
            workflow_id = workflow_id,
            command_id = command_id,
            submitters = submitters,
            record_time = record_time,
            synchronizer_id = synchronizer_id,
            trace_context = trace_context,
            external_transaction_hash = external_transaction_hash,
          )
        case dto: DbDto.EventVariousWitnessed =>
          dto.copy(
            event_offset = event_offset,
            update_id = update_id,
            workflow_id = workflow_id,
            command_id = command_id,
            submitters = submitters,
            record_time = record_time,
            synchronizer_id = synchronizer_id,
            trace_context = trace_context,
            external_transaction_hash = external_transaction_hash,
          )
        case x => x
      }
      .toVector
      .appended(
        DbDto.TransactionMeta(
          event_offset = event_offset,
          update_id = update_id,
          record_time = record_time,
          synchronizer_id = synchronizer_id,
          publication_time = publication_time,
          event_sequential_id_first = dtoEventSeqId(dbDtosInOrder.headOption.value),
          event_sequential_id_last = dtoEventSeqId(dbDtosInOrder.lastOption.value),
        )
      )
}
