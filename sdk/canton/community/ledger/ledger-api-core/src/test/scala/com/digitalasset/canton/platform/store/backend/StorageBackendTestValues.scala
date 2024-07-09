// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.domain.ParticipantId
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.TransactionMetering
import com.digitalasset.canton.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.digitalasset.canton.platform.store.dao.JdbcLedgerDao
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.ledger.EventId
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.daml.lf.value.Value.ContractId
import com.google.protobuf.ByteString

import java.time.Instant
import java.util.UUID

/** Except where specified, values should be treated as opaque
  */
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
private[store] object StorageBackendTestValues {

  def hashCid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  /** Produces offsets that are ordered the same as the input value */
  def offset(x: Long): Offset = Offset.fromHexString(Ref.HexString.assertFromString(f"$x%08d"))
  def ledgerEnd(o: Long, e: Long): ParameterStorageBackend.LedgerEnd =
    ParameterStorageBackend.LedgerEnd(offset(o), e, 0)
  def transactionIdFromOffset(x: Offset): Ref.LedgerString =
    Ref.LedgerString.assertFromString(x.toHexString)

  def timestampFromInstant(i: Instant): Timestamp = Timestamp.assertFromInstant(i)
  val someTime: Timestamp = timestampFromInstant(Instant.now())

  val someParticipantId: ParticipantId = ParticipantId(
    Ref.ParticipantId.assertFromString("participant")
  )
  val someTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("pkg:Mod:Template")
  val someTemplateId2: Ref.Identifier = Ref.Identifier.assertFromString("pkg:Mod:Template2")
  val someTemplateId3: Ref.Identifier = Ref.Identifier.assertFromString("pkg:Mod:Template3")
  val somePackageName: Ref.PackageName = Ref.PackageName.assertFromString("pkg-name")
  val somePackageVersion: Ref.PackageVersion = Ref.PackageVersion.assertFromString("1.2.3")
  val someIdentityParams: ParameterStorageBackend.IdentityParams =
    ParameterStorageBackend.IdentityParams(someParticipantId)
  val someParty: Ref.Party = Ref.Party.assertFromString("party")
  val someParty2: Ref.Party = Ref.Party.assertFromString("party2")
  val someParty3: Ref.Party = Ref.Party.assertFromString("party3")
  val someApplicationId: Ref.ApplicationId = Ref.ApplicationId.assertFromString("application_id")
  val someSubmissionId: Ref.SubmissionId = Ref.SubmissionId.assertFromString("submission_id")
  val someLedgerMeteringEnd: LedgerMeteringEnd = LedgerMeteringEnd(Offset.beforeBegin, someTime)
  val someDriverMetadata = Bytes.assertFromString("00abcd")
  val someDriverMetadataBytes = someDriverMetadata.toByteArray

  val someArchive: DamlLf.Archive = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build

  // This is not a valid serialization of a Daml-Lf value. This is ok.
  // The tests never deserialize Daml-Lf values, we the just need some non-empty array
  // because Oracle converts empty arrays to NULL, which then breaks non-null constraints.
  val someSerializedDamlLfValue: Array[Byte] = Array.fill[Byte](8)(15)

  private val serializableTraceContext: Array[Byte] =
    SerializableTraceContext(TraceContext.empty).toDamlProto.toByteArray

  def dtoPartyEntry(
      offset: Offset,
      party: String = someParty,
      isLocal: Boolean = true,
      displayNameOverride: Option[Option[String]] = None,
      reject: Boolean = false,
  ): DbDto.PartyEntry = {
    val displayName = displayNameOverride.getOrElse(Some(party))
    DbDto.PartyEntry(
      ledger_offset = offset.toHexString,
      recorded_at = someTime.micros,
      submission_id = Some("submission_id"),
      party = Some(party),
      display_name = displayName,
      typ = if (reject) JdbcLedgerDao.rejectType else JdbcLedgerDao.acceptType,
      rejection_reason = Option.when(reject)("some rejection reason"),
      is_local = Some(isLocal),
    )
  }

  /** A simple create event.
    * Corresponds to a transaction with a single create node.
    */
  def dtoCreate(
      offset: Offset,
      eventSequentialId: Long,
      contractId: ContractId,
      signatory: String = "signatory",
      observer: String = "observer",
      nonStakeholderInformees: Set[String] = Set.empty,
      commandId: String = UUID.randomUUID().toString,
      ledgerEffectiveTime: Timestamp = someTime,
      driverMetadata: Option[Array[Byte]] = None,
      keyHash: Option[String] = None,
      domainId: String = "x::sourcedomain",
      createKey: Option[Array[Byte]] = None,
      createKeyMaintainer: Option[String] = None,
      traceContext: Array[Byte] = serializableTraceContext,
  ): DbDto.EventCreate = {
    val transactionId = transactionIdFromOffset(offset)
    val stakeholders = Set(signatory, observer)
    val informees = stakeholders ++ nonStakeholderInformees
    DbDto.EventCreate(
      event_offset = offset.toHexString,
      transaction_id = transactionId,
      ledger_effective_time = ledgerEffectiveTime.micros,
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some(someApplicationId),
      submitters = None,
      node_index = 0,
      event_id = EventId(transactionId, NodeId(0)).toLedgerString,
      contract_id = contractId.coid,
      template_id = someTemplateId.toString,
      package_name = somePackageName.toString,
      package_version = Some(somePackageVersion.toString()),
      flat_event_witnesses = stakeholders,
      tree_event_witnesses = informees,
      create_argument = someSerializedDamlLfValue,
      create_signatories = Set(signatory),
      create_observers = Set(observer),
      create_key_value = createKey,
      create_key_maintainers = createKeyMaintainer.map(Set(_)),
      create_key_hash = keyHash,
      create_argument_compression = None,
      create_key_value_compression = None,
      event_sequential_id = eventSequentialId,
      driver_metadata = driverMetadata,
      domain_id = domainId,
      trace_context = traceContext,
      record_time = someTime.micros,
    )
  }

  /** A simple exercise event.
    * Corresponds to a transaction with a single exercise node.
    *
    * @param signatory The signatory of the contract (see corresponding create node)
    * @param actor The choice actor, who is also the submitter
    */
  def dtoExercise(
      offset: Offset,
      eventSequentialId: Long,
      consuming: Boolean,
      contractId: ContractId,
      signatory: String = "signatory",
      actor: String = "actor",
      commandId: String = UUID.randomUUID().toString,
      domainId: String = "x::sourcedomain",
      traceContext: Array[Byte] = serializableTraceContext,
  ): DbDto.EventExercise = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.EventExercise(
      consuming = consuming,
      event_offset = offset.toHexString,
      transaction_id = transactionId,
      ledger_effective_time = someTime.micros,
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some(someApplicationId),
      submitters = Some(Set(actor)),
      node_index = 0,
      event_id = EventId(transactionId, NodeId(0)).toLedgerString,
      contract_id = contractId.coid,
      template_id = someTemplateId.toString,
      package_name = somePackageName,
      flat_event_witnesses = if (consuming) Set(signatory) else Set.empty,
      tree_event_witnesses = Set(signatory, actor),
      create_key_value = None,
      exercise_choice = "exercise_choice",
      exercise_argument = someSerializedDamlLfValue,
      exercise_result = Some(someSerializedDamlLfValue),
      exercise_actors = Set(actor),
      exercise_child_event_ids = Vector.empty,
      create_key_value_compression = None,
      exercise_argument_compression = None,
      exercise_result_compression = None,
      event_sequential_id = eventSequentialId,
      domain_id = domainId,
      trace_context = traceContext,
      record_time = someTime.micros,
    )
  }

  def dtoAssign(
      offset: Offset,
      eventSequentialId: Long,
      contractId: ContractId,
      signatory: String = "signatory",
      observer: String = "observer",
      commandId: String = UUID.randomUUID().toString,
      driverMetadata: Bytes = someDriverMetadata,
      sourceDomainId: String = "x::sourcedomain",
      targetDomainId: String = "x::targetdomain",
      traceContext: Array[Byte] = serializableTraceContext,
  ): DbDto.EventAssign = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.EventAssign(
      event_offset = offset.toHexString,
      update_id = transactionId,
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      submitter = Option(someParty),
      contract_id = contractId.coid,
      template_id = someTemplateId.toString,
      package_name = somePackageName.toString,
      package_version = Some(somePackageVersion.toString()),
      flat_event_witnesses = Set(signatory, observer),
      create_argument = someSerializedDamlLfValue,
      create_signatories = Set(signatory),
      create_observers = Set(observer),
      create_key_value = None,
      create_key_maintainers = None,
      create_key_hash = None,
      create_argument_compression = Some(123),
      create_key_value_compression = Some(456),
      event_sequential_id = eventSequentialId,
      ledger_effective_time = someTime.micros,
      driver_metadata = driverMetadata.toByteArray,
      source_domain_id = sourceDomainId,
      target_domain_id = targetDomainId,
      unassign_id = "123456789",
      reassignment_counter = 1000L,
      trace_context = traceContext,
      record_time = someTime.micros,
    )
  }

  def dtoUnassign(
      offset: Offset,
      eventSequentialId: Long,
      contractId: ContractId,
      signatory: String = "signatory",
      observer: String = "observer",
      commandId: String = UUID.randomUUID().toString,
      sourceDomainId: String = "x::sourcedomain",
      targetDomainId: String = "x::targetdomain",
      traceContext: Array[Byte] = serializableTraceContext,
  ): DbDto.EventUnassign = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.EventUnassign(
      event_offset = offset.toHexString,
      update_id = transactionId,
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      submitter = Option(someParty),
      contract_id = contractId.coid,
      template_id = someTemplateId.toString,
      package_name = somePackageName,
      flat_event_witnesses = Set(signatory, observer),
      event_sequential_id = eventSequentialId,
      source_domain_id = sourceDomainId,
      target_domain_id = targetDomainId,
      unassign_id = "123456789",
      reassignment_counter = 1000L,
      assignment_exclusivity = Some(11111),
      trace_context = traceContext,
      record_time = someTime.micros,
    )
  }

  def dtoCompletion(
      offset: Offset,
      submitter: String = "signatory",
      commandId: String = UUID.randomUUID().toString,
      applicationId: String = someApplicationId,
      submissionId: Option[String] = Some(UUID.randomUUID().toString),
      deduplicationOffset: Option[String] = None,
      deduplicationDurationSeconds: Option[Long] = None,
      deduplicationDurationNanos: Option[Int] = None,
      deduplicationStart: Option[Timestamp] = None,
      domainId: String = "x::sourcedomain",
      traceContext: Array[Byte] = serializableTraceContext,
  ): DbDto.CommandCompletion =
    DbDto.CommandCompletion(
      completion_offset = offset.toHexString,
      record_time = someTime.micros,
      application_id = applicationId,
      submitters = Set(submitter),
      command_id = commandId,
      transaction_id = Some(transactionIdFromOffset(offset)),
      rejection_status_code = None,
      rejection_status_message = None,
      rejection_status_details = None,
      submission_id = submissionId,
      deduplication_offset = deduplicationOffset,
      deduplication_duration_seconds = deduplicationDurationSeconds,
      deduplication_duration_nanos = deduplicationDurationNanos,
      deduplication_start = deduplicationStart.map(_.micros),
      domain_id = domainId,
      trace_context = traceContext,
    )

  def dtoTransactionMeta(
      offset: Offset,
      event_sequential_id_first: Long,
      event_sequential_id_last: Long,
  ): DbDto.TransactionMeta = DbDto.TransactionMeta(
    transactionIdFromOffset(offset),
    event_offset = offset.toHexString,
    event_sequential_id_first = event_sequential_id_first,
    event_sequential_id_last = event_sequential_id_last,
  )

  def dtoTransactionMetering(
      metering: TransactionMetering
  ): DbDto.TransactionMetering = {
    import metering.*
    DbDto.TransactionMetering(
      applicationId,
      actionCount,
      meteringTimestamp.micros,
      ledgerOffset.toHexString,
    )
  }

  def dtoCreateFilter(
      event_sequential_id: Long,
      template_id: Ref.Identifier,
      party_id: String,
  ): DbDto.IdFilterCreateStakeholder =
    DbDto.IdFilterCreateStakeholder(event_sequential_id, template_id.toString, party_id)

  def dtoInterning(
      internal: Int,
      external: String,
  ): DbDto.StringInterningDto = DbDto.StringInterningDto(
    internalId = internal,
    externalString = external,
  )

  def dtoTransactionId(dto: DbDto): Ref.TransactionId = {
    dto match {
      case e: DbDto.EventCreate => Ref.TransactionId.assertFromString(e.transaction_id)
      case e: DbDto.EventExercise => Ref.TransactionId.assertFromString(e.transaction_id)
      case e: DbDto.EventAssign => Ref.TransactionId.assertFromString(e.update_id)
      case e: DbDto.EventUnassign => Ref.TransactionId.assertFromString(e.update_id)
      case _ => sys.error(s"$dto does not have a transaction id")
    }
  }

  def dtoEventSeqId(dto: DbDto): Long = {
    dto match {
      case e: DbDto.EventCreate => e.event_sequential_id
      case e: DbDto.EventExercise => e.event_sequential_id
      case e: DbDto.EventAssign => e.event_sequential_id
      case e: DbDto.EventUnassign => e.event_sequential_id
      case _ => sys.error(s"$dto does not have a event sequential id")
    }
  }

  def dtoOffset(dto: DbDto): String = {
    dto match {
      case e: DbDto.EventCreate =>
        e.event_offset
      case e: DbDto.EventExercise =>
        e.event_offset
      case e: DbDto.EventAssign => e.event_offset
      case e: DbDto.EventUnassign => e.event_offset
      case _ => sys.error(s"$dto does not have a offset id")
    }
  }

  def dtoApplicationId(dto: DbDto): Ref.ApplicationId = {
    dto match {
      case e: DbDto.EventCreate => Ref.ApplicationId.assertFromString(e.application_id.get)
      case e: DbDto.EventExercise => Ref.ApplicationId.assertFromString(e.application_id.get)
      case e: DbDto.CommandCompletion => Ref.ApplicationId.assertFromString(e.application_id)
      case _ => sys.error(s"$dto does not have an application id")
    }
  }

  def metaFromSingle(dbDto: DbDto): DbDto.TransactionMeta = DbDto.TransactionMeta(
    transaction_id = dtoTransactionId(dbDto),
    event_offset = dtoOffset(dbDto),
    event_sequential_id_first = dtoEventSeqId(dbDto),
    event_sequential_id_last = dtoEventSeqId(dbDto),
  )
}
