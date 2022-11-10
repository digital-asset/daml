// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.TransactionMetering
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.NodeId
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.dao.JdbcLedgerDao
import com.daml.platform.store.backend.MeteringParameterStorageBackend.LedgerMeteringEnd
import com.google.protobuf.ByteString

import java.time.{Duration, Instant}
import java.util.UUID

/** Except where specified, values should be treated as opaque
  */
private[backend] object StorageBackendTestValues {

  def hashCid(key: String): ContractId = ContractId.V1(Hash.hashPrivateKey(key))

  /** Produces offsets that are ordered the same as the input value */
  def offset(x: Long): Offset = Offset.fromHexString(Ref.HexString.assertFromString(f"$x%08d"))
  def ledgerEnd(o: Long, e: Long): ParameterStorageBackend.LedgerEnd =
    ParameterStorageBackend.LedgerEnd(offset(o), e, 0)
  def transactionIdFromOffset(x: Offset): Ref.LedgerString =
    Ref.LedgerString.assertFromString(x.toHexString)

  def timestampFromInstant(i: Instant): Timestamp = Timestamp.assertFromInstant(i)
  val someTime: Timestamp = timestampFromInstant(Instant.now())

  val someConfiguration: Configuration =
    Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofHours(23))

  val someLedgerId: LedgerId = LedgerId("ledger")
  val someParticipantId: ParticipantId = ParticipantId(
    Ref.ParticipantId.assertFromString("participant")
  )
  val someTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("pkg:Mod:Template")
  val someIdentityParams: ParameterStorageBackend.IdentityParams =
    ParameterStorageBackend.IdentityParams(someLedgerId, someParticipantId)
  val someParty: Ref.Party = Ref.Party.assertFromString("party")
  val someApplicationId: Ref.ApplicationId = Ref.ApplicationId.assertFromString("application_id")
  val someSubmissionId: Ref.SubmissionId = Ref.SubmissionId.assertFromString("submission_id")
  val someLedgerMeteringEnd: LedgerMeteringEnd = LedgerMeteringEnd(Offset.beforeBegin, someTime)

  val someArchive: DamlLf.Archive = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build

  // This is not a valid serialization of a Daml-Lf value. This is ok.
  // The tests never deserialize Daml-Lf values, we the just need some non-empty array
  // because Oracle converts empty arrays to NULL, which then breaks non-null constraints.
  val someSerializedDamlLfValue: Array[Byte] = Array.fill[Byte](8)(15)

  def dtoConfiguration(
      offset: Offset,
      configuration: Configuration = someConfiguration,
  ): DbDto.ConfigurationEntry =
    DbDto.ConfigurationEntry(
      ledger_offset = offset.toHexString,
      recorded_at = someTime.micros,
      submission_id = "submission_id",
      typ = JdbcLedgerDao.acceptType,
      configuration = Configuration.encode(configuration).toByteArray,
      rejection_reason = None,
    )

  def dtoPartyEntry(
      offset: Offset,
      party: String = someParty,
      isLocal: Boolean = true,
      displayNameOverride: Option[Option[String]] = None,
  ): DbDto.PartyEntry = {
    val displayName = displayNameOverride.getOrElse(Some(party))
    DbDto.PartyEntry(
      ledger_offset = offset.toHexString,
      recorded_at = someTime.micros,
      submission_id = Some("submission_id"),
      party = Some(party),
      display_name = displayName,
      typ = JdbcLedgerDao.acceptType,
      rejection_reason = None,
      is_local = Some(isLocal),
    )
  }

  def dtoPackage(offset: Offset): DbDto.Package = DbDto.Package(
    package_id = someArchive.getHash,
    upload_id = "upload_id",
    source_description = Some("source_description"),
    package_size = someArchive.getPayload.size.toLong,
    known_since = someTime.micros,
    ledger_offset = offset.toHexString,
    _package = someArchive.toByteArray,
  )

  def dtoPackageEntry(offset: Offset): DbDto.PackageEntry = DbDto.PackageEntry(
    ledger_offset = offset.toHexString,
    recorded_at = someTime.micros,
    submission_id = Some("submission_id"),
    typ = JdbcLedgerDao.acceptType,
    rejection_reason = None,
  )

  /** A simple create event.
    * Corresponds to a transaction with a single create node.
    */
  def dtoCreate(
      offset: Offset,
      eventSequentialId: Long,
      contractId: ContractId,
      signatory: String = "signatory",
      observer: String = "observer",
      commandId: String = UUID.randomUUID().toString,
      ledgerEffectiveTime: Option[Timestamp] = Some(someTime),
  ): DbDto.EventCreate = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.EventCreate(
      event_offset = Some(offset.toHexString),
      transaction_id = Some(transactionId),
      ledger_effective_time = ledgerEffectiveTime.map(_.micros),
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some(someApplicationId),
      submitters = None,
      node_index = Some(0),
      event_id = Some(EventId(transactionId, NodeId(0)).toLedgerString),
      contract_id = contractId.coid,
      template_id = Some(someTemplateId.toString),
      flat_event_witnesses = Set(signatory, observer),
      tree_event_witnesses = Set(signatory, observer),
      create_argument = Some(someSerializedDamlLfValue),
      create_signatories = Some(Set(signatory)),
      create_observers = Some(Set(observer)),
      create_agreement_text = None,
      create_key_value = None,
      create_key_hash = None,
      create_argument_compression = None,
      create_key_value_compression = None,
      event_sequential_id = eventSequentialId,
      driver_metadata = None,
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
  ): DbDto.EventExercise = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.EventExercise(
      consuming = consuming,
      event_offset = Some(offset.toHexString),
      transaction_id = Some(transactionId),
      ledger_effective_time = Some(someTime.micros),
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some(someApplicationId),
      submitters = Some(Set(actor)),
      node_index = Some(0),
      event_id = Some(EventId(transactionId, NodeId(0)).toLedgerString),
      contract_id = contractId.coid,
      template_id = Some(someTemplateId.toString),
      flat_event_witnesses = if (consuming) Set(signatory) else Set.empty,
      tree_event_witnesses = Set(signatory, actor),
      create_key_value = None,
      exercise_choice = Some("exercise_choice"),
      exercise_argument = Some(someSerializedDamlLfValue),
      exercise_result = Some(someSerializedDamlLfValue),
      exercise_actors = Some(Set(actor)),
      exercise_child_event_ids = Some(Vector.empty),
      create_key_value_compression = None,
      exercise_argument_compression = None,
      exercise_result_compression = None,
      event_sequential_id = eventSequentialId,
    )
  }

  /** A single divulgence event
    */
  def dtoDivulgence(
      offset: Option[Offset],
      eventSequentialId: Long,
      contractId: ContractId,
      submitter: String = "signatory",
      divulgee: String = "divulgee",
      commandId: String = UUID.randomUUID().toString,
  ): DbDto.EventDivulgence = {
    DbDto.EventDivulgence(
      event_offset = offset.map(_.toHexString),
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some(someApplicationId),
      submitters = Some(Set(submitter)),
      contract_id = contractId.coid,
      template_id = Some(someTemplateId.toString),
      tree_event_witnesses = Set(divulgee),
      create_argument = Some(someSerializedDamlLfValue),
      create_argument_compression = None,
      event_sequential_id = eventSequentialId,
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
    )

  def dtoTransactionMetering(
      metering: TransactionMetering
  ): DbDto.TransactionMetering = {
    import metering._
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
  ): DbDto.FilterCreateStakeholder =
    DbDto.FilterCreateStakeholder(event_sequential_id, template_id.toString, party_id)

  def dtoInterning(
      internal: Int,
      external: String,
  ): DbDto.StringInterningDto = DbDto.StringInterningDto(
    internalId = internal,
    externalString = external,
  )

  def dtoTransactionId(dto: DbDto): Ref.TransactionId = {
    dto match {
      case e: DbDto.EventCreate => Ref.TransactionId.assertFromString(e.transaction_id.get)
      case e: DbDto.EventExercise => Ref.TransactionId.assertFromString(e.transaction_id.get)
      case _ => sys.error(s"$dto does not have a transaction id")
    }
  }

  def dtoApplicationId(dto: DbDto): Ref.ApplicationId = {
    dto match {
      case e: DbDto.EventCreate => Ref.ApplicationId.assertFromString(e.application_id.get)
      case e: DbDto.EventExercise => Ref.ApplicationId.assertFromString(e.application_id.get)
      case e: DbDto.EventDivulgence => Ref.ApplicationId.assertFromString(e.application_id.get)
      case e: DbDto.CommandCompletion => Ref.ApplicationId.assertFromString(e.application_id)
      case _ => sys.error(s"$dto does not have an application id")
    }
  }
}
