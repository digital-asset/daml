// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.time.{Duration, Instant}
import java.util.UUID

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain.{LedgerId, ParticipantId}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.NodeId
import com.daml.platform.store.appendonlydao.JdbcLedgerDao
import com.google.protobuf.ByteString

/** Except where specified, values should be treated as opaque
  */
private[backend] object StorageBackendTestValues {

  /** Produces offsets that are ordered the same as the input value */
  def offset(x: Long): Offset = Offset.fromHexString(Ref.HexString.assertFromString(f"$x%08X"))
  def ledgerEnd(o: Long, e: Long): ParameterStorageBackend.LedgerEnd = ParameterStorageBackend.LedgerEnd(offset(o), e)
  def transactionIdFromOffset(x: Offset): Ref.LedgerString =
    Ref.LedgerString.assertFromString(x.toHexString)

  val someTime: Instant = Instant.now()

  val someConfiguration: Configuration =
    Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofHours(23))

  val someLedgerId: LedgerId = LedgerId("ledger")
  val someParticipantId: ParticipantId = ParticipantId(
    Ref.ParticipantId.assertFromString("participant")
  )
  val someTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("pkg:Mod:Template")
  val someIdentityParams: ParameterStorageBackend.IdentityParams =
    ParameterStorageBackend.IdentityParams(someLedgerId, someParticipantId)

  val someArchive: DamlLf.Archive = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build

  // This is not a valid serialization of a Daml-Lf value. This is ok.
  // The tests never deserialize Daml-Lf values, we the just need some non-empty array
  // because Oracle converts empty arrays to NULL, which then breaks non-null constraints.
  val someSerializedDamlLfValue: Array[Byte] = Array.fill[Byte](8)(15)

  def dtoConfiguration(offset: Offset): List[DbDto] = List(
    DbDto.ConfigurationEntry(
      ledger_offset = offset.toHexString,
      recorded_at = someTime,
      submission_id = "submission_id",
      typ = JdbcLedgerDao.acceptType,
      configuration = Configuration.encode(someConfiguration).toByteArray,
      rejection_reason = None,
    )
  )

  def dtoParty(
      offset: Offset,
      party: String = "party",
  ): List[DbDto] = List(
    DbDto.PartyEntry(
      ledger_offset = offset.toHexString,
      recorded_at = someTime,
      submission_id = Some("submission_id"),
      party = Some(party),
      display_name = Some(party),
      typ = JdbcLedgerDao.acceptType,
      rejection_reason = None,
      is_local = Some(true),
    ),
    DbDto.Party(
      party = party,
      display_name = Some(party),
      explicit = true,
      ledger_offset = Some(offset.toHexString),
      is_local = true,
    ),
  )

  def dtoPackage(offset: Offset): List[DbDto] = List(
    DbDto.Package(
      package_id = someArchive.getHash,
      upload_id = "upload_id",
      source_description = Some("source_description"),
      package_size = someArchive.getPayload.size.toLong,
      known_since = someTime,
      ledger_offset = offset.toHexString,
      _package = someArchive.toByteArray,
    ),
    DbDto.PackageEntry(
      ledger_offset = offset.toHexString,
      recorded_at = someTime,
      submission_id = Some("submission_id"),
      typ = JdbcLedgerDao.acceptType,
      rejection_reason = None,
    ),
  )

  /** A simple create event.
    * Corresponds to a transaction with a single create node.
    */
  def dtoCreate(
      offset: Offset,
      eventSequentialId: Long,
      contractId: String,
      signatory: String = "signatory",
      observer: String = "observer",
      commandId: String = UUID.randomUUID().toString,
  ): DbDto.EventCreate = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.EventCreate(
      event_offset = Some(offset.toHexString),
      transaction_id = Some(transactionId),
      ledger_effective_time = Some(someTime),
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some("application_id"),
      submitters = None,
      node_index = Some(0),
      event_id = Some(EventId(transactionId, NodeId(0)).toLedgerString),
      contract_id = contractId,
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
      contractId: String,
      signatory: String = "signatory",
      actor: String = "actor",
      commandId: String = UUID.randomUUID().toString,
  ): DbDto.EventExercise = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.EventExercise(
      consuming = true,
      event_offset = Some(offset.toHexString),
      transaction_id = Some(transactionId),
      ledger_effective_time = Some(someTime),
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some("application_id"),
      submitters = Some(Set(actor)),
      node_index = Some(0),
      event_id = Some(EventId(transactionId, NodeId(0)).toLedgerString),
      contract_id = contractId,
      template_id = Some(someTemplateId.toString),
      flat_event_witnesses = if (consuming) Set(signatory) else Set.empty,
      tree_event_witnesses = Set(signatory, actor),
      create_key_value = None,
      exercise_choice = Some("exercise_choice"),
      exercise_argument = Some(someSerializedDamlLfValue),
      exercise_result = Some(someSerializedDamlLfValue),
      exercise_actors = Some(Set(actor)),
      exercise_child_event_ids = Some(Set.empty),
      create_key_value_compression = None,
      exercise_argument_compression = None,
      exercise_result_compression = None,
      event_sequential_id = eventSequentialId,
    )
  }

  /** A single divulgence event
    */
  def dtoDivulgence(
      offset: Offset,
      eventSequentialId: Long,
      contractId: String,
      submitter: String = "signatory",
      divulgee: String = "divulgee",
      commandId: String = UUID.randomUUID().toString,
  ): DbDto.EventDivulgence = {
    DbDto.EventDivulgence(
      event_offset = Some(offset.toHexString),
      command_id = Some(commandId),
      workflow_id = Some("workflow_id"),
      application_id = Some("application_id"),
      submitters = Some(Set(submitter)),
      contract_id = contractId,
      template_id = Some(someTemplateId.toString),
      tree_event_witnesses = Set(divulgee),
      create_argument = Some(someSerializedDamlLfValue),
      create_argument_compression = None,
      event_sequential_id = eventSequentialId,
    )
  }

  def dtoCompletion(
      offset: Offset,
      submitter: String = "submitter",
      commandId: String = UUID.randomUUID().toString,
  ): DbDto.CommandCompletion = {
    val transactionId = transactionIdFromOffset(offset)
    DbDto.CommandCompletion(
      completion_offset = offset.toHexString,
      record_time = someTime,
      application_id = "application_id",
      submitters = Set(submitter),
      command_id = commandId,
      transaction_id = Some(transactionId),
      status_code = None,
      status_message = None,
    )
  }

  // An arbitrary complex transaction
  def dtoTransaction(
      offset: Offset,
      eventSequentialId: Long,
  ): List[DbDto] = {
    val transactionId = transactionIdFromOffset(offset)
    val commandId = UUID.randomUUID().toString
    List(
      DbDto.EventExercise(
        consuming = false,
        event_offset = Some(offset.toHexString),
        transaction_id = Some(transactionId),
        ledger_effective_time = Some(someTime),
        command_id = Some(commandId),
        workflow_id = Some("workflow_id"),
        application_id = Some("application_id"),
        submitters = Some(Set("signatory")),
        node_index = Some(0),
        event_id = Some(EventId(transactionId, NodeId(0)).toLedgerString),
        contract_id = "contract_id1",
        template_id = Some(someTemplateId.toString),
        flat_event_witnesses = Set.empty,
        tree_event_witnesses = Set("signatory"),
        create_key_value = None,
        exercise_choice = Some("exercise_choice"),
        exercise_argument = Some(someSerializedDamlLfValue),
        exercise_result = Some(someSerializedDamlLfValue),
        exercise_actors = Some(Set("signatory")),
        exercise_child_event_ids = Some(
          Set(
            EventId(transactionId, NodeId(1)).toLedgerString,
            EventId(transactionId, NodeId(2)).toLedgerString,
          )
        ),
        create_key_value_compression = None,
        exercise_argument_compression = None,
        exercise_result_compression = None,
        event_sequential_id = eventSequentialId,
      ),
      DbDto.EventExercise(
        consuming = true,
        event_offset = Some(offset.toHexString),
        transaction_id = Some(transactionId),
        ledger_effective_time = Some(someTime),
        command_id = Some(commandId),
        workflow_id = Some("workflow_id"),
        application_id = Some("application_id"),
        submitters = Some(Set("signatory")),
        node_index = Some(1),
        event_id = Some(EventId(transactionId, NodeId(1)).toLedgerString),
        contract_id = "contract_id2",
        template_id = Some(someTemplateId.toString),
        flat_event_witnesses = Set.empty,
        tree_event_witnesses = Set("signatory"),
        create_key_value = None,
        exercise_choice = Some("exercise_choice"),
        exercise_argument = Some(someSerializedDamlLfValue),
        exercise_result = Some(someSerializedDamlLfValue),
        exercise_actors = Some(Set("signatory")),
        exercise_child_event_ids = Some(Set.empty),
        create_key_value_compression = None,
        exercise_argument_compression = None,
        exercise_result_compression = None,
        event_sequential_id = eventSequentialId + 1,
      ),
      DbDto.EventCreate(
        event_offset = Some(offset.toHexString),
        transaction_id = Some(transactionId),
        ledger_effective_time = Some(someTime),
        command_id = Some(commandId),
        workflow_id = Some("workflow_id"),
        application_id = Some("application_id"),
        submitters = None,
        node_index = Some(2),
        event_id = Some(EventId(transactionId, NodeId(2)).toLedgerString),
        contract_id = "contract_id3",
        template_id = Some(someTemplateId.toString),
        flat_event_witnesses = Set("signatory", "observer"),
        tree_event_witnesses = Set("signatory", "observer"),
        create_argument = Some(someSerializedDamlLfValue),
        create_signatories = Some(Set("signatory")),
        create_observers = Some(Set("observer")),
        create_agreement_text = None,
        create_key_value = None,
        create_key_hash = None,
        create_argument_compression = None,
        create_key_value_compression = None,
        event_sequential_id = eventSequentialId + 2,
      ),
      DbDto.EventDivulgence(
        event_offset = Some(offset.toHexString),
        command_id = Some(commandId),
        workflow_id = Some("workflow_id"),
        application_id = Some("application_id"),
        submitters = Some(Set("signatory")),
        contract_id = "contract_id4",
        template_id = Some(someTemplateId.toString),
        tree_event_witnesses = Set("divulgee"),
        create_argument = Some(someSerializedDamlLfValue),
        create_argument_compression = Some(0),
        event_sequential_id = eventSequentialId + 3,
      ),
      DbDto.CommandCompletion(
        completion_offset = offset.toHexString,
        record_time = someTime,
        application_id = "application_id",
        submitters = Set("signatory"),
        command_id = commandId,
        transaction_id = Some(transactionId),
        status_code = None,
        status_message = None,
      ),
    )
  }

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
