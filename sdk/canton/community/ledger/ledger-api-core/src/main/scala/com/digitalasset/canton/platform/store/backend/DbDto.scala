// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.scalautil.NeverEqualsOverride
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.value.Value.ContractId

sealed trait DbDto
    extends NeverEqualsOverride
    with Product
    with Serializable // to aid type inference for case class implementors

object DbDto {

  final case class EventActivate(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitters: Option[Set[String]],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      external_transaction_hash: Option[Array[Byte]],

      // event related columns
      event_type: Int,
      event_sequential_id: Long,
      node_id: Int,
      additional_witnesses: Option[Set[String]],
      source_synchronizer_id: Option[SynchronizerId],
      reassignment_counter: Option[Long],
      reassignment_id: Option[Array[Byte]],
      representative_package_id: String,

      // contract related columns
      notPersistedContractId: ContractId, // just needed for processing
      internal_contract_id: Long,
      create_key_hash: Option[String],
  ) extends DbDto
  final case class IdFilterActivateStakeholder(idFilter: IdFilter) extends IdFilterDbDto
  final case class IdFilterActivateWitness(idFilter: IdFilter) extends IdFilterDbDto

  final case class EventDeactivate(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitters: Option[Set[String]],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      external_transaction_hash: Option[Array[Byte]],

      // event related columns
      event_type: Int,
      event_sequential_id: Long,
      node_id: Int,
      deactivated_event_sequential_id: Option[Long],
      additional_witnesses: Option[Set[String]],
      exercise_choice: Option[String],
      exercise_choice_interface_id: Option[String],
      exercise_argument: Option[Array[Byte]],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Option[Set[String]],
      exercise_last_descendant_node_id: Option[Int],
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],
      reassignment_id: Option[Array[Byte]],
      assignment_exclusivity: Option[Long],
      target_synchronizer_id: Option[SynchronizerId],
      reassignment_counter: Option[Long],

      // contract related columns
      contract_id: ContractId,
      internal_contract_id: Option[Long],
      template_id: String,
      package_id: String,
      stakeholders: Set[String],
      ledger_effective_time: Option[Long],
  ) extends DbDto
  final case class IdFilterDeactivateStakeholder(idFilter: IdFilter) extends IdFilterDbDto
  final case class IdFilterDeactivateWitness(idFilter: IdFilter) extends IdFilterDbDto

  final case class EventVariousWitnessed(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitters: Option[Set[String]],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      external_transaction_hash: Option[Array[Byte]],

      // event related columns
      event_type: Int,
      event_sequential_id: Long,
      node_id: Int,
      additional_witnesses: Set[String],
      consuming: Option[Boolean],
      exercise_choice: Option[String],
      exercise_choice_interface_id: Option[String],
      exercise_argument: Option[Array[Byte]],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Option[Set[String]],
      exercise_last_descendant_node_id: Option[Int],
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],
      representative_package_id: Option[String],

      // contract related columns
      contract_id: Option[ContractId],
      internal_contract_id: Option[Long],
      template_id: Option[String],
      package_id: Option[String],
      ledger_effective_time: Option[Long],
  ) extends DbDto

  final case class IdFilterVariousWitness(idFilter: IdFilter) extends IdFilterDbDto

  sealed trait IdFilterDbDto extends DbDto {
    def idFilter: IdFilter
    def withEventSequentialId(id: Long): IdFilterDbDto = {
      def idFilterWithEventSequentialId(idFilter: IdFilter): IdFilter =
        idFilter.copy(event_sequential_id = id)
      this match {
        case IdFilterActivateStakeholder(idFilter) =>
          IdFilterActivateStakeholder(idFilterWithEventSequentialId(idFilter))
        case IdFilterActivateWitness(idFilter) =>
          IdFilterActivateWitness(idFilterWithEventSequentialId(idFilter))
        case IdFilterDeactivateStakeholder(idFilter) =>
          IdFilterDeactivateStakeholder(idFilterWithEventSequentialId(idFilter))
        case IdFilterDeactivateWitness(idFilter) =>
          IdFilterDeactivateWitness(idFilterWithEventSequentialId(idFilter))
        case IdFilterVariousWitness(idFilter) =>
          IdFilterVariousWitness(idFilterWithEventSequentialId(idFilter))
      }
    }
  }
  final case class IdFilter(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) {
    def activateStakeholder: IdFilterActivateStakeholder = IdFilterActivateStakeholder(this)
    def activateWitness: IdFilterActivateWitness = IdFilterActivateWitness(this)
    def deactivateStakeholder: IdFilterDeactivateStakeholder = IdFilterDeactivateStakeholder(this)
    def deactivateWitness: IdFilterDeactivateWitness = IdFilterDeactivateWitness(this)
    def variousWitness: IdFilterVariousWitness = IdFilterVariousWitness(this)
  }

  // TODO(#28008) remove
  final case class EventCreate(
      event_offset: Long,
      update_id: Array[Byte],
      ledger_effective_time: Long,
      command_id: Option[String],
      workflow_id: Option[String],
      user_id: Option[String],
      submitters: Option[Set[String]],
      node_id: Int,
      contract_id: ContractId,
      template_id: String,
      package_id: String,
      representative_package_id: String,
      flat_event_witnesses: Set[String],
      tree_event_witnesses: Set[String],
      create_argument: Array[Byte],
      create_signatories: Set[String],
      create_observers: Set[String],
      create_key_value: Option[Array[Byte]],
      create_key_maintainers: Option[Set[String]],
      create_key_hash: Option[String],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      event_sequential_id: Long,
      authentication_data: Array[Byte],
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      record_time: Long,
      external_transaction_hash: Option[Array[Byte]],
      internal_contract_id: Long,
  ) extends DbDto

  // TODO(#28008) remove
  final case class EventExercise(
      consuming: Boolean,
      event_offset: Long,
      update_id: Array[Byte],
      ledger_effective_time: Long,
      command_id: Option[String],
      workflow_id: Option[String],
      user_id: Option[String],
      submitters: Option[Set[String]],
      node_id: Int,
      contract_id: ContractId,
      template_id: String,
      package_id: String,
      // only for consuming, for non-consuming exercise this field is omitted
      flat_event_witnesses: Set[String],
      tree_event_witnesses: Set[String],
      exercise_choice: String,
      exercise_choice_interface_id: Option[String],
      exercise_argument: Array[Byte],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Set[String],
      exercise_last_descendant_node_id: Int,
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],
      event_sequential_id: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      record_time: Long,
      external_transaction_hash: Option[Array[Byte]],
      deactivated_event_sequential_id: Option[Long],
  ) extends DbDto

  // TODO(#28008) remove
  final case class EventAssign(
      event_offset: Long,
      update_id: Array[Byte],
      command_id: Option[String],
      workflow_id: Option[String],
      submitter: Option[String],
      node_id: Int,
      contract_id: ContractId,
      template_id: String,
      package_id: String,
      flat_event_witnesses: Set[String],
      create_argument: Array[Byte],
      create_signatories: Set[String],
      create_observers: Set[String],
      create_key_value: Option[Array[Byte]],
      create_key_maintainers: Option[Set[String]],
      create_key_hash: Option[String],
      create_argument_compression: Option[Int],
      create_key_value_compression: Option[Int],
      event_sequential_id: Long,
      ledger_effective_time: Long,
      authentication_data: Array[Byte],
      source_synchronizer_id: SynchronizerId,
      target_synchronizer_id: SynchronizerId,
      reassignment_id: Array[Byte],
      reassignment_counter: Long,
      trace_context: Array[Byte],
      record_time: Long,
      internal_contract_id: Long,
  ) extends DbDto

  // TODO(#28008) remove
  final case class EventUnassign(
      event_offset: Long,
      update_id: Array[Byte],
      command_id: Option[String],
      workflow_id: Option[String],
      submitter: Option[String],
      node_id: Int,
      contract_id: ContractId,
      template_id: String,
      package_id: String,
      flat_event_witnesses: Set[String],
      event_sequential_id: Long,
      source_synchronizer_id: SynchronizerId,
      target_synchronizer_id: SynchronizerId,
      reassignment_id: Array[Byte],
      reassignment_counter: Long,
      assignment_exclusivity: Option[Long],
      trace_context: Array[Byte],
      record_time: Long,
      deactivated_event_sequential_id: Option[Long],
  ) extends DbDto

  final case class EventPartyToParticipant(
      event_sequential_id: Long,
      event_offset: Long,
      update_id: Array[Byte],
      party_id: String,
      participant_id: String,
      participant_permission: Int,
      participant_authorization_event: Int,
      synchronizer_id: SynchronizerId,
      record_time: Long,
      trace_context: Array[Byte],
  ) extends DbDto

  final case class PartyEntry(
      ledger_offset: Long,
      recorded_at: Long,
      submission_id: Option[String],
      party: Option[String],
      typ: String,
      rejection_reason: Option[String],
      is_local: Option[Boolean],
  ) extends DbDto

  final case class CommandCompletion(
      completion_offset: Long,
      record_time: Long,
      publication_time: Long,
      user_id: String,
      submitters: Set[String],
      command_id: String,
      update_id: Option[Array[Byte]],
      rejection_status_code: Option[Int],
      rejection_status_message: Option[String],
      rejection_status_details: Option[Array[Byte]],
      submission_id: Option[String],
      deduplication_offset: Option[Long],
      deduplication_duration_seconds: Option[Long],
      deduplication_duration_nanos: Option[Int],
      synchronizer_id: SynchronizerId,
      message_uuid: Option[String],
      is_transaction: Boolean,
      trace_context: Array[Byte],
  ) extends DbDto

  final case class StringInterningDto(
      internalId: Int,
      externalString: String,
  ) extends DbDto

  object StringInterningDto {
    def from(entry: (Int, String)): StringInterningDto =
      StringInterningDto(entry._1, entry._2)
  }

  // TODO(#28008) remove
  final case class IdFilterCreateStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) extends DbDto

  // TODO(#28008) remove
  final case class IdFilterCreateNonStakeholderInformee(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) extends DbDto

  // TODO(#28008) remove
  final case class IdFilterConsumingStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) extends DbDto

  // TODO(#28008) remove
  final case class IdFilterConsumingNonStakeholderInformee(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) extends DbDto

  // TODO(#28008) remove
  final case class IdFilterNonConsumingInformee(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) extends DbDto

  // TODO(#28008) remove
  final case class IdFilterUnassignStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) extends DbDto

  // TODO(#28008) remove
  final case class IdFilterAssignStakeholder(
      event_sequential_id: Long,
      template_id: String,
      party_id: String,
      first_per_sequential_id: Boolean,
  ) extends DbDto

  final case class TransactionMeta(
      update_id: Array[Byte],
      event_offset: Long,
      publication_time: Long,
      record_time: Long,
      synchronizer_id: SynchronizerId,
      event_sequential_id_first: Long,
      event_sequential_id_last: Long,
  ) extends DbDto

  final case class SequencerIndexMoved(synchronizerId: SynchronizerId) extends DbDto

  def createDbDtos(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitters: Option[Set[String]],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      external_transaction_hash: Option[Array[Byte]],

      // event related columns
      event_sequential_id: Long,
      node_id: Int,
      additional_witnesses: Set[String],
      representative_package_id: String,

      // contract related columns
      notPersistedContractId: ContractId,
      internal_contract_id: Long,
      create_key_hash: Option[String],
  )(stakeholders: Set[String], template_id: String): Iterator[DbDto] =
    Iterator(
      EventActivate(
        // update related columns
        event_offset = event_offset,
        update_id = update_id,
        workflow_id = workflow_id,
        command_id = command_id,
        submitters = submitters,
        record_time = record_time,
        synchronizer_id = synchronizer_id,
        trace_context = trace_context,
        external_transaction_hash = external_transaction_hash,
        // event related columns
        event_type = PersistentEventType.Create.asInt,
        event_sequential_id = event_sequential_id,
        node_id = node_id,
        additional_witnesses = Some(additional_witnesses),
        source_synchronizer_id = None,
        reassignment_counter = None,
        reassignment_id = None,
        representative_package_id = representative_package_id,
        // contract related columns
        notPersistedContractId = notPersistedContractId,
        internal_contract_id = internal_contract_id,
        create_key_hash = create_key_hash,
      )
    ) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = stakeholders.iterator,
    )(_.activateStakeholder) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = additional_witnesses.iterator,
    )(_.activateWitness)

  def assignDbDtos(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitter: Option[String],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],

      // event related columns
      event_sequential_id: Long,
      node_id: Int,
      source_synchronizer_id: SynchronizerId,
      reassignment_counter: Long,
      reassignment_id: Array[Byte],
      representative_package_id: String,

      // contract related columns
      notPersistedContractId: ContractId,
      internal_contract_id: Long,
  )(stakeholders: Set[String], template_id: String): Iterator[DbDto] =
    Iterator(
      EventActivate(
        // update related columns
        event_offset = event_offset,
        update_id = update_id,
        workflow_id = workflow_id,
        command_id = command_id,
        submitters = submitter.map(Set(_)),
        record_time = record_time,
        synchronizer_id = synchronizer_id,
        trace_context = trace_context,
        external_transaction_hash = None,
        event_type = PersistentEventType.Assign.asInt,
        // event related columns
        event_sequential_id = event_sequential_id,
        node_id = node_id,
        additional_witnesses = None,
        source_synchronizer_id = Some(source_synchronizer_id),
        reassignment_counter = Some(reassignment_counter),
        reassignment_id = Some(reassignment_id),
        representative_package_id = representative_package_id,
        // contract related columns
        notPersistedContractId = notPersistedContractId,
        internal_contract_id = internal_contract_id,
        create_key_hash = None,
      )
    ) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = stakeholders.iterator,
    )(_.activateStakeholder)

  def consumingExerciseDbDtos(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitters: Option[Set[String]],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      external_transaction_hash: Option[Array[Byte]],

      // event related columns
      event_sequential_id: Long,
      node_id: Int,
      deactivated_event_sequential_id: Option[Long],
      additional_witnesses: Set[String],
      exercise_choice: String,
      exercise_choice_interface_id: Option[String],
      exercise_argument: Array[Byte],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Set[String],
      exercise_last_descendant_node_id: Int,
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],

      // contract related columns
      contract_id: ContractId,
      internal_contract_id: Option[Long],
      template_id: String,
      package_id: String,
      stakeholders: Set[String],
      ledger_effective_time: Long,
  ): Iterator[DbDto] =
    Iterator(
      EventDeactivate(
        // update related columns
        event_offset = event_offset,
        update_id = update_id,
        workflow_id = workflow_id,
        command_id = command_id,
        submitters = submitters,
        record_time = record_time,
        synchronizer_id = synchronizer_id,
        trace_context = trace_context,
        external_transaction_hash = external_transaction_hash,
        // event related columns
        event_type = PersistentEventType.ConsumingExercise.asInt,
        event_sequential_id = event_sequential_id,
        node_id = node_id,
        deactivated_event_sequential_id = deactivated_event_sequential_id,
        additional_witnesses = Some(additional_witnesses),
        exercise_choice = Some(exercise_choice),
        exercise_choice_interface_id = exercise_choice_interface_id,
        exercise_argument = Some(exercise_argument),
        exercise_result = exercise_result,
        exercise_actors = Some(exercise_actors),
        exercise_last_descendant_node_id = Some(exercise_last_descendant_node_id),
        exercise_argument_compression = exercise_argument_compression,
        exercise_result_compression = exercise_result_compression,
        reassignment_id = None,
        assignment_exclusivity = None,
        target_synchronizer_id = None,
        reassignment_counter = None,
        // contract related columns
        contract_id = contract_id,
        internal_contract_id = internal_contract_id,
        template_id = template_id,
        package_id = package_id,
        stakeholders = stakeholders,
        ledger_effective_time = Some(ledger_effective_time),
      )
    ) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = stakeholders.iterator,
    )(_.deactivateStakeholder) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = additional_witnesses.iterator,
    )(_.deactivateWitness)

  def unassignDbDtos(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitter: Option[String],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],

      // event related columns
      event_sequential_id: Long,
      node_id: Int,
      deactivated_event_sequential_id: Option[Long],
      reassignment_id: Array[Byte],
      assignment_exclusivity: Option[Long],
      target_synchronizer_id: SynchronizerId,
      reassignment_counter: Long,

      // contract related columns
      contract_id: ContractId,
      internal_contract_id: Option[Long],
      template_id: String,
      package_id: String,
      stakeholders: Set[String],
  ): Iterator[DbDto] =
    Iterator(
      EventDeactivate(
        // update related columns
        event_offset = event_offset,
        update_id = update_id,
        workflow_id = workflow_id,
        command_id = command_id,
        submitters = submitter.map(Set(_)),
        record_time = record_time,
        synchronizer_id = synchronizer_id,
        trace_context = trace_context,
        external_transaction_hash = None,
        // event related columns
        event_type = PersistentEventType.Unassign.asInt,
        event_sequential_id = event_sequential_id,
        node_id = node_id,
        deactivated_event_sequential_id = deactivated_event_sequential_id,
        additional_witnesses = None,
        exercise_choice = None,
        exercise_choice_interface_id = None,
        exercise_argument = None,
        exercise_result = None,
        exercise_actors = None,
        exercise_last_descendant_node_id = None,
        exercise_argument_compression = None,
        exercise_result_compression = None,
        reassignment_id = Some(reassignment_id),
        assignment_exclusivity = assignment_exclusivity,
        target_synchronizer_id = Some(target_synchronizer_id),
        reassignment_counter = Some(reassignment_counter),
        // contract related columns
        contract_id = contract_id,
        internal_contract_id = internal_contract_id,
        template_id = template_id,
        package_id = package_id,
        stakeholders = stakeholders,
        ledger_effective_time = None,
      )
    ) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = stakeholders.iterator,
    )(_.deactivateStakeholder)

  def witnessedCreateDbDtos(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitters: Option[Set[String]],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      external_transaction_hash: Option[Array[Byte]],

      // event related columns
      event_sequential_id: Long,
      node_id: Int,
      additional_witnesses: Set[String],
      representative_package_id: String,

      // contract related columns
      internal_contract_id: Long,
  )(template_id: String): Iterator[DbDto] =
    Iterator(
      EventVariousWitnessed(
        // update related columns
        event_offset = event_offset,
        update_id = update_id,
        workflow_id = workflow_id,
        command_id = command_id,
        submitters = submitters,
        record_time = record_time,
        synchronizer_id = synchronizer_id,
        trace_context = trace_context,
        external_transaction_hash = external_transaction_hash,

        // event related columns
        event_type = PersistentEventType.WitnessedCreate.asInt,
        event_sequential_id = event_sequential_id,
        node_id = node_id,
        additional_witnesses = additional_witnesses,
        consuming = None,
        exercise_choice = None,
        exercise_choice_interface_id = None,
        exercise_argument = None,
        exercise_result = None,
        exercise_actors = None,
        exercise_last_descendant_node_id = None,
        exercise_argument_compression = None,
        exercise_result_compression = None,
        representative_package_id = Some(representative_package_id),

        // contract related columns
        contract_id = None,
        internal_contract_id = Some(internal_contract_id),
        template_id = None,
        package_id = None,
        ledger_effective_time = None,
      )
    ) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = additional_witnesses.iterator,
    )(_.variousWitness)

  def witnessedExercisedDbDtos(
      // update related columns
      event_offset: Long,
      update_id: Array[Byte],
      workflow_id: Option[String],
      command_id: Option[String],
      submitters: Option[Set[String]],
      record_time: Long,
      synchronizer_id: SynchronizerId,
      trace_context: Array[Byte],
      external_transaction_hash: Option[Array[Byte]],

      // event related columns
      event_sequential_id: Long,
      node_id: Int,
      additional_witnesses: Set[String],
      consuming: Boolean,
      exercise_choice: String,
      exercise_choice_interface_id: Option[String],
      exercise_argument: Array[Byte],
      exercise_result: Option[Array[Byte]],
      exercise_actors: Set[String],
      exercise_last_descendant_node_id: Int,
      exercise_argument_compression: Option[Int],
      exercise_result_compression: Option[Int],

      // contract related columns
      contract_id: ContractId,
      internal_contract_id: Option[Long],
      template_id: String,
      package_id: String,
      ledger_effective_time: Long,
  ): Iterator[DbDto] =
    Iterator(
      EventVariousWitnessed(
        // update related columns
        event_offset = event_offset,
        update_id = update_id,
        workflow_id = workflow_id,
        command_id = command_id,
        submitters = submitters,
        record_time = record_time,
        synchronizer_id = synchronizer_id,
        trace_context = trace_context,
        external_transaction_hash = external_transaction_hash,

        // event related columns
        event_type =
          if (consuming) PersistentEventType.WitnessedConsumingExercise.asInt
          else PersistentEventType.NonConsumingExercise.asInt,
        event_sequential_id = event_sequential_id,
        node_id = node_id,
        additional_witnesses = additional_witnesses,
        consuming = Some(consuming),
        exercise_choice = Some(exercise_choice),
        exercise_choice_interface_id = exercise_choice_interface_id,
        exercise_argument = Some(exercise_argument),
        exercise_result = exercise_result,
        exercise_actors = Some(exercise_actors),
        exercise_last_descendant_node_id = Some(exercise_last_descendant_node_id),
        exercise_argument_compression = exercise_argument_compression,
        exercise_result_compression = exercise_result_compression,
        representative_package_id = None,

        // contract related columns
        contract_id = Some(contract_id),
        internal_contract_id = internal_contract_id,
        template_id = Some(template_id),
        package_id = Some(package_id),
        ledger_effective_time = Some(ledger_effective_time),
      )
    ) ++ idFilters(
      event_sequential_id = event_sequential_id,
      template_id = template_id,
      party_ids = additional_witnesses.iterator,
    )(_.variousWitness)

  def idFilters(
      party_ids: Iterator[String],
      template_id: String,
      event_sequential_id: Long,
  )(toIdFilterDbDto: IdFilter => IdFilterDbDto): Iterator[IdFilterDbDto] =
    party_ids
      .take(1)
      .map(party_id =>
        IdFilter(
          event_sequential_id = event_sequential_id,
          template_id = template_id,
          party_id = party_id,
          first_per_sequential_id = true,
        )
      )
      .++(
        party_ids.map(party_id =>
          IdFilter(
            event_sequential_id = event_sequential_id,
            template_id = template_id,
            party_id = party_id,
            first_per_sequential_id = false,
          )
        )
      )
      .map(toIdFilterDbDto)
}
