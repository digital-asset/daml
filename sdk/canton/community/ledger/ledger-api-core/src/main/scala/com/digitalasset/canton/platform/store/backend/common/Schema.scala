// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.platform.store.backend.Conversions.IntArrayDBSerialization.encodeToByteArray
import com.digitalasset.canton.platform.store.backend.DbDto
import com.digitalasset.canton.platform.store.interning.StringInterning

import java.sql.Connection
import scala.reflect.ClassTag

private[backend] trait Schema[FROM] {
  def prepareData(in: Vector[FROM], stringInterning: StringInterning): Array[Array[Array[_]]]
  def executeUpdate(data: Array[Array[Array[_]]], connection: Connection): Unit
}

private[backend] object AppendOnlySchema {

  type Batch = Array[Array[Array[_]]]

  private[backend] trait FieldStrategy {
    def string[FROM](extractor: StringInterning => FROM => String): Field[FROM, String, _] =
      StringField(extractor)

    def stringOptional[FROM](
        extractor: StringInterning => FROM => Option[String]
    ): Field[FROM, Option[String], _] =
      StringOptional(extractor)

    def stringArray[FROM](
        extractor: StringInterning => FROM => Iterable[String]
    ): Field[FROM, Iterable[String], _] =
      StringArray(extractor)

    def bytea[FROM](
        extractor: StringInterning => FROM => Array[Byte]
    ): Field[FROM, Array[Byte], _] =
      Bytea(extractor)

    def byteaOptional[FROM](
        extractor: StringInterning => FROM => Option[Array[Byte]]
    ): Field[FROM, Option[Array[Byte]], _] =
      ByteaOptional(extractor)

    def parties[FROM](
        extractor: StringInterning => FROM => Set[String]
    ): Field[FROM, Array[Byte], _] =
      bytea(stringInterning =>
        from =>
          encodeToByteArray(
            extractor(stringInterning)(from)
              .map(stringInterning.party.unsafe.internalize)
          )
      )

    def partiesOptional[FROM](
        extractor: StringInterning => FROM => Option[Set[String]]
    ): Field[FROM, Option[Array[Byte]], _] =
      byteaOptional(stringInterning =>
        from =>
          extractor(stringInterning)(from)
            .map(_.map(stringInterning.party.unsafe.internalize))
            .map(encodeToByteArray)
      )

    def bigint[FROM](extractor: StringInterning => FROM => Long): Field[FROM, Long, _] =
      Bigint(extractor)

    def bigintOptional[FROM](
        extractor: StringInterning => FROM => Option[Long]
    ): Field[FROM, Option[Long], _] =
      BigintOptional(extractor)

    def smallintOptional[FROM](
        extractor: StringInterning => FROM => Option[Int]
    ): Field[FROM, Option[Int], _] =
      SmallintOptional(extractor)

    def int[FROM](extractor: StringInterning => FROM => Int): Field[FROM, Int, _] =
      Integer(extractor)

    def intOptional[FROM](
        extractor: StringInterning => FROM => Option[Int]
    ): Field[FROM, Option[Int], _] =
      IntOptional(extractor)

    def booleanOptional[FROM](
        extractor: StringInterning => FROM => Option[Boolean]
    ): Field[FROM, Option[Boolean], _] =
      BooleanOptional(extractor)

    def boolean[FROM](
        extractor: StringInterning => FROM => Boolean
    ): Field[FROM, Boolean, _] =
      BooleanMandatory(extractor)

    def insert[FROM](tableName: String)(fields: (String, Field[FROM, _, _])*): Table[FROM]
    def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int, ordering: Ordering[FROM])(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM]
  }

  def apply(fieldStrategy: FieldStrategy): Schema[DbDto] = {
    val eventsCreate: Table[DbDto.EventCreate] =
      fieldStrategy.insert("lapi_events_create")(
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.string(_ => _.update_id),
        "ledger_effective_time" -> fieldStrategy.bigint(_ => _.ledger_effective_time),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "submitters" -> fieldStrategy.partiesOptional(_ => _.submitters),
        "node_id" -> fieldStrategy.int(_ => _.node_id),
        "contract_id" -> fieldStrategy.bytea(_ => _.contract_id.toBytes.toByteArray),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.templateId.unsafe.internalize(dbDto.template_id)
        ),
        "package_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.packageId.unsafe.internalize(dbDto.package_id)
        ),
        "representative_package_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.packageId.unsafe.internalize(dbDto.representative_package_id)
        ),
        "flat_event_witnesses" -> fieldStrategy.parties(_ => _.flat_event_witnesses),
        "tree_event_witnesses" -> fieldStrategy.parties(_ => _.tree_event_witnesses),
        "create_argument" -> fieldStrategy.bytea(_ => _.create_argument),
        "create_signatories" -> fieldStrategy.parties(_ => _.create_signatories),
        "create_observers" -> fieldStrategy.parties(_ => _.create_observers),
        "create_key_value" -> fieldStrategy.byteaOptional(_ => _.create_key_value),
        "create_key_maintainers" -> fieldStrategy.partiesOptional(_ => _.create_key_maintainers),
        "create_key_hash" -> fieldStrategy.stringOptional(_ => _.create_key_hash),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "create_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_argument_compression
        ),
        "create_key_value_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_key_value_compression
        ),
        "authentication_data" -> fieldStrategy.bytea(_ => _.authentication_data),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "external_transaction_hash" -> fieldStrategy.byteaOptional(_ => _.external_transaction_hash),
      )

    val exerciseFields: Vector[(String, Field[DbDto.EventExercise, _, _])] =
      Vector[(String, Field[DbDto.EventExercise, _, _])](
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "contract_id" -> fieldStrategy.bytea(_ => _.contract_id.toBytes.toByteArray),
        "update_id" -> fieldStrategy.string(_ => _.update_id),
        "ledger_effective_time" -> fieldStrategy.bigint(_ => _.ledger_effective_time),
        "node_id" -> fieldStrategy.int(_ => _.node_id),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "submitters" -> fieldStrategy.partiesOptional(_ => _.submitters),
        "exercise_choice" -> fieldStrategy.string(_ => _.exercise_choice),
        "exercise_argument" -> fieldStrategy.bytea(_ => _.exercise_argument),
        "exercise_result" -> fieldStrategy.byteaOptional(_ => _.exercise_result),
        "exercise_actors" -> fieldStrategy.parties(_ => _.exercise_actors),
        "exercise_last_descendant_node_id" -> fieldStrategy.int(_ =>
          _.exercise_last_descendant_node_id
        ),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.templateId.unsafe.internalize(dbDto.template_id)
        ),
        "package_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.packageId.unsafe.internalize(dbDto.package_id)
        ),
        "tree_event_witnesses" -> fieldStrategy.parties(_ => _.tree_event_witnesses),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "exercise_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_argument_compression
        ),
        "exercise_result_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_result_compression
        ),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "external_transaction_hash" -> fieldStrategy.byteaOptional(_ => _.external_transaction_hash),
      )

    val consumingExerciseFields: Vector[(String, Field[DbDto.EventExercise, _, _])] =
      exerciseFields ++ Vector[(String, Field[DbDto.EventExercise, _, _])](
        "flat_event_witnesses" -> fieldStrategy.parties(_ => _.flat_event_witnesses),
        "deactivated_event_sequential_id" -> fieldStrategy.bigintOptional(_ =>
          _.deactivated_event_sequential_id
        ),
      )

    val eventsConsumingExercise: Table[DbDto.EventExercise] =
      fieldStrategy.insert("lapi_events_consuming_exercise")(consumingExerciseFields*)

    val eventsNonConsumingExercise: Table[DbDto.EventExercise] =
      fieldStrategy.insert("lapi_events_non_consuming_exercise")(exerciseFields*)

    val eventsUnassign: Table[DbDto.EventUnassign] =
      fieldStrategy.insert("lapi_events_unassign")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.string(_ => _.update_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "submitter" -> fieldStrategy.intOptional(stringInterning =>
          _.submitter.map(stringInterning.party.unsafe.internalize)
        ),
        "node_id" -> fieldStrategy.int(_ => _.node_id),
        "contract_id" -> fieldStrategy.bytea(_ => u => u.contract_id.toBytes.toByteArray),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.templateId.unsafe.internalize(dbDto.template_id)
        ),
        "package_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.packageId.unsafe.internalize(dbDto.package_id)
        ),
        "flat_event_witnesses" -> fieldStrategy.parties(_ => _.flat_event_witnesses),
        "source_synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.source_synchronizer_id)
        ),
        "target_synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.target_synchronizer_id)
        ),
        "reassignment_id" -> fieldStrategy.string(_ => _.reassignment_id),
        "reassignment_counter" -> fieldStrategy.bigint(_ => _.reassignment_counter),
        "assignment_exclusivity" -> fieldStrategy.bigintOptional(_ => _.assignment_exclusivity),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "deactivated_event_sequential_id" -> fieldStrategy.bigintOptional(_ =>
          _.deactivated_event_sequential_id
        ),
      )

    val eventsAssign: Table[DbDto.EventAssign] =
      fieldStrategy.insert("lapi_events_assign")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.string(_ => _.update_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "submitter" -> fieldStrategy.intOptional(stringInterning =>
          _.submitter.map(stringInterning.party.unsafe.internalize)
        ),
        "node_id" -> fieldStrategy.int(_ => _.node_id),
        "contract_id" -> fieldStrategy.bytea(_ => _.contract_id.toBytes.toByteArray),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.templateId.unsafe.internalize(dbDto.template_id)
        ),
        "package_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.packageId.unsafe.internalize(dbDto.package_id)
        ),
        "flat_event_witnesses" -> fieldStrategy.parties(_ => _.flat_event_witnesses),
        "source_synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.source_synchronizer_id)
        ),
        "target_synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.target_synchronizer_id)
        ),
        "reassignment_id" -> fieldStrategy.string(_ => _.reassignment_id),
        "reassignment_counter" -> fieldStrategy.bigint(_ => _.reassignment_counter),
        "create_argument" -> fieldStrategy.bytea(_ => _.create_argument),
        "create_signatories" -> fieldStrategy.parties(_ => _.create_signatories),
        "create_observers" -> fieldStrategy.parties(_ => _.create_observers),
        "create_key_value" -> fieldStrategy.byteaOptional(_ => _.create_key_value),
        "create_key_maintainers" -> fieldStrategy.partiesOptional(_ => _.create_key_maintainers),
        "create_key_hash" -> fieldStrategy.stringOptional(_ => _.create_key_hash),
        "create_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_argument_compression
        ),
        "create_key_value_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_key_value_compression
        ),
        "ledger_effective_time" -> fieldStrategy.bigint(_ => _.ledger_effective_time),
        "authentication_data" -> fieldStrategy.bytea(_ => _.authentication_data),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
      )

    val partyEntries: Table[DbDto.PartyEntry] =
      fieldStrategy.insert("lapi_party_entries")(
        "ledger_offset" -> fieldStrategy.bigint(_ => _.ledger_offset),
        "recorded_at" -> fieldStrategy.bigint(_ => _.recorded_at),
        "submission_id" -> fieldStrategy.stringOptional(_ => _.submission_id),
        "party" -> fieldStrategy.stringOptional(_ => _.party),
        "typ" -> fieldStrategy.string(_ => _.typ),
        "rejection_reason" -> fieldStrategy.stringOptional(_ => _.rejection_reason),
        "is_local" -> fieldStrategy.booleanOptional(_ => _.is_local),
        "party_id" -> fieldStrategy.intOptional(stringInterning =>
          _.party.map(stringInterning.party.unsafe.internalize)
        ),
      )

    val partyToParticipant: Table[DbDto.EventPartyToParticipant] =
      fieldStrategy.insert("lapi_events_party_to_participant")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.string(_ => _.update_id),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "participant_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.participantId.unsafe.internalize(dto.participant_id)
        ),
        "participant_permission" -> fieldStrategy.int(_ => _.participant_permission),
        "participant_authorization_event" -> fieldStrategy.int(_ =>
          _.participant_authorization_event
        ),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
      )

    val commandCompletions: Table[DbDto.CommandCompletion] =
      fieldStrategy.insert("lapi_command_completions")(
        "completion_offset" -> fieldStrategy.bigint(_ => _.completion_offset),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "publication_time" -> fieldStrategy.bigint(_ => _.publication_time),
        "user_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.userId.unsafe.internalize(dbDto.user_id)
        ),
        "submitters" -> fieldStrategy.parties(_ => _.submitters),
        "command_id" -> fieldStrategy.string(_ => _.command_id),
        "update_id" -> fieldStrategy.stringOptional(_ => _.update_id),
        "rejection_status_code" -> fieldStrategy.intOptional(_ => _.rejection_status_code),
        "rejection_status_message" -> fieldStrategy.stringOptional(_ => _.rejection_status_message),
        "rejection_status_details" -> fieldStrategy.byteaOptional(_ => _.rejection_status_details),
        "submission_id" -> fieldStrategy.stringOptional(_ => _.submission_id),
        "deduplication_offset" -> fieldStrategy.bigintOptional(_ => _.deduplication_offset),
        "deduplication_duration_seconds" -> fieldStrategy.bigintOptional(_ =>
          _.deduplication_duration_seconds
        ),
        "deduplication_duration_nanos" -> fieldStrategy.intOptional(_ =>
          _.deduplication_duration_nanos
        ),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "message_uuid" -> fieldStrategy.stringOptional(_ => _.message_uuid),
        "is_transaction" -> fieldStrategy.boolean(_ => _.is_transaction),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
      )

    val stringInterningTable: Table[DbDto.StringInterningDto] =
      fieldStrategy.insert("lapi_string_interning")(
        "internal_id" -> fieldStrategy.int(_ => _.internalId),
        "external_string" -> fieldStrategy.string(_ => _.externalString),
      )

    val idFilterCreateStakeholderTable: Table[DbDto.IdFilterCreateStakeholder] =
      fieldStrategy.insert("lapi_pe_create_id_filter_stakeholder")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.first_per_sequential_id)(true)
        ),
      )

    val idFilterCreateNonStakeholderInformeeTable
        : Table[DbDto.IdFilterCreateNonStakeholderInformee] =
      fieldStrategy.insert("lapi_pe_create_id_filter_non_stakeholder_informee")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.first_per_sequential_id)(true)
        ),
      )

    val idFilterConsumingStakeholderTable: Table[DbDto.IdFilterConsumingStakeholder] =
      fieldStrategy.insert("lapi_pe_consuming_id_filter_stakeholder")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.first_per_sequential_id)(true)
        ),
      )

    val idFilterConsumingNonStakeholderInformeeTable
        : Table[DbDto.IdFilterConsumingNonStakeholderInformee] =
      fieldStrategy.insert("lapi_pe_consuming_id_filter_non_stakeholder_informee")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.first_per_sequential_id)(true)
        ),
      )

    val idFilterNonConsumingInformeeTable: Table[DbDto.IdFilterNonConsumingInformee] =
      fieldStrategy.insert("lapi_pe_non_consuming_id_filter_informee")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.first_per_sequential_id)(true)
        ),
      )

    val idFilterUnassignStakeholderTable: Table[DbDto.IdFilterUnassignStakeholder] =
      fieldStrategy.insert("lapi_pe_reassignment_id_filter_stakeholder")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.first_per_sequential_id)(true)
        ),
      )

    val idFilterAssignStakeholderTable: Table[DbDto.IdFilterAssignStakeholder] =
      fieldStrategy.insert("lapi_pe_assign_id_filter_stakeholder")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.first_per_sequential_id)(true)
        ),
      )

    val transactionMeta: Table[DbDto.TransactionMeta] =
      fieldStrategy.insert("lapi_update_meta")(
        "update_id" -> fieldStrategy.string(_ => _.update_id),
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "publication_time" -> fieldStrategy.bigint(_ => _.publication_time),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "event_sequential_id_first" -> fieldStrategy.bigint(_ => _.event_sequential_id_first),
        "event_sequential_id_last" -> fieldStrategy.bigint(_ => _.event_sequential_id_last),
      )

    val executes: Seq[Array[Array[_]] => Connection => Unit] = List(
      eventsCreate.executeUpdate,
      eventsConsumingExercise.executeUpdate,
      eventsNonConsumingExercise.executeUpdate,
      eventsUnassign.executeUpdate,
      eventsAssign.executeUpdate,
      partyEntries.executeUpdate,
      partyToParticipant.executeUpdate,
      commandCompletions.executeUpdate,
      stringInterningTable.executeUpdate,
      idFilterCreateStakeholderTable.executeUpdate,
      idFilterCreateNonStakeholderInformeeTable.executeUpdate,
      idFilterConsumingStakeholderTable.executeUpdate,
      idFilterConsumingNonStakeholderInformeeTable.executeUpdate,
      idFilterNonConsumingInformeeTable.executeUpdate,
      idFilterUnassignStakeholderTable.executeUpdate,
      idFilterAssignStakeholderTable.executeUpdate,
      transactionMeta.executeUpdate,
    )

    new Schema[DbDto] {
      override def prepareData(
          in: Vector[DbDto],
          stringInterning: StringInterning,
      ): Array[Array[Array[_]]] = {
        def collectWithFilter[T <: DbDto: ClassTag](filter: T => Boolean): Vector[T] =
          in.collect { case dbDto: T if filter(dbDto) => dbDto }
        def collect[T <: DbDto: ClassTag]: Vector[T] = collectWithFilter[T](_ => true)
        import DbDto.*
        Array(
          eventsCreate.prepareData(collect[EventCreate], stringInterning),
          eventsConsumingExercise
            .prepareData(collectWithFilter[EventExercise](_.consuming), stringInterning),
          eventsNonConsumingExercise
            .prepareData(collectWithFilter[EventExercise](!_.consuming), stringInterning),
          eventsUnassign.prepareData(collect[EventUnassign], stringInterning),
          eventsAssign.prepareData(collect[EventAssign], stringInterning),
          partyEntries.prepareData(collect[PartyEntry], stringInterning),
          partyToParticipant.prepareData(collect[EventPartyToParticipant], stringInterning),
          commandCompletions.prepareData(collect[CommandCompletion], stringInterning),
          stringInterningTable.prepareData(collect[StringInterningDto], stringInterning),
          idFilterCreateStakeholderTable
            .prepareData(collect[IdFilterCreateStakeholder], stringInterning),
          idFilterCreateNonStakeholderInformeeTable
            .prepareData(collect[IdFilterCreateNonStakeholderInformee], stringInterning),
          idFilterConsumingStakeholderTable
            .prepareData(collect[IdFilterConsumingStakeholder], stringInterning),
          idFilterConsumingNonStakeholderInformeeTable
            .prepareData(collect[IdFilterConsumingNonStakeholderInformee], stringInterning),
          idFilterNonConsumingInformeeTable
            .prepareData(collect[IdFilterNonConsumingInformee], stringInterning),
          idFilterUnassignStakeholderTable
            .prepareData(collect[IdFilterUnassignStakeholder], stringInterning),
          idFilterAssignStakeholderTable
            .prepareData(collect[IdFilterAssignStakeholder], stringInterning),
          transactionMeta.prepareData(collect[TransactionMeta], stringInterning),
        )
      }

      override def executeUpdate(data: Array[Array[Array[_]]], connection: Connection): Unit =
        executes.zip(data).foreach { case (execute, data) =>
          execute(data)(connection)
        }
    }
  }
}
