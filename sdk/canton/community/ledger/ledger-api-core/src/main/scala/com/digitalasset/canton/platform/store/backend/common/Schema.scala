// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.platform.store.backend.Conversions.IntArrayDBSerialization.encodeToByteArray
import com.digitalasset.canton.platform.store.backend.DbDto
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, Identifier, NameTypeConRef, Party}

import java.sql.Connection
import scala.reflect.ClassTag

private[backend] trait Schema[FROM] {
  def prepareData(in: Vector[FROM], stringInterning: StringInterning): Array[Array[Array[?]]]
  def executeUpdate(data: Array[Array[Array[?]]], connection: Connection): Unit
}

private[backend] object AppendOnlySchema {

  type Batch = Array[Array[Array[?]]]

  private[backend] trait FieldStrategy {
    def string[FROM](extractor: StringInterning => FROM => String): Field[FROM, String, ?] =
      StringField(extractor)

    def stringOptional[FROM](
        extractor: StringInterning => FROM => Option[String]
    ): Field[FROM, Option[String], ?] =
      StringOptional(extractor)

    def stringArray[FROM](
        extractor: StringInterning => FROM => Iterable[String]
    ): Field[FROM, Iterable[String], ?] =
      StringArray(extractor)

    def bytea[FROM](
        extractor: StringInterning => FROM => Array[Byte]
    ): Field[FROM, Array[Byte], ?] =
      Bytea(extractor)

    def byteaOptional[FROM](
        extractor: StringInterning => FROM => Option[Array[Byte]]
    ): Field[FROM, Option[Array[Byte]], ?] =
      ByteaOptional(extractor)

    def party[FROM](extractor: FROM => Party): Field[FROM, Int, ?] =
      int[FROM](stringInterning => from => stringInterning.party.internalize(extractor(from)))

    def partyOptional[FROM](extractor: FROM => Option[Party]): Field[FROM, Option[Int], ?] =
      intOptional[FROM](stringInterning =>
        from => extractor(from).map(stringInterning.party.internalize)
      )

    def parties[FROM](
        extractor: FROM => Set[Party]
    ): Field[FROM, Array[Byte], ?] =
      bytea(stringInterning =>
        from =>
          encodeToByteArray(
            extractor(from).map(stringInterning.party.internalize)
          )
      )

    def partiesOptional[FROM](
        extractor: FROM => Option[Set[Party]]
    ): Field[FROM, Option[Array[Byte]], ?] =
      byteaOptional(stringInterning =>
        from =>
          extractor(from)
            .map(_.map(stringInterning.party.internalize))
            .map(encodeToByteArray)
      )

    def template[FROM](extractor: FROM => NameTypeConRef): Field[FROM, Int, ?] =
      int[FROM](intern => from => intern.templateId.internalize(extractor(from)))

    def templateOptional[FROM](
        extractor: FROM => Option[NameTypeConRef]
    ): Field[FROM, Option[Int], ?] =
      intOptional[FROM](intern => from => extractor(from).map(intern.templateId.internalize))

    def interface[FROM](extractor: FROM => Identifier): Field[FROM, Int, ?] =
      int[FROM](intern => from => intern.interfaceId.internalize(extractor(from)))

    def interfaceOptional[FROM](
        extractor: FROM => Option[Identifier]
    ): Field[FROM, Option[Int], ?] =
      intOptional[FROM](intern => from => extractor(from).map(intern.interfaceId.internalize))

    def choice[FROM](extractor: FROM => ChoiceName): Field[FROM, Int, ?] =
      int[FROM](intern => from => intern.choiceName.internalize(extractor(from)))

    def choiceOptional[FROM](extractor: FROM => Option[ChoiceName]): Field[FROM, Option[Int], ?] =
      intOptional[FROM](intern => from => extractor(from).map(intern.choiceName.internalize))

    def bigint[FROM](extractor: StringInterning => FROM => Long): Field[FROM, Long, ?] =
      Bigint(extractor)

    def bigintOptional[FROM](
        extractor: StringInterning => FROM => Option[Long]
    ): Field[FROM, Option[Long], ?] =
      BigintOptional(extractor)

    def smallintOptional[FROM](
        extractor: StringInterning => FROM => Option[Int]
    ): Field[FROM, Option[Int], ?] =
      SmallintOptional(extractor)

    def smallint[FROM](
        extractor: StringInterning => FROM => Int
    ): Field[FROM, Int, ?] =
      Smallint(extractor)

    def int[FROM](extractor: StringInterning => FROM => Int): Field[FROM, Int, ?] =
      Integer(extractor)

    def intOptional[FROM](
        extractor: StringInterning => FROM => Option[Int]
    ): Field[FROM, Option[Int], ?] =
      IntOptional(extractor)

    def booleanOptional[FROM](
        extractor: StringInterning => FROM => Option[Boolean]
    ): Field[FROM, Option[Boolean], ?] =
      BooleanOptional(extractor)

    def boolean[FROM](
        extractor: StringInterning => FROM => Boolean
    ): Field[FROM, Boolean, ?] =
      BooleanMandatory(extractor)

    def insert[FROM](tableName: String)(fields: (String, Field[FROM, ?, ?])*): Table[FROM]
  }

  def apply(fieldStrategy: FieldStrategy): Schema[DbDto] = {
    def idFilter[T <: DbDto.IdFilterDbDto](tableName: String): Table[T] =
      fieldStrategy.insert(tableName)(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.idFilter.event_sequential_id),
        "template_id" -> fieldStrategy.template(_.idFilter.template_id),
        "party_id" -> fieldStrategy.party(_.idFilter.party_id),
        "first_per_sequential_id" -> fieldStrategy.booleanOptional(_ =>
          dto => Option.when(dto.idFilter.first_per_sequential_id)(true)
        ),
      )

    val eventActivate: Table[DbDto.EventActivate] =
      fieldStrategy.insert("lapi_events_activate_contract")(
        // update related columns
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.bytea(_ => _.update_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "submitters" -> fieldStrategy.partiesOptional(_.submitters),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
        "external_transaction_hash" -> fieldStrategy.byteaOptional(_ =>
          _.external_transaction_hash
        ),

        // event related columns
        "event_type" -> fieldStrategy.smallint(_ => _.event_type),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "node_id" -> fieldStrategy.int(_ => _.node_id),
        "additional_witnesses" -> fieldStrategy.partiesOptional(_.additional_witnesses),
        "source_synchronizer_id" -> fieldStrategy.intOptional(stringInterning =>
          _.source_synchronizer_id.map(stringInterning.synchronizerId.internalize)
        ),
        "reassignment_counter" -> fieldStrategy.bigintOptional(_ => _.reassignment_counter),
        "reassignment_id" -> fieldStrategy.byteaOptional(_ => _.reassignment_id),
        "representative_package_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.packageId.unsafe.internalize(dbDto.representative_package_id)
        ),

        // contract related columns
        "internal_contract_id" -> fieldStrategy.bigint(_ => _.internal_contract_id),
        "create_key_hash" -> fieldStrategy.stringOptional(_ => _.create_key_hash),
      )
    val idFilterActivateStakeholder: Table[DbDto.IdFilterActivateStakeholder] =
      idFilter("lapi_filter_activate_stakeholder")
    val idFilterActivateWitness: Table[DbDto.IdFilterActivateWitness] =
      idFilter("lapi_filter_activate_witness")

    val eventDeactivate: Table[DbDto.EventDeactivate] =
      fieldStrategy.insert("lapi_events_deactivate_contract")(
        // update related columns
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.bytea(_ => _.update_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "submitters" -> fieldStrategy.partiesOptional(_.submitters),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
        "external_transaction_hash" -> fieldStrategy.byteaOptional(_ =>
          _.external_transaction_hash
        ),

        // event related columns
        "event_type" -> fieldStrategy.smallint(_ => _.event_type),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "node_id" -> fieldStrategy.int(_ => _.node_id),
        "deactivated_event_sequential_id" -> fieldStrategy.bigintOptional(_ =>
          _.deactivated_event_sequential_id
        ),
        "additional_witnesses" -> fieldStrategy.partiesOptional(_.additional_witnesses),
        "exercise_choice" -> fieldStrategy.choiceOptional(_.exercise_choice),
        "exercise_choice_interface" -> fieldStrategy.interfaceOptional(
          _.exercise_choice_interface_id
        ),
        "exercise_argument" -> fieldStrategy.byteaOptional(_ => _.exercise_argument),
        "exercise_result" -> fieldStrategy.byteaOptional(_ => _.exercise_result),
        "exercise_actors" -> fieldStrategy.partiesOptional(_.exercise_actors),
        "exercise_last_descendant_node_id" -> fieldStrategy.intOptional(_ =>
          _.exercise_last_descendant_node_id
        ),
        "exercise_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_argument_compression
        ),
        "exercise_result_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_result_compression
        ),
        "reassignment_id" -> fieldStrategy.byteaOptional(_ => _.reassignment_id),
        "assignment_exclusivity" -> fieldStrategy.bigintOptional(_ => _.assignment_exclusivity),
        "target_synchronizer_id" -> fieldStrategy.intOptional(stringInterning =>
          _.target_synchronizer_id.map(stringInterning.synchronizerId.internalize)
        ),
        "reassignment_counter" -> fieldStrategy.bigintOptional(_ => _.reassignment_counter),

        // contract related columns
        "contract_id" -> fieldStrategy.bytea(_ => _.contract_id.toBytes.toByteArray),
        "internal_contract_id" -> fieldStrategy.bigintOptional(_ => _.internal_contract_id),
        "template_id" -> fieldStrategy.template(_.template_id),
        "package_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.packageId.unsafe.internalize(dbDto.package_id)
        ),
        "stakeholders" -> fieldStrategy.parties(_.stakeholders),
        "ledger_effective_time" -> fieldStrategy.bigintOptional(_ => _.ledger_effective_time),
      )
    val idFilterDeactivateStakeholder: Table[DbDto.IdFilterDeactivateStakeholder] =
      idFilter("lapi_filter_deactivate_stakeholder")
    val idFilterDeactivateWitness: Table[DbDto.IdFilterDeactivateWitness] =
      idFilter("lapi_filter_deactivate_witness")

    val eventVariousWitnessed: Table[DbDto.EventVariousWitnessed] =
      fieldStrategy.insert("lapi_events_various_witnessed")(
        // update related columns
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.bytea(_ => _.update_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "submitters" -> fieldStrategy.partiesOptional(_.submitters),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "trace_context" -> fieldStrategy.bytea(_ => _.trace_context),
        "external_transaction_hash" -> fieldStrategy.byteaOptional(_ =>
          _.external_transaction_hash
        ),

        // event related columns
        "event_type" -> fieldStrategy.smallint(_ => _.event_type),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "node_id" -> fieldStrategy.int(_ => _.node_id),
        "additional_witnesses" -> fieldStrategy.parties(_.additional_witnesses),
        "consuming" -> fieldStrategy.booleanOptional(_ => _.consuming),
        "exercise_choice" -> fieldStrategy.choiceOptional(_.exercise_choice),
        "exercise_choice_interface" -> fieldStrategy.interfaceOptional(
          _.exercise_choice_interface_id
        ),
        "exercise_argument" -> fieldStrategy.byteaOptional(_ => _.exercise_argument),
        "exercise_result" -> fieldStrategy.byteaOptional(_ => _.exercise_result),
        "exercise_actors" -> fieldStrategy.partiesOptional(_.exercise_actors),
        "exercise_last_descendant_node_id" -> fieldStrategy.intOptional(_ =>
          _.exercise_last_descendant_node_id
        ),
        "exercise_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_argument_compression
        ),
        "exercise_result_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_result_compression
        ),
        "representative_package_id" -> fieldStrategy.intOptional(stringInterning =>
          _.representative_package_id.map(stringInterning.packageId.unsafe.internalize)
        ),

        // contract related columns
        "contract_id" -> fieldStrategy.byteaOptional(_ => _.contract_id.map(_.toBytes.toByteArray)),
        "internal_contract_id" -> fieldStrategy.bigintOptional(_ => _.internal_contract_id),
        "template_id" -> fieldStrategy.templateOptional(_.template_id),
        "package_id" -> fieldStrategy.intOptional(stringInterning =>
          _.package_id.map(stringInterning.packageId.unsafe.internalize)
        ),
        "ledger_effective_time" -> fieldStrategy.bigintOptional(_ => _.ledger_effective_time),
      )
    val idFilterVariousWitness: Table[DbDto.IdFilterVariousWitness] =
      idFilter("lapi_filter_various_witness")

    val partyEntries: Table[DbDto.PartyEntry] =
      fieldStrategy.insert("lapi_party_entries")(
        "ledger_offset" -> fieldStrategy.bigint(_ => _.ledger_offset),
        "recorded_at" -> fieldStrategy.bigint(_ => _.recorded_at),
        "submission_id" -> fieldStrategy.stringOptional(_ => _.submission_id),
        "party" -> fieldStrategy.stringOptional(_ => _.party),
        "typ" -> fieldStrategy.string(_ => _.typ),
        "rejection_reason" -> fieldStrategy.stringOptional(_ => _.rejection_reason),
        "is_local" -> fieldStrategy.booleanOptional(_ => _.is_local),
        "party_id" -> fieldStrategy.partyOptional(_.party),
      )

    val partyToParticipant: Table[DbDto.EventPartyToParticipant] =
      fieldStrategy.insert("lapi_events_party_to_participant")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "update_id" -> fieldStrategy.bytea(_ => _.update_id),
        "party_id" -> fieldStrategy.party(_.party_id),
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
        "submitters" -> fieldStrategy.parties(_.submitters),
        "command_id" -> fieldStrategy.string(_ => _.command_id),
        "update_id" -> fieldStrategy.byteaOptional(_ => _.update_id),
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

    val transactionMeta: Table[DbDto.TransactionMeta] =
      fieldStrategy.insert("lapi_update_meta")(
        "update_id" -> fieldStrategy.bytea(_ => _.update_id),
        "event_offset" -> fieldStrategy.bigint(_ => _.event_offset),
        "publication_time" -> fieldStrategy.bigint(_ => _.publication_time),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "synchronizer_id" -> fieldStrategy.int(stringInterning =>
          dbDto => stringInterning.synchronizerId.internalize(dbDto.synchronizer_id)
        ),
        "event_sequential_id_first" -> fieldStrategy.bigint(_ => _.event_sequential_id_first),
        "event_sequential_id_last" -> fieldStrategy.bigint(_ => _.event_sequential_id_last),
      )

    val executes: Seq[Array[Array[?]] => Connection => Unit] = List(
      eventActivate.executeUpdate,
      idFilterActivateStakeholder.executeUpdate,
      idFilterActivateWitness.executeUpdate,
      eventDeactivate.executeUpdate,
      idFilterDeactivateStakeholder.executeUpdate,
      idFilterDeactivateWitness.executeUpdate,
      eventVariousWitnessed.executeUpdate,
      idFilterVariousWitness.executeUpdate,
      partyEntries.executeUpdate,
      partyToParticipant.executeUpdate,
      commandCompletions.executeUpdate,
      stringInterningTable.executeUpdate,
      transactionMeta.executeUpdate,
    )

    new Schema[DbDto] {
      override def prepareData(
          in: Vector[DbDto],
          stringInterning: StringInterning,
      ): Array[Array[Array[?]]] = {
        def collectWithFilter[T <: DbDto: ClassTag](filter: T => Boolean): Vector[T] =
          in.collect { case dbDto: T if filter(dbDto) => dbDto }
        def collect[T <: DbDto: ClassTag]: Vector[T] = collectWithFilter[T](_ => true)
        import DbDto.*
        Array(
          eventActivate.prepareData(collect[EventActivate], stringInterning),
          idFilterActivateStakeholder
            .prepareData(collect[IdFilterActivateStakeholder], stringInterning),
          idFilterActivateWitness.prepareData(collect[IdFilterActivateWitness], stringInterning),
          eventDeactivate.prepareData(collect[EventDeactivate], stringInterning),
          idFilterDeactivateStakeholder
            .prepareData(collect[IdFilterDeactivateStakeholder], stringInterning),
          idFilterDeactivateWitness
            .prepareData(collect[IdFilterDeactivateWitness], stringInterning),
          eventVariousWitnessed.prepareData(collect[EventVariousWitnessed], stringInterning),
          idFilterVariousWitness.prepareData(collect[IdFilterVariousWitness], stringInterning),
          partyEntries.prepareData(collect[PartyEntry], stringInterning),
          partyToParticipant.prepareData(collect[EventPartyToParticipant], stringInterning),
          commandCompletions.prepareData(collect[CommandCompletion], stringInterning),
          stringInterningTable.prepareData(collect[StringInterningDto], stringInterning),
          transactionMeta.prepareData(collect[TransactionMeta], stringInterning),
        )
      }

      override def executeUpdate(data: Array[Array[Array[?]]], connection: Connection): Unit =
        executes.zip(data).foreach { case (execute, data) =>
          execute(data)(connection)
        }
    }
  }
}
