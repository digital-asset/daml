// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.interning.StringInterning

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

    def stringArrayOptional[FROM](
        extractor: StringInterning => FROM => Option[Iterable[String]]
    ): Field[FROM, Option[Iterable[String]], _] =
      StringArrayOptional(extractor)

    def intArray[FROM](
        extractor: StringInterning => FROM => Iterable[Int]
    ): Field[FROM, Iterable[Int], _] =
      IntArray(extractor)

    def intArrayOptional[FROM](
        extractor: StringInterning => FROM => Option[Iterable[Int]]
    ): Field[FROM, Option[Iterable[Int]], _] =
      IntArrayOptional(extractor)

    def bytea[FROM](
        extractor: StringInterning => FROM => Array[Byte]
    ): Field[FROM, Array[Byte], _] =
      Bytea(extractor)

    def byteaOptional[FROM](
        extractor: StringInterning => FROM => Option[Array[Byte]]
    ): Field[FROM, Option[Array[Byte]], _] =
      ByteaOptional(extractor)

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

    def insert[FROM](tableName: String)(fields: (String, Field[FROM, _, _])*): Table[FROM]
    def idempotentInsert[FROM](tableName: String, keyFieldIndex: Int)(
        fields: (String, Field[FROM, _, _])*
    ): Table[FROM]
  }

  def apply(fieldStrategy: FieldStrategy): Schema[DbDto] = {
    val eventsDivulgence: Table[DbDto.EventDivulgence] =
      fieldStrategy.insert("participant_events_divulgence")(
        "event_offset" -> fieldStrategy.stringOptional(_ => _.event_offset),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "application_id" -> fieldStrategy.stringOptional(_ => _.application_id),
        "submitters" -> fieldStrategy.intArrayOptional(stringInterning =>
          _.submitters.map(_.map(stringInterning.party.unsafe.internalize))
        ),
        "contract_id" -> fieldStrategy.string(_ => _.contract_id),
        "template_id" -> fieldStrategy.intOptional(stringInterning =>
          _.template_id.map(stringInterning.templateId.unsafe.internalize)
        ),
        "tree_event_witnesses" -> fieldStrategy.intArray(stringInterning =>
          _.tree_event_witnesses.map(stringInterning.party.unsafe.internalize)
        ),
        "create_argument" -> fieldStrategy.byteaOptional(_ => _.create_argument),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "create_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_argument_compression
        ),
      )

    val eventsCreate: Table[DbDto.EventCreate] =
      fieldStrategy.insert("participant_events_create")(
        "event_offset" -> fieldStrategy.stringOptional(_ => _.event_offset),
        "transaction_id" -> fieldStrategy.stringOptional(_ => _.transaction_id),
        "ledger_effective_time" -> fieldStrategy.bigintOptional(_ => _.ledger_effective_time),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "application_id" -> fieldStrategy.stringOptional(_ => _.application_id),
        "submitters" -> fieldStrategy.intArrayOptional(stringInterning =>
          _.submitters.map(_.map(stringInterning.party.unsafe.internalize))
        ),
        "node_index" -> fieldStrategy.intOptional(_ => _.node_index),
        "event_id" -> fieldStrategy.stringOptional(_ => _.event_id),
        "contract_id" -> fieldStrategy.string(_ => _.contract_id),
        "template_id" -> fieldStrategy.intOptional(stringInterning =>
          _.template_id.map(stringInterning.templateId.unsafe.internalize)
        ),
        "flat_event_witnesses" -> fieldStrategy.intArray(stringInterning =>
          _.flat_event_witnesses.map(stringInterning.party.unsafe.internalize)
        ),
        "tree_event_witnesses" -> fieldStrategy.intArray(stringInterning =>
          _.tree_event_witnesses.map(stringInterning.party.unsafe.internalize)
        ),
        "create_argument" -> fieldStrategy.byteaOptional(_ => _.create_argument),
        "create_signatories" -> fieldStrategy.intArrayOptional(stringInterning =>
          _.create_signatories.map(_.map(stringInterning.party.unsafe.internalize))
        ),
        "create_observers" -> fieldStrategy.intArrayOptional(stringInterning =>
          _.create_observers.map(_.map(stringInterning.party.unsafe.internalize))
        ),
        "create_agreement_text" -> fieldStrategy.stringOptional(_ => _.create_agreement_text),
        "create_key_value" -> fieldStrategy.byteaOptional(_ => _.create_key_value),
        "create_key_hash" -> fieldStrategy.stringOptional(_ => _.create_key_hash),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "create_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_argument_compression
        ),
        "create_key_value_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_key_value_compression
        ),
        "driver_metadata" -> fieldStrategy.byteaOptional(_ => _.driver_metadata),
      )

    val exerciseFields: Vector[(String, Field[DbDto.EventExercise, _, _])] =
      Vector[(String, Field[DbDto.EventExercise, _, _])](
        "event_id" -> fieldStrategy.stringOptional(_ => _.event_id),
        "event_offset" -> fieldStrategy.stringOptional(_ => _.event_offset),
        "contract_id" -> fieldStrategy.string(_ => _.contract_id),
        "transaction_id" -> fieldStrategy.stringOptional(_ => _.transaction_id),
        "ledger_effective_time" -> fieldStrategy.bigintOptional(_ => _.ledger_effective_time),
        "node_index" -> fieldStrategy.intOptional(_ => _.node_index),
        "command_id" -> fieldStrategy.stringOptional(_ => _.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_ => _.workflow_id),
        "application_id" -> fieldStrategy.stringOptional(_ => _.application_id),
        "submitters" -> fieldStrategy.intArrayOptional(stringInterning =>
          _.submitters.map(_.map(stringInterning.party.unsafe.internalize))
        ),
        "create_key_value" -> fieldStrategy.byteaOptional(_ => _.create_key_value),
        "exercise_choice" -> fieldStrategy.stringOptional(_ => _.exercise_choice),
        "exercise_argument" -> fieldStrategy.byteaOptional(_ => _.exercise_argument),
        "exercise_result" -> fieldStrategy.byteaOptional(_ => _.exercise_result),
        "exercise_actors" -> fieldStrategy.intArrayOptional(stringInterning =>
          _.exercise_actors.map(_.map(stringInterning.party.unsafe.internalize))
        ),
        "exercise_child_event_ids" -> fieldStrategy.stringArrayOptional(_ =>
          _.exercise_child_event_ids
        ),
        "template_id" -> fieldStrategy.intOptional(stringInterning =>
          _.template_id.map(stringInterning.templateId.unsafe.internalize)
        ),
        "flat_event_witnesses" -> fieldStrategy.intArray(stringInterning =>
          _.flat_event_witnesses.map(stringInterning.party.unsafe.internalize)
        ),
        "tree_event_witnesses" -> fieldStrategy.intArray(stringInterning =>
          _.tree_event_witnesses.map(stringInterning.party.unsafe.internalize)
        ),
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "create_key_value_compression" -> fieldStrategy.smallintOptional(_ =>
          _.create_key_value_compression
        ),
        "exercise_argument_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_argument_compression
        ),
        "exercise_result_compression" -> fieldStrategy.smallintOptional(_ =>
          _.exercise_result_compression
        ),
      )

    val eventsConsumingExercise: Table[DbDto.EventExercise] =
      fieldStrategy.insert("participant_events_consuming_exercise")(exerciseFields: _*)

    val eventsNonConsumingExercise: Table[DbDto.EventExercise] =
      fieldStrategy.insert("participant_events_non_consuming_exercise")(exerciseFields: _*)

    val configurationEntries: Table[DbDto.ConfigurationEntry] =
      fieldStrategy.insert("configuration_entries")(
        "ledger_offset" -> fieldStrategy.string(_ => _.ledger_offset),
        "recorded_at" -> fieldStrategy.bigint(_ => _.recorded_at),
        "submission_id" -> fieldStrategy.string(_ => _.submission_id),
        "typ" -> fieldStrategy.string(_ => _.typ),
        "configuration" -> fieldStrategy.bytea(_ => _.configuration),
        "rejection_reason" -> fieldStrategy.stringOptional(_ => _.rejection_reason),
      )

    val packageEntries: Table[DbDto.PackageEntry] =
      fieldStrategy.insert("package_entries")(
        "ledger_offset" -> fieldStrategy.string(_ => _.ledger_offset),
        "recorded_at" -> fieldStrategy.bigint(_ => _.recorded_at),
        "submission_id" -> fieldStrategy.stringOptional(_ => _.submission_id),
        "typ" -> fieldStrategy.string(_ => _.typ),
        "rejection_reason" -> fieldStrategy.stringOptional(_ => _.rejection_reason),
      )

    val packages: Table[DbDto.Package] =
      fieldStrategy.idempotentInsert(
        tableName = "packages",
        keyFieldIndex = 0,
      )(
        "package_id" -> fieldStrategy.string(_ => _.package_id),
        "upload_id" -> fieldStrategy.string(_ => _.upload_id),
        "source_description" -> fieldStrategy.stringOptional(_ => _.source_description),
        "package_size" -> fieldStrategy.bigint(_ => _.package_size),
        "known_since" -> fieldStrategy.bigint(_ => _.known_since),
        "ledger_offset" -> fieldStrategy.string(_ => _.ledger_offset),
        "package" -> fieldStrategy.bytea(_ => _._package),
      )

    val partyEntries: Table[DbDto.PartyEntry] =
      fieldStrategy.insert("party_entries")(
        "ledger_offset" -> fieldStrategy.string(_ => _.ledger_offset),
        "recorded_at" -> fieldStrategy.bigint(_ => _.recorded_at),
        "submission_id" -> fieldStrategy.stringOptional(_ => _.submission_id),
        "party" -> fieldStrategy.stringOptional(_ => _.party),
        "display_name" -> fieldStrategy.stringOptional(_ => _.display_name),
        "typ" -> fieldStrategy.string(_ => _.typ),
        "rejection_reason" -> fieldStrategy.stringOptional(_ => _.rejection_reason),
        "is_local" -> fieldStrategy.booleanOptional(_ => _.is_local),
        "party_id" -> fieldStrategy.intOptional(stringInterning =>
          _.party.map(stringInterning.party.unsafe.internalize)
        ),
      )

    val commandCompletions: Table[DbDto.CommandCompletion] =
      fieldStrategy.insert("participant_command_completions")(
        "completion_offset" -> fieldStrategy.string(_ => _.completion_offset),
        "record_time" -> fieldStrategy.bigint(_ => _.record_time),
        "application_id" -> fieldStrategy.string(_ => _.application_id),
        "submitters" -> fieldStrategy.intArray(stringInterning =>
          _.submitters.map(stringInterning.party.unsafe.internalize)
        ),
        "command_id" -> fieldStrategy.string(_ => _.command_id),
        "transaction_id" -> fieldStrategy.stringOptional(_ => _.transaction_id),
        "rejection_status_code" -> fieldStrategy.intOptional(_ => _.rejection_status_code),
        "rejection_status_message" -> fieldStrategy.stringOptional(_ => _.rejection_status_message),
        "rejection_status_details" -> fieldStrategy.byteaOptional(_ => _.rejection_status_details),
        "submission_id" -> fieldStrategy.stringOptional(_ => _.submission_id),
        "deduplication_offset" -> fieldStrategy.stringOptional(_ => _.deduplication_offset),
        "deduplication_duration_seconds" -> fieldStrategy.bigintOptional(_ =>
          _.deduplication_duration_seconds
        ),
        "deduplication_duration_nanos" -> fieldStrategy.intOptional(_ =>
          _.deduplication_duration_nanos
        ),
        "deduplication_start" -> fieldStrategy.bigintOptional(_ => _.deduplication_start),
      )

    val stringInterningTable: Table[DbDto.StringInterningDto] =
      fieldStrategy.insert("string_interning")(
        "internal_id" -> fieldStrategy.int(_ => _.internalId),
        "external_string" -> fieldStrategy.string(_ => _.externalString),
      )

    val idFilterCreateStakeholderTable: Table[DbDto.IdFilterCreateStakeholder] =
      fieldStrategy.insert("pe_create_id_filter_stakeholder")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
      )

    val idFilterCreateNonStakeholderInformeeTable
        : Table[DbDto.IdFilterCreateNonStakeholderInformee] =
      fieldStrategy.insert("pe_create_id_filter_non_stakeholder_informee")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
      )

    val idFilterConsumingStakeholderTable: Table[DbDto.IdFilterConsumingStakeholder] =
      fieldStrategy.insert("pe_consuming_id_filter_stakeholder")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "template_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.templateId.unsafe.internalize(dto.template_id)
        ),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
      )

    val idFilterConsumingNonStakeholderInformeeTable
        : Table[DbDto.IdFilterConsumingNonStakeholderInformee] =
      fieldStrategy.insert("pe_consuming_id_filter_non_stakeholder_informee")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
      )

    val idFilterNonConsumingInformeeTable: Table[DbDto.IdFilterNonConsumingInformee] =
      fieldStrategy.insert("pe_non_consuming_id_filter_informee")(
        "event_sequential_id" -> fieldStrategy.bigint(_ => _.event_sequential_id),
        "party_id" -> fieldStrategy.int(stringInterning =>
          dto => stringInterning.party.unsafe.internalize(dto.party_id)
        ),
      )

    val transactionMeta: Table[DbDto.TransactionMeta] =
      fieldStrategy.insert("participant_transaction_meta")(
        "transaction_id" -> fieldStrategy.string(_ => _.transaction_id),
        "event_offset" -> fieldStrategy.string(_ => _.event_offset),
        "event_sequential_id_first" -> fieldStrategy.bigint(_ => _.event_sequential_id_first),
        "event_sequential_id_last" -> fieldStrategy.bigint(_ => _.event_sequential_id_last),
      )

    val transactionMetering: Table[DbDto.TransactionMetering] =
      fieldStrategy.insert("transaction_metering")(
        fields = "application_id" -> fieldStrategy.string(_ => _.application_id),
        "action_count" -> fieldStrategy.int(_ => _.action_count),
        "metering_timestamp" -> fieldStrategy.bigint(_ => _.metering_timestamp),
        "ledger_offset" -> fieldStrategy.string(_ => _.ledger_offset),
      )

    val executes: Seq[Array[Array[_]] => Connection => Unit] = List(
      eventsDivulgence.executeUpdate,
      eventsCreate.executeUpdate,
      eventsConsumingExercise.executeUpdate,
      eventsNonConsumingExercise.executeUpdate,
      configurationEntries.executeUpdate,
      packageEntries.executeUpdate,
      packages.executeUpdate,
      partyEntries.executeUpdate,
      commandCompletions.executeUpdate,
      stringInterningTable.executeUpdate,
      idFilterCreateStakeholderTable.executeUpdate,
      idFilterCreateNonStakeholderInformeeTable.executeUpdate,
      idFilterConsumingStakeholderTable.executeUpdate,
      idFilterConsumingNonStakeholderInformeeTable.executeUpdate,
      idFilterNonConsumingInformeeTable.executeUpdate,
      transactionMeta.executeUpdate,
      transactionMetering.executeUpdate,
    )

    new Schema[DbDto] {
      override def prepareData(
          in: Vector[DbDto],
          stringInterning: StringInterning,
      ): Array[Array[Array[_]]] = {
        def collectWithFilter[T <: DbDto: ClassTag](filter: T => Boolean): Vector[T] =
          in.collect { case dbDto: T if filter(dbDto) => dbDto }
        def collect[T <: DbDto: ClassTag]: Vector[T] = collectWithFilter[T](_ => true)
        import DbDto._
        Array(
          eventsDivulgence.prepareData(collect[EventDivulgence], stringInterning),
          eventsCreate.prepareData(collect[EventCreate], stringInterning),
          eventsConsumingExercise
            .prepareData(collectWithFilter[EventExercise](_.consuming), stringInterning),
          eventsNonConsumingExercise
            .prepareData(collectWithFilter[EventExercise](!_.consuming), stringInterning),
          configurationEntries.prepareData(collect[ConfigurationEntry], stringInterning),
          packageEntries.prepareData(collect[PackageEntry], stringInterning),
          packages.prepareData(collect[Package], stringInterning),
          partyEntries.prepareData(collect[PartyEntry], stringInterning),
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
          transactionMeta.prepareData(collect[TransactionMeta], stringInterning),
          transactionMetering.prepareData(collect[TransactionMetering], stringInterning),
        )
      }

      override def executeUpdate(data: Array[Array[Array[_]]], connection: Connection): Unit =
        executes.zip(data).foreach { case (execute, data) =>
          execute(data)(connection)
        }
    }
  }
}
