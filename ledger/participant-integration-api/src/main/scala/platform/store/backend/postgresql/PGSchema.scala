// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import java.sql.Connection
import java.time.Instant

import com.daml.platform.store.backend.DbDto
import com.daml.platform.store.backend.postgresql.AppendOnlySchema.FieldStrategy

import scala.reflect.ClassTag

trait Schema[FROM] {
  def prepareData(in: Vector[FROM]): Array[Array[Array[_]]]
  def executeUpdate(data: Array[Array[Array[_]]], connection: Connection): Unit
}

object AppendOnlySchema {

  type Batch = Array[Array[Array[_]]]

  trait FieldStrategy {
    def string[FROM, _](extractor: FROM => String): Field[FROM, String, _]
    def stringOptional[FROM, _](extractor: FROM => Option[String]): Field[FROM, Option[String], _]
    def stringArray[FROM, _](extractor: FROM => Iterable[String]): Field[FROM, Iterable[String], _]
    def stringArrayOptional[FROM, _](
        extractor: FROM => Option[Iterable[String]]
    ): Field[FROM, Option[Iterable[String]], _]
    def bytea[FROM, _](extractor: FROM => Array[Byte]): Field[FROM, Array[Byte], _]
    def byteaOptional[FROM, _](
        extractor: FROM => Option[Array[Byte]]
    ): Field[FROM, Option[Array[Byte]], _]
    def bigint[FROM, _](extractor: FROM => Long): Field[FROM, Long, _]
    def smallintOptional[FROM, _](extractor: FROM => Option[Int]): Field[FROM, Option[Int], _]
    def timestamp[FROM, _](extractor: FROM => Instant): Field[FROM, Instant, _]
    def timestampOptional[FROM, _](
        extractor: FROM => Option[Instant]
    ): Field[FROM, Option[Instant], _]
    def intOptional[FROM, _](extractor: FROM => Option[Int]): Field[FROM, Option[Int], _]
    def boolean[FROM, _](extractor: FROM => Boolean): Field[FROM, Boolean, _]
    def booleanOptional[FROM, _](
        extractor: FROM => Option[Boolean]
    ): Field[FROM, Option[Boolean], _]
  }

  def apply(fieldStrategy: FieldStrategy): Schema[DbDto] = {
    val eventsDivulgence: Table[DbDto.EventDivulgence] =
      PGTable.transposedInsert("participant_events_divulgence")(
        "event_offset" -> fieldStrategy.stringOptional(_.event_offset),
        "command_id" -> fieldStrategy.stringOptional(_.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_.workflow_id),
        "application_id" -> fieldStrategy.stringOptional(_.application_id),
        "submitters" -> fieldStrategy.stringArrayOptional(_.submitters),
        "contract_id" -> fieldStrategy.string(_.contract_id),
        "template_id" -> fieldStrategy.stringOptional(_.template_id),
        "tree_event_witnesses" -> fieldStrategy.stringArray(_.tree_event_witnesses),
        "create_argument" -> fieldStrategy.byteaOptional(_.create_argument),
        "event_sequential_id" -> fieldStrategy.bigint(_.event_sequential_id),
        "create_argument_compression" -> fieldStrategy.smallintOptional(
          _.create_argument_compression
        ),
      )

    val eventsCreate: Table[DbDto.EventCreate] =
      PGTable.transposedInsert("participant_events_create")(
        "event_offset" -> fieldStrategy.stringOptional(_.event_offset),
        "transaction_id" -> fieldStrategy.stringOptional(_.transaction_id),
        "ledger_effective_time" -> fieldStrategy.timestampOptional(_.ledger_effective_time),
        "command_id" -> fieldStrategy.stringOptional(_.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_.workflow_id),
        "application_id" -> fieldStrategy.stringOptional(_.application_id),
        "submitters" -> fieldStrategy.stringArrayOptional(_.submitters),
        "node_index" -> fieldStrategy.intOptional(_.node_index),
        "event_id" -> fieldStrategy.stringOptional(_.event_id),
        "contract_id" -> fieldStrategy.string(_.contract_id),
        "template_id" -> fieldStrategy.stringOptional(_.template_id),
        "flat_event_witnesses" -> fieldStrategy.stringArray(_.flat_event_witnesses),
        "tree_event_witnesses" -> fieldStrategy.stringArray(_.tree_event_witnesses),
        "create_argument" -> fieldStrategy.byteaOptional(_.create_argument),
        "create_signatories" -> fieldStrategy.stringArrayOptional(_.create_signatories),
        "create_observers" -> fieldStrategy.stringArrayOptional(_.create_observers),
        "create_agreement_text" -> fieldStrategy.stringOptional(_.create_agreement_text),
        "create_key_value" -> fieldStrategy.byteaOptional(_.create_key_value),
        "create_key_hash" -> fieldStrategy.stringOptional(_.create_key_hash),
        "event_sequential_id" -> fieldStrategy.bigint(_.event_sequential_id),
        "create_argument_compression" -> fieldStrategy.smallintOptional(
          _.create_argument_compression
        ),
        "create_key_value_compression" -> fieldStrategy.smallintOptional(
          _.create_key_value_compression
        ),
      )

    val exerciseFields: Vector[(String, Field[DbDto.EventExercise, _, _])] =
      Vector[(String, Field[DbDto.EventExercise, _, _])](
        "event_id" -> fieldStrategy.stringOptional(_.event_id),
        "event_offset" -> fieldStrategy.stringOptional(_.event_offset),
        "contract_id" -> fieldStrategy.string(_.contract_id),
        "transaction_id" -> fieldStrategy.stringOptional(_.transaction_id),
        "ledger_effective_time" -> fieldStrategy.timestampOptional(_.ledger_effective_time),
        "node_index" -> fieldStrategy.intOptional(_.node_index),
        "command_id" -> fieldStrategy.stringOptional(_.command_id),
        "workflow_id" -> fieldStrategy.stringOptional(_.workflow_id),
        "application_id" -> fieldStrategy.stringOptional(_.application_id),
        "submitters" -> fieldStrategy.stringArrayOptional(_.submitters),
        "create_key_value" -> fieldStrategy.byteaOptional(_.create_key_value),
        "exercise_choice" -> fieldStrategy.stringOptional(_.exercise_choice),
        "exercise_argument" -> fieldStrategy.byteaOptional(_.exercise_argument),
        "exercise_result" -> fieldStrategy.byteaOptional(_.exercise_result),
        "exercise_actors" -> fieldStrategy.stringArrayOptional(_.exercise_actors),
        "exercise_child_event_ids" -> fieldStrategy.stringArrayOptional(_.exercise_child_event_ids),
        "template_id" -> fieldStrategy.stringOptional(_.template_id),
        "flat_event_witnesses" -> fieldStrategy.stringArray(_.flat_event_witnesses),
        "tree_event_witnesses" -> fieldStrategy.stringArray(_.tree_event_witnesses),
        "event_sequential_id" -> fieldStrategy.bigint(_.event_sequential_id),
        "create_key_value_compression" -> fieldStrategy.smallintOptional(
          _.create_key_value_compression
        ),
        "exercise_argument_compression" -> fieldStrategy.smallintOptional(
          _.exercise_argument_compression
        ),
        "exercise_result_compression" -> fieldStrategy.smallintOptional(
          _.exercise_result_compression
        ),
      )

    val eventsConsumingExercise: Table[DbDto.EventExercise] =
      PGTable.transposedInsert("participant_events_consuming_exercise")(exerciseFields: _*)

    val eventsNonConsumingExercise: Table[DbDto.EventExercise] =
      PGTable.transposedInsert("participant_events_non_consuming_exercise")(exerciseFields: _*)

    val configurationEntries: Table[DbDto.ConfigurationEntry] =
      PGTable.transposedInsert("configuration_entries")(
        "ledger_offset" -> fieldStrategy.string(_.ledger_offset),
        "recorded_at" -> fieldStrategy.timestamp(_.recorded_at),
        "submission_id" -> fieldStrategy.string(_.submission_id),
        "typ" -> fieldStrategy.string(_.typ),
        "configuration" -> fieldStrategy.bytea(_.configuration),
        "rejection_reason" -> fieldStrategy.stringOptional(_.rejection_reason),
      )

    val packageEntries: Table[DbDto.PackageEntry] =
      PGTable.transposedInsert("package_entries")(
        "ledger_offset" -> fieldStrategy.string(_.ledger_offset),
        "recorded_at" -> fieldStrategy.timestamp(_.recorded_at),
        "submission_id" -> fieldStrategy.stringOptional(_.submission_id),
        "typ" -> fieldStrategy.string(_.typ),
        "rejection_reason" -> fieldStrategy.stringOptional(_.rejection_reason),
      )

    val packages: Table[DbDto.Package] = PGTable.transposedInsertWithSuffix(
      tableName = "packages",
      insertSuffix = "on conflict (package_id) do nothing",
    )(
      "package_id" -> fieldStrategy.string(_.package_id),
      "upload_id" -> fieldStrategy.string(_.upload_id),
      "source_description" -> fieldStrategy.stringOptional(_.source_description),
      "size" -> fieldStrategy.bigint(_.size),
      "known_since" -> fieldStrategy.timestamp(_.known_since),
      "ledger_offset" -> fieldStrategy.string(_.ledger_offset),
      "package" -> fieldStrategy.bytea(_._package),
    )

    val partyEntries: Table[DbDto.PartyEntry] =
      PGTable.transposedInsert("party_entries")(
        "ledger_offset" -> fieldStrategy.string(_.ledger_offset),
        "recorded_at" -> fieldStrategy.timestamp(_.recorded_at),
        "submission_id" -> fieldStrategy.stringOptional(_.submission_id),
        "party" -> fieldStrategy.stringOptional(_.party),
        "display_name" -> fieldStrategy.stringOptional(_.display_name),
        "typ" -> fieldStrategy.string(_.typ),
        "rejection_reason" -> fieldStrategy.stringOptional(_.rejection_reason),
        "is_local" -> fieldStrategy.booleanOptional(_.is_local),
      )

    val parties: Table[DbDto.Party] =
      PGTable.transposedInsert("parties")(
        "party" -> fieldStrategy.string(_.party),
        "display_name" -> fieldStrategy.stringOptional(_.display_name),
        "explicit" -> fieldStrategy.boolean(_.explicit),
        "ledger_offset" -> fieldStrategy.stringOptional(_.ledger_offset),
        "is_local" -> fieldStrategy.boolean(_.is_local),
      )

    val commandCompletions: Table[DbDto.CommandCompletion] =
      PGTable.transposedInsert("participant_command_completions")(
        "completion_offset" -> fieldStrategy.string(_.completion_offset),
        "record_time" -> fieldStrategy.timestamp(_.record_time),
        "application_id" -> fieldStrategy.string(_.application_id),
        "submitters" -> fieldStrategy.stringArray(_.submitters),
        "command_id" -> fieldStrategy.string(_.command_id),
        "transaction_id" -> fieldStrategy.stringOptional(_.transaction_id),
        "status_code" -> fieldStrategy.intOptional(_.status_code),
        "status_message" -> fieldStrategy.stringOptional(_.status_message),
      )

    val commandSubmissionDeletes: Table[DbDto.CommandDeduplication] =
      PGTable.transposedDelete("participant_command_submissions")(
        "deduplication_key" -> fieldStrategy.string(_.deduplication_key)
      )

    val executes: Seq[Array[Array[_]] => Connection => Unit] = List(
      eventsDivulgence.executeUpdate,
      eventsCreate.executeUpdate,
      eventsConsumingExercise.executeUpdate,
      eventsNonConsumingExercise.executeUpdate,
      configurationEntries.executeUpdate,
      packageEntries.executeUpdate,
      packages.executeUpdate,
      parties.executeUpdate,
      partyEntries.executeUpdate,
      commandCompletions.executeUpdate,
      commandSubmissionDeletes.executeUpdate,
    )

    new Schema[DbDto] {
      override def prepareData(in: Vector[DbDto]): Array[Array[Array[_]]] = {
        def collectWithFilter[T <: DbDto: ClassTag](filter: T => Boolean): Vector[T] =
          in.collect { case dbDto: T if filter(dbDto) => dbDto }
        def collect[T <: DbDto: ClassTag]: Vector[T] = collectWithFilter[T](_ => true)
        import DbDto._
        Array(
          eventsDivulgence.prepareData(collect[EventDivulgence]),
          eventsCreate.prepareData(collect[EventCreate]),
          eventsConsumingExercise.prepareData(collectWithFilter[EventExercise](_.consuming)),
          eventsNonConsumingExercise.prepareData(collectWithFilter[EventExercise](!_.consuming)),
          configurationEntries.prepareData(collect[ConfigurationEntry]),
          packageEntries.prepareData(collect[PackageEntry]),
          packages.prepareData(collect[Package]),
          parties.prepareData(collect[Party]),
          partyEntries.prepareData(collect[PartyEntry]),
          commandCompletions.prepareData(collect[CommandCompletion]),
          commandSubmissionDeletes.prepareData(collect[CommandDeduplication]),
        )
      }

      override def executeUpdate(data: Array[Array[Array[_]]], connection: Connection): Unit =
        executes.zip(data).foreach { case (execute, data) =>
          execute(data)(connection)
        }
    }
  }
}

private[postgresql] object PGSchema {
  private val PGFieldStrategy = new FieldStrategy {
    override def string[FROM, _](extractor: FROM => String): Field[FROM, String, _] =
      PGString(extractor)

    override def stringOptional[FROM, _](
        extractor: FROM => Option[String]
    ): Field[FROM, Option[String], _] =
      PGStringOptional(extractor)

    override def stringArray[FROM, _](
        extractor: FROM => Iterable[String]
    ): Field[FROM, Iterable[String], _] =
      PGStringArray(extractor)

    override def stringArrayOptional[FROM, _](
        extractor: FROM => Option[Iterable[String]]
    ): Field[FROM, Option[Iterable[String]], _] =
      PGStringArrayOptional(extractor)

    override def bytea[FROM, _](extractor: FROM => Array[Byte]): Field[FROM, Array[Byte], _] =
      PGBytea(extractor)

    override def byteaOptional[FROM, _](
        extractor: FROM => Option[Array[Byte]]
    ): Field[FROM, Option[Array[Byte]], _] =
      PGByteaOptional(extractor)

    override def bigint[FROM, _](extractor: FROM => Long): Field[FROM, Long, _] =
      PGBigint(extractor)

    override def smallintOptional[FROM, _](
        extractor: FROM => Option[Int]
    ): Field[FROM, Option[Int], _] =
      PGSmallintOptional(extractor)

    override def timestamp[FROM, _](extractor: FROM => Instant): Field[FROM, Instant, _] =
      PGTimestamp(extractor)

    override def timestampOptional[FROM, _](
        extractor: FROM => Option[Instant]
    ): Field[FROM, Option[Instant], _] =
      PGTimestampOptional(extractor)

    override def intOptional[FROM, _](extractor: FROM => Option[Int]): Field[FROM, Option[Int], _] =
      PGIntOptional(extractor)

    override def boolean[FROM, _](extractor: FROM => Boolean): Field[FROM, Boolean, _] =
      PGBoolean(extractor)

    override def booleanOptional[FROM, _](
        extractor: FROM => Option[Boolean]
    ): Field[FROM, Option[Boolean], _] =
      PGBooleanOptional(extractor)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(PGFieldStrategy)
}
