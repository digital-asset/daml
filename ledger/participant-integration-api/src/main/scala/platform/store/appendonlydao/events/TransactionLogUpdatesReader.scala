// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import anorm.SqlParser._
import anorm._
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.lf.ledger.EventId
import com.daml.platform
import com.daml.platform.store.Conversions.{contractId, eventId, offset, _}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

object TransactionLogUpdatesReader {
  private val createsAndArchives: EventsRange[Long] => SimpleSql[Row] =
    (range: EventsRange[Long]) => SQL"""
                                         SELECT
                                             event_kind,
                                             transaction_id,
                                             node_index,
                                             command_id,
                                             workflow_id,
                                             event_id,
                                             contract_id,
                                             template_id,
                                             ledger_effective_time,
                                             create_signatories,
                                             create_observers,
                                             create_agreement_text,
                                             create_key_value,
                                             create_key_value_compression,
                                             create_argument,
                                             create_argument_compression,
                                             tree_event_witnesses,
                                             flat_event_witnesses,
                                             exercise_choice,
                                             exercise_argument,
                                             exercise_argument_compression,
                                             exercise_result,
                                             exercise_result_compression,
                                             exercise_actors,
                                             exercise_child_event_ids,
                                             event_sequential_id,
                                             event_offset
                                         FROM
                                             participant_events
                                         WHERE
                                             event_sequential_id > #${range.startExclusive}
                                             and event_sequential_id <= #${range.endInclusive}
                                             and event_kind != 0
                                         ORDER BY event_sequential_id ASC
                                         """

  private val rawTransactionEventParser: RowParser[RawTransactionEvent] =
    (int("event_kind") ~
      str("transaction_id") ~
      int("node_index") ~
      str("command_id").? ~
      str("workflow_id").? ~
      eventId("event_id") ~
      contractId("contract_id") ~
      identifier("template_id").? ~
      instant("ledger_effective_time").? ~
      array[String]("create_signatories").? ~
      array[String]("create_observers").? ~
      str("create_agreement_text").? ~
      binaryStream("create_key_value").? ~
      int("create_key_value_compression").? ~
      binaryStream("create_argument").? ~
      int("create_argument_compression").? ~
      array[String]("tree_event_witnesses") ~
      array[String]("flat_event_witnesses") ~
      str("exercise_choice").? ~
      binaryStream("exercise_argument").? ~
      int("exercise_argument_compression").? ~
      binaryStream("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[String]("exercise_actors").? ~
      array[String]("exercise_child_event_ids").? ~
      long("event_sequential_id") ~
      offset("event_offset")).map {
      case eventKind ~ transactionId ~ nodeIndex ~ commandId ~ workflowId ~ eventId ~ contractId ~ templateId ~ ledgerEffectiveTime ~ createSignatories ~
          createObservers ~ createAgreementText ~ createKeyValue ~ createKeyCompression ~
          createArgument ~ createArgumentCompression ~ treeEventWitnesses ~ flatEventWitnesses ~ exerciseChoice ~
          exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~
          exerciseChildEventIds ~ eventSequentialId ~ offset =>
        new RawTransactionEvent(
          eventKind,
          transactionId,
          nodeIndex,
          commandId,
          workflowId,
          eventId,
          contractId,
          templateId,
          ledgerEffectiveTime,
          createSignatories,
          createObservers,
          createAgreementText,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          treeEventWitnesses.toSet,
          flatEventWitnesses.toSet,
          exerciseChoice,
          exerciseArgument,
          exerciseArgumentCompression,
          exerciseResult,
          exerciseResultCompression,
          exerciseActors,
          exerciseChildEventIds,
          eventSequentialId,
          offset,
        )
    }

  /** This method intentionally produces a generic DTO to perform as much work as possible outside of the db thread pool
    * (specifically the translation to the `TransactionLogUpdate.Event`)
    */
  def readRawEvents(range: EventsRange[Long])(implicit
      conn: Connection
  ): Vector[RawTransactionEvent] =
    createsAndArchives(EventsRange(range.startExclusive, range.endInclusive))
      .asVectorOf(rawTransactionEventParser)

  def toTransactionEvent(
      raw: RawTransactionEvent
  ): TransactionLogUpdate.Event =
    raw.eventKind match {
      case EventKind.NonConsumingExercise | EventKind.ConsumingExercise =>
        TransactionLogUpdate.ExercisedEvent(
          eventOffset = raw.offset,
          transactionId = raw.transactionId,
          nodeIndex = raw.nodeIndex,
          eventSequentialId = raw.eventSequentialId,
          eventId = raw.eventId,
          contractId = raw.contractId,
          ledgerEffectiveTime = raw.ledgerEffectiveTime
            .mandatory("ledgerEffectiveTime"),
          templateId = raw.templateId.mandatory("template_id"),
          commandId = raw.commandId.getOrElse(""),
          workflowId = raw.workflowId.getOrElse(""),
          contractKey = raw.createKeyValue.map(
            decompressAndDeserialize(
              Compression.Algorithm.assertLookup(raw.createKeyCompression),
              _,
            )
          ),
          treeEventWitnesses = raw.treeEventWitnesses,
          flatEventWitnesses = raw.flatEventWitnesses,
          choice = raw.exerciseChoice.mandatory("exercise_choice"),
          actingParties = raw.exerciseActors
            .mandatory("exercise_actors")
            .iterator
            .map(Ref.Party.assertFromString)
            .toSet,
          children = raw.exerciseChildEventIds
            .mandatory("exercise_child_events_ids")
            .toSeq,
          exerciseArgument = ValueSerializer.deserializeValue(
            Compression.Algorithm
              .assertLookup(raw.exerciseArgumentCompression)
              .decompress(
                raw.exerciseArgument.mandatory("exercise_argument")
              )
          ),
          exerciseResult = raw.exerciseResult.map { inputStream =>
            ValueSerializer.deserializeValue(
              Compression.Algorithm
                .assertLookup(raw.exerciseResultCompression)
                .decompress(inputStream)
            )
          },
          consuming = raw.eventKind == EventKind.ConsumingExercise,
        )
      case EventKind.Create =>
        val createArgument =
          raw.createArgument.mandatory("create_argument")
        val maybeGlobalKey =
          raw.createKeyValue.map(
            decompressAndDeserialize(
              Compression.Algorithm.assertLookup(raw.createKeyCompression),
              _,
            )
          )

        val createArgumentDecompressed = decompressAndDeserialize(
          Compression.Algorithm.assertLookup(raw.createArgumentCompression),
          createArgument,
        )

        TransactionLogUpdate.CreatedEvent(
          eventOffset = raw.offset,
          transactionId = raw.transactionId,
          nodeIndex = raw.nodeIndex,
          eventSequentialId = raw.eventSequentialId,
          eventId = raw.eventId,
          contractId = raw.contractId,
          ledgerEffectiveTime = raw.ledgerEffectiveTime.mandatory("ledgerEffectiveTime"),
          templateId = raw.templateId.mandatory("template_id"),
          commandId = raw.commandId.getOrElse(""),
          workflowId = raw.workflowId.getOrElse(""),
          contractKey = maybeGlobalKey,
          treeEventWitnesses = raw.treeEventWitnesses,
          flatEventWitnesses = raw.flatEventWitnesses,
          createArgument = createArgumentDecompressed,
          createSignatories = raw.createSignatories.mandatory("create_signatories").toSet,
          createObservers = raw.createObservers.mandatory("create_observers").toSet,
          createAgreementText = raw.createAgreementText,
        )
      case unknownKind =>
        throw InvalidEventKind(unknownKind)
    }

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: InputStream) =
    ValueSerializer.deserializeValue(algorithm.decompress(value))

  final case class FieldMissingError(field: String) extends RuntimeException {
    override def getMessage: String = s"Missing mandatory field $field"
  }

  final case class InvalidEventKind(eventKind: Int) extends RuntimeException {
    override def getMessage: String =
      s"Invalid event kind: $eventKind"
  }

  private object EventKind {
    val Create = 10
    val ConsumingExercise = 20
    val NonConsumingExercise = 25
  }

  class RawTransactionEvent(
      val eventKind: Int,
      val transactionId: String,
      val nodeIndex: Int,
      val commandId: Option[String],
      val workflowId: Option[String],
      val eventId: EventId,
      val contractId: platform.store.appendonlydao.events.ContractId,
      val templateId: Option[platform.store.appendonlydao.events.Identifier],
      val ledgerEffectiveTime: Option[Instant],
      val createSignatories: Option[Array[String]],
      val createObservers: Option[Array[String]],
      val createAgreementText: Option[String],
      val createKeyValue: Option[InputStream],
      val createKeyCompression: Option[Int],
      val createArgument: Option[InputStream],
      val createArgumentCompression: Option[Int],
      val treeEventWitnesses: Set[String],
      val flatEventWitnesses: Set[String],
      val exerciseChoice: Option[String],
      val exerciseArgument: Option[InputStream],
      val exerciseArgumentCompression: Option[Int],
      val exerciseResult: Option[InputStream],
      val exerciseResultCompression: Option[Int],
      val exerciseActors: Option[Array[String]],
      val exerciseChildEventIds: Option[Array[String]],
      val eventSequentialId: Long,
      val offset: Offset,
  )

  private implicit class MandatoryField[T](val opt: Option[T]) extends AnyVal {
    def mandatory(fieldName: String): T = opt.getOrElse(throw FieldMissingError(fieldName))
  }
}
