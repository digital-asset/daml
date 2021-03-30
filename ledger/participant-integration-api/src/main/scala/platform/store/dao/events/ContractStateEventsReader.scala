// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.sql.Connection

import anorm.SqlParser.{binaryStream, int, long}
import anorm._
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.lf.transaction.GlobalKey
import com.daml.platform.store.Conversions.{contractId, offset, _}
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.dao.events
import com.daml.platform.store.dao.events.ContractStateEventsReader.ContractStateEvent.{
  Archived,
  Created,
}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf

import scala.util.control.NoStackTrace

object ContractStateEventsReader {
  private val EventKindCreated = 10
  private val EventKindArchived = 20

  val contractStateRowParser: RowParser[RawContractStateEvent] =
    (int("event_kind") ~
      contractId("contract_id") ~
      identifier("template_id").? ~
      binaryStream("create_key_value").? ~
      int("create_key_value_compression").? ~
      binaryStream("create_argument").? ~
      int("create_argument_compression").? ~
      long("event_sequential_id") ~
      flatEventWitnessesColumn("flat_event_witnesses") ~
      offset("event_offset")).map {
      case eventKind ~ contractId ~ templateId ~ createKeyValue ~ createKeyCompression ~ createArgument ~ createArgumentCompression ~ eventSequentialId ~ flatEventWitnesses ~ offset =>
        RawContractStateEvent(
          eventKind,
          contractId,
          templateId,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          flatEventWitnesses,
          eventSequentialId,
          offset,
        )
    }

  /*
  This method intentionally produces a generic DTO to perform as much work as possible outside of the db thread pool
  (specifically the translation to the `ContractStateEvent`)
   */
  def readRawEvents(range: EventsRange[(Offset, Long)])(implicit
      conn: Connection
  ): Vector[RawContractStateEvent] =
    createsAndArchives(EventsRange(range.startExclusive._2, range.endInclusive._2))
      .asVectorOf(contractStateRowParser)

  def toContractStateEvent(
      raw: RawContractStateEvent,
      lfValueTranslation: LfValueTranslation,
  ): ContractStateEvent =
    raw.eventKind match {
      case EventKindArchived =>
        Archived(
          contractId = raw.contractId,
          stakeholders = raw.flatEventWitnesses,
          eventOffset = raw.offset,
          eventSequentialId = raw.eventSequentialId,
        )
      case EventKindCreated =>
        val templateId = raw.templateId.getOrElse(throw CreateMissingError("template_id"))
        val createArgument =
          raw.createArgument.getOrElse(throw CreateMissingError("create_argument"))
        val maybeGlobalKey =
          decompressGlobalKey(templateId, raw.createKeyValue, raw.createKeyCompression)
        val contract = getCachedOrDecompressContract(
          raw.contractId,
          templateId,
          createArgument,
          raw.createArgumentCompression,
          lfValueTranslation,
        )
        Created(
          contractId = raw.contractId,
          contract = contract,
          globalKey = maybeGlobalKey,
          stakeholders = raw.flatEventWitnesses,
          eventOffset = raw.offset,
          eventSequentialId = raw.eventSequentialId,
        )
      case unknownKind =>
        throw InvalidEventKind(unknownKind)
    }

  private def cachedContractValue(
      contractId: ContractId,
      lfValueTranslation: LfValueTranslation,
  ): Option[LfValueTranslationCache.ContractCache.Value] =
    lfValueTranslation.cache.contracts.getIfPresent(
      LfValueTranslationCache.ContractCache.Key(contractId)
    )

  private def getCachedOrDecompressContract(
      contractId: ContractId,
      templateId: events.Identifier,
      createArgument: InputStream,
      maybeCreateArgumentCompression: Option[Int],
      lfValueTranslation: LfValueTranslation,
  ): Contract = {
    val createArgumentCompression =
      Compression.Algorithm.assertLookup(maybeCreateArgumentCompression)
    val deserializedCreateArgument = cachedContractValue(contractId, lfValueTranslation)
      .map(_.argument)
      .getOrElse(decompressAndDeserialize(createArgumentCompression, createArgument))

    Contract(
      template = templateId,
      arg = deserializedCreateArgument,
      agreementText = "",
    )
  }

  private def decompressGlobalKey(
      templateId: events.Identifier,
      maybeCreateKeyValue: Option[InputStream],
      maybeCreateKeyValueCompression: Option[Int],
  ): Option[Key] =
    for {
      createKeyValue <- maybeCreateKeyValue
      createKeyValueCompression = Compression.Algorithm.assertLookup(
        maybeCreateKeyValueCompression
      )
      keyValue = decompressAndDeserialize(createKeyValueCompression, createKeyValue)
    } yield GlobalKey.assertBuild(templateId, keyValue.value)

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: InputStream) =
    ValueSerializer.deserializeValue(algorithm.decompress(value))

  // TODO use participant_events.event_kind once it lands with the append-only schema
  private val eventKindQueryClause =
    """CASE
      |    WHEN create_argument IS NOT NULL
      |        THEN 10
      |    WHEN exercise_consuming IS NOT NULL AND exercise_consuming = true
      |        THEN 20
      |    END event_kind
      |""".stripMargin
  // TODO use participant_events.event_kind once it lands with the append-only schema
  private val isCreateQueryPredicate = "create_argument IS NOT NULL"
  // TODO use participant_events.event_kind once it lands with the append-only schema
  private val isConsumingExercisePredicate =
    "exercise_consuming IS TRUE"

  private val createsAndArchives: EventsRange[Long] => SimpleSql[Row] =
    (range: EventsRange[Long]) => SQL(s"""
           |SELECT
           |    $eventKindQueryClause,
           |    contract_id,
           |    template_id,
           |    create_key_value,
           |    create_key_value_compression,
           |    create_argument,
           |    create_argument_compression,
           |    flat_event_witnesses,
           |    event_sequential_id,
           |    event_offset
           |FROM
           |    participant_events
           |WHERE
           |    event_sequential_id > ${range.startExclusive}
           |    and event_sequential_id <= ${range.endInclusive}
           |    and ($isCreateQueryPredicate or $isConsumingExercisePredicate)
           |ORDER BY event_sequential_id ASC
           |""".stripMargin)

  sealed trait ContractStateEvent extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: Long
  }
  object ContractStateEvent {
    final case class Created(
        contractId: ContractId,
        contract: Contract,
        globalKey: Option[GlobalKey],
        stakeholders: Set[Party],
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
    final case class Archived(
        contractId: ContractId,
        stakeholders: Set[Party],
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
    final case class LedgerEndMarker(
        eventOffset: Offset,
        eventSequentialId: Long,
    ) extends ContractStateEvent
  }

  case class CreateMissingError(field: String) extends NoStackTrace {
    override def getMessage: String =
      s"Create events should not be missing $field"
  }

  case class InvalidEventKind(eventKind: Int) extends NoStackTrace {
    override def getMessage: String =
      s"Invalid event kind: $eventKind"
  }

  private[events] case class RawContractStateEvent(
      eventKind: Int,
      contractId: ContractId,
      templateId: Option[Ref.Identifier],
      createKeyValue: Option[InputStream],
      createKeyCompression: Option[Int],
      createArgument: Option[InputStream],
      createArgumentCompression: Option[Int],
      flatEventWitnesses: Set[Party],
      eventSequentialId: Long,
      offset: Offset,
  )

}
