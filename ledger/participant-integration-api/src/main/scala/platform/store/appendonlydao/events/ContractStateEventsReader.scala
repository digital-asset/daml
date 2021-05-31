// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.{binaryStream, int, long}
import anorm._
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.Conversions.{contractId, offset, _}
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.serialization.{Compression, ValueSerializer}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.LfValueTranslationCache

import scala.util.control.NoStackTrace

object ContractStateEventsReader {
  val contractStateRowParser: RowParser[RawContractStateEvent] =
    (int("event_kind") ~
      contractId("contract_id") ~
      identifier("template_id").? ~
      instant("ledger_effective_time").? ~
      binaryStream("create_key_value").? ~
      int("create_key_value_compression").? ~
      binaryStream("create_argument").? ~
      int("create_argument_compression").? ~
      long("event_sequential_id") ~
      flatEventWitnessesColumn("flat_event_witnesses") ~
      offset("event_offset")).map {
      case eventKind ~ contractId ~ templateId ~ ledgerEffectiveTime ~ createKeyValue ~ createKeyCompression ~ createArgument ~ createArgumentCompression ~ eventSequentialId ~ flatEventWitnesses ~ offset =>
        RawContractStateEvent(
          eventKind,
          contractId,
          templateId,
          ledgerEffectiveTime,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          flatEventWitnesses,
          eventSequentialId,
          offset,
        )
    }

  /** This method intentionally produces a generic DTO to perform as much work as possible outside of the db thread pool
    * (specifically the translation to the `ContractStateEvent`)
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
      case EventKind.ConsumingExercise =>
        val templateId = raw.templateId.getOrElse(throw CreateMissingError("template_id"))
        val maybeGlobalKey =
          decompressKey(templateId, raw.createKeyValue, raw.createKeyCompression)
        ContractStateEvent.Archived(
          contractId = raw.contractId,
          globalKey = maybeGlobalKey,
          stakeholders = raw.flatEventWitnesses,
          eventOffset = raw.offset,
          eventSequentialId = raw.eventSequentialId,
        )
      case EventKind.Create =>
        val templateId = raw.templateId.getOrElse(throw CreateMissingError("template_id"))
        val createArgument =
          raw.createArgument.getOrElse(throw CreateMissingError("create_argument"))
        val maybeGlobalKey =
          decompressKey(templateId, raw.createKeyValue, raw.createKeyCompression)
        val contract = getCachedOrDecompressContract(
          raw.contractId,
          templateId,
          createArgument,
          raw.createArgumentCompression,
          lfValueTranslation,
        )
        ContractStateEvent.Created(
          contractId = raw.contractId,
          contract = contract,
          globalKey = maybeGlobalKey,
          ledgerEffectiveTime =
            raw.ledgerEffectiveTime.getOrElse(throw CreateMissingError("ledger_effective_time")),
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

  private def decompressKey(
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
    } yield Key.assertBuild(templateId, keyValue.value)

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: InputStream) =
    ValueSerializer.deserializeValue(algorithm.decompress(value))

  private val createsAndArchives: EventsRange[Long] => SimpleSql[Row] =
    (range: EventsRange[Long]) => SQL(s"""
                                         |SELECT
                                         |    event_kind,
                                         |    contract_id,
                                         |    template_id,
                                         |    create_key_value,
                                         |    create_key_value_compression,
                                         |    create_argument,
                                         |    create_argument_compression,
                                         |    flat_event_witnesses,
                                         |    ledger_effective_time,
                                         |    event_sequential_id,
                                         |    event_offset
                                         |FROM
                                         |    participant_events
                                         |WHERE
                                         |    event_sequential_id > ${range.startExclusive}
                                         |    and event_sequential_id <= ${range.endInclusive}
                                         |    and (event_kind = 10 or event_kind = 20)
                                         |ORDER BY event_sequential_id ASC
                                         |""".stripMargin)

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
      ledgerEffectiveTime: Option[Instant],
      createKeyValue: Option[InputStream],
      createKeyCompression: Option[Int],
      createArgument: Option[InputStream],
      createArgumentCompression: Option[Int],
      flatEventWitnesses: Set[Party],
      eventSequentialId: Long,
      offset: Offset,
  )

  private object EventKind {
    val Create = 10
    val ConsumingExercise = 20
  }
}
