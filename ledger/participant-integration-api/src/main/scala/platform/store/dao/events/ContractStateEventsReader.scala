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
import com.daml.platform.store.dao.events
import com.daml.platform.store.dao.events.ContractStateEventsReader.ContractStateEvent.{
  Archived,
  Created,
}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf

import scala.util.control.NoStackTrace

object ContractStateEventsReader {

  val contractStateRowParser: RowParser[RawContractStateEvent] =
    (contractId("contract_id") ~
      identifier("template_id").? ~
      binaryStream("create_key_value").? ~
      int("create_key_value_compression").? ~
      binaryStream("create_argument").? ~
      int("create_argument_compression").? ~
      long("event_sequential_id") ~
      flatEventWitnessesColumn("flat_event_witnesses") ~
      offset("event_offset")).map {
      case contractId ~ templateId ~ createKeyValue ~ createKeyCompression ~ createArgument ~ createArgumentCompression ~ eventSequentialId ~ flatEventWitnesses ~ offset =>
        RawContractStateEvent(
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
    createsAndArchives(EventsRange(range.startExclusive._2, range.endInclusive._2), "ASC")
      .asVectorOf(contractStateRowParser)

  def toContractStateEvent(
      raw: RawContractStateEvent,
      lfValueTranslation: LfValueTranslation,
  ): ContractStateEvent = {
    // Events are differentiated basing on the assumption that only `create` or `archived` events are considered.
    // `maybeCreateArgument` can only be defined if an event is `create` - otherwise the event must be `archived`
    // See the definition of `RawContractEvent`
    if (raw.createArgument.isEmpty) {
      Archived(
        contractId = raw.contractId,
        stakeholders = raw.flatEventWitnesses,
        eventOffset = raw.offset,
        eventSequentialId = raw.eventSequentialId,
      )
    } else {
      val (maybeGlobalKey: Option[GlobalKey], contract: Contract) = globalKeyAndContract(
        raw.contractId,
        raw.templateId.getOrElse(throw CreateMissingError("template_id")),
        raw.createKeyValue,
        raw.createKeyCompression,
        raw.createArgument.getOrElse(throw CreateMissingError("create_argument")),
        raw.createArgumentCompression,
        lfValueTranslation,
      )
      Created(
        contractId = raw.contractId,
        contract = contract,
        globalKey =
          maybeGlobalKey, // KTODO: Look into using the retrieving the cached value for global key
        stakeholders = raw.flatEventWitnesses,
        eventOffset = raw.offset,
        eventSequentialId = raw.eventSequentialId,
      )
    }
  }

  private def globalKeyAndContract(
      contractId: ContractId,
      templateId: events.Identifier,
      maybeCreateKeyValue: Option[InputStream],
      maybeCreateKeyValueCompression: Option[Int],
      createArgument: InputStream,
      maybeCreateArgumentCompression: Option[Int],
      lfValueTranslation: LfValueTranslation,
  ): (Option[Key], Contract) = {
    val maybeContractArgument =
      lfValueTranslation.cache.contracts
        .getIfPresent(LfValueTranslation.ContractCache.Key(contractId))
        .map(_.argument)
        .toRight(createArgument) // KTODO: ???

    val maybeGlobalKey =
      for {
        createKeyValue <- maybeCreateKeyValue
        createKeyValueCompression = Compression.Algorithm.assertLookup(
          maybeCreateKeyValueCompression
        )
        keyValue = decompressAndDeserialize(createKeyValueCompression, createKeyValue)
      } yield GlobalKey.assertBuild(templateId, keyValue.value)
    val contract =
      toContract( // KTODO: Do not eagerly decode contract here (do it only for creates since it will only be needed for divulgence for deletes)
        templateId,
        createArgument = maybeContractArgument,
        createArgumentCompression =
          Compression.Algorithm.assertLookup(maybeCreateArgumentCompression),
      )
    (maybeGlobalKey, contract)
  }

  private[store] def toContract(
      templateId: Identifier,
      createArgument: Either[InputStream, events.Value],
      createArgumentCompression: Compression.Algorithm,
  ): Contract = {
    val deserialized =
      createArgument.fold(decompressAndDeserialize(createArgumentCompression, _), identity)

    Contract(
      template = templateId,
      arg = deserialized,
      agreementText = "",
    )
  }

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: InputStream) =
    ValueSerializer.deserializeValue(algorithm.decompress(value))

  private val createsAndArchives: (EventsRange[Long], String) => SimpleSql[Row] =
    (range: EventsRange[Long], limitExpr: String) => SQL"""
              SELECT
                contract_id,
                template_id,
                create_key_value,
                create_key_value_compression,
                create_argument,
                create_argument_compression,
                flat_event_witnesses,
                event_sequential_id,
                event_offset
              FROM participant_events
              WHERE event_sequential_id > ${range.startExclusive}
                    and event_sequential_id <= ${range.endInclusive}
                    and ((create_argument IS NOT NULL) -- created
                      OR (exercise_consuming IS TRUE)) -- archived
              ORDER BY event_sequential_id #$limitExpr"""

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

  private[events] case class RawContractStateEvent(
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
