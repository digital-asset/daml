// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.io.ByteArrayInputStream
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.serialization.{Compression, ValueSerializer}
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.backend.ContractStorageBackend.RawContractStateEvent

import scala.util.control.NoStackTrace

object ContractStateEventsReader {

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
      createArgument: Array[Byte],
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
      maybeCreateKeyValue: Option[Array[Byte]],
      maybeCreateKeyValueCompression: Option[Int],
  ): Option[Key] =
    for {
      createKeyValue <- maybeCreateKeyValue
      createKeyValueCompression = Compression.Algorithm.assertLookup(
        maybeCreateKeyValueCompression
      )
      keyValue = decompressAndDeserialize(createKeyValueCompression, createKeyValue)
    } yield Key.assertBuild(templateId, keyValue.unversioned)

  private def decompressAndDeserialize(algorithm: Compression.Algorithm, value: Array[Byte]) =
    ValueSerializer.deserializeValue(algorithm.decompress(new ByteArrayInputStream(value)))

  case class CreateMissingError(field: String) extends NoStackTrace {
    override def getMessage: String =
      s"Create events should not be missing $field"
  }

  case class InvalidEventKind(eventKind: Int) extends NoStackTrace {
    override def getMessage: String =
      s"Invalid event kind: $eventKind"
  }

  private object EventKind {
    val Create = 10
    val ConsumingExercise = 20
  }
}
