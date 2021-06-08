// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.io.InputStream

import com.daml.lf.data.Ref
import com.daml.platform.store.backend.StorageBackend.RawTransactionEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

object TransactionLogUpdatesReader {

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

  private implicit class MandatoryField[T](val opt: Option[T]) extends AnyVal {
    def mandatory(fieldName: String): T = opt.getOrElse(throw FieldMissingError(fieldName))
  }
}
