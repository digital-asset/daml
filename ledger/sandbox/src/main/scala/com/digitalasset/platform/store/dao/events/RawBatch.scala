// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.NamedParameter
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.{ApplicationId, CommandId, TransactionId, WorkflowId}
import com.daml.platform.events.EventIdFormatter.fromTransactionId
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.RawBatch.PartialParameters
import com.daml.platform.store.serialization.ValueSerializer

private[events] final class RawBatch(query: String, parameters: Vector[PartialParameters]) {
  def applySerialization(): Vector[Vector[NamedParameter]] =
    parameters.map(_.applySerialization())
}

private[events] object RawBatch {

  sealed abstract class PartialParameters {
    def applySerialization(): Vector[NamedParameter]
  }

  final class Contract(
      contractId: ContractId,
      templateId: Identifier,
      createArgument: Value,
      createLedgerEffectiveTime: Option[Instant],
      stakeholders: Set[Party],
      key: Option[Key],
  ) extends PartialParameters {

    private val partial =
      Vector[NamedParameter](
        "contract_id" -> contractId,
        "template_id" -> templateId,
        "create_ledger_effective_time" -> createLedgerEffectiveTime,
        "create_stakeholders" -> stakeholders.toArray[String],
        "create_key_hash" -> key.map(_.hash),
      )

    override def applySerialization(): Vector[NamedParameter] =
      partial :+
        ("create_argument" -> ValueSerializer.serializeValue(
          value = createArgument,
          errorContext = s"Cannot serialize create argument for ${contractId.coid}",
        ): NamedParameter)
  }

  sealed abstract class Event(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      commandId: Option[CommandId],
      transactionId: TransactionId,
      nodeId: NodeId,
      submitter: Option[Party],
      ledgerEffectiveTime: Instant,
      offset: Offset,
  ) extends PartialParameters {
    final protected val base: Vector[NamedParameter] =
      Vector[NamedParameter](
        "event_id" -> fromTransactionId(transactionId, nodeId),
        "event_offset" -> offset,
        "transaction_id" -> transactionId,
        "workflow_id" -> workflowId,
        "ledger_effective_time" -> ledgerEffectiveTime,
        "node_index" -> nodeId.index,
        "command_id" -> commandId,
        "application_id" -> applicationId,
        "submitter" -> submitter,
      )
  }

  object Event {

    final class Created(
        applicationId: Option[ApplicationId],
        workflowId: Option[WorkflowId],
        commandId: Option[CommandId],
        transactionId: TransactionId,
        nodeId: NodeId,
        submitter: Option[Party],
        ledgerEffectiveTime: Instant,
        offset: Offset,
        create: Create,
    ) extends Event(
          applicationId = applicationId,
          workflowId = workflowId,
          commandId = commandId,
          transactionId = transactionId,
          nodeId = nodeId,
          submitter = submitter,
          ledgerEffectiveTime = ledgerEffectiveTime,
          offset = offset,
        ) {
      val partial: Vector[NamedParameter] =
        base ++ Vector[NamedParameter](
          "contract_id" -> create.coid.coid,
          "template_id" -> create.coinst.template,
          "create_signatories" -> create.signatories.toArray[String],
          "create_observers" -> create.stakeholders.diff(create.signatories).toArray[String],
          "create_agreement_text" -> Some(create.coinst.agreementText).filter(_.nonEmpty),
        )
      override def applySerialization(): Vector[NamedParameter] =
        partial ++ Vector[NamedParameter](
          "create_argument" -> serializeCreateArgOrThrow(create),
          "create_key_value" -> serializeNullableKeyOrThrow(create),
        )
    }

    final class Exercised(
        applicationId: Option[ApplicationId],
        workflowId: Option[WorkflowId],
        commandId: Option[CommandId],
        transactionId: TransactionId,
        nodeId: NodeId,
        submitter: Option[Party],
        ledgerEffectiveTime: Instant,
        offset: Offset,
        exercise: Exercise,
    ) extends Event(
          applicationId = applicationId,
          workflowId = workflowId,
          commandId = commandId,
          transactionId = transactionId,
          nodeId = nodeId,
          submitter = submitter,
          ledgerEffectiveTime = ledgerEffectiveTime,
          offset = offset,
        ) {
      val partial: Vector[NamedParameter] =
        base ++ Vector[NamedParameter](
          "contract_id" -> exercise.targetCoid,
          "template_id" -> exercise.templateId,
          "exercise_consuming" -> exercise.consuming,
          "exercise_choice" -> exercise.choiceId,
          "exercise_actors" -> exercise.actingParties.toArray[String],
          "exercise_child_event_ids" -> exercise.children
            .map(fromTransactionId(transactionId, _))
            .toArray[String],
        )
      override def applySerialization(): Vector[NamedParameter] =
        partial ++ Vector[NamedParameter](
          "exercise_argument" -> serializeExerciseArgOrThrow(exercise),
          "exercise_result" -> serializeNullableExerciseResultOrThrow(exercise),
        )
    }

    private def cantSerialize(attribute: String, forContract: ContractId): String =
      s"Cannot serialize $attribute for ${forContract.coid}"

    private def serializeCreateArgOrThrow(c: Create): Array[Byte] =
      ValueSerializer.serializeValue(
        value = c.coinst.arg,
        errorContext = cantSerialize(attribute = "create argument", forContract = c.coid),
      )

    private def serializeNullableKeyOrThrow(c: Create): Option[Array[Byte]] =
      c.key.map(
        k =>
          ValueSerializer.serializeValue(
            value = k.key,
            errorContext = cantSerialize(attribute = "key", forContract = c.coid),
        )
      )

    private def serializeExerciseArgOrThrow(e: Exercise): Array[Byte] =
      ValueSerializer.serializeValue(
        value = e.chosenValue,
        errorContext = cantSerialize(attribute = "exercise argument", forContract = e.targetCoid),
      )

    private def serializeNullableExerciseResultOrThrow(e: Exercise): Option[Array[Byte]] =
      e.exerciseResult.map(
        exerciseResult =>
          ValueSerializer.serializeValue(
            value = exerciseResult,
            errorContext = cantSerialize(attribute = "exercise result", forContract = e.targetCoid),
        )
      )

  }

}
