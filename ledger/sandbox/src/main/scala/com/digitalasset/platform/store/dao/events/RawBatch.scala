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

private[events] final class RawBatch(query: String, parameters: Vector[PartialParameters]) {
  def applySerialization(
      lfValueTranslation: LfValueTranslation,
  ): Vector[Vector[NamedParameter]] =
    parameters.map(_.applySerialization(lfValueTranslation))
}

private[events] object RawBatch {

  sealed abstract class PartialParameters {
    def applySerialization(
        lfValueTranslation: LfValueTranslation,
    ): Vector[NamedParameter]
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

    override def applySerialization(
        lfValueTranslation: LfValueTranslation,
    ): Vector[NamedParameter] =
      partial :+ lfValueTranslation.serialize(contractId, createArgument)
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
      flatWitnesses: Set[Party],
      treeWitnesses: Set[Party],
  ) extends PartialParameters {
    final protected val eventId = fromTransactionId(transactionId, nodeId)
    final protected val base: Vector[NamedParameter] =
      Vector[NamedParameter](
        "event_id" -> eventId,
        "event_offset" -> offset,
        "transaction_id" -> transactionId,
        "workflow_id" -> workflowId,
        "ledger_effective_time" -> ledgerEffectiveTime,
        "node_index" -> nodeId.index,
        "command_id" -> commandId,
        "application_id" -> applicationId,
        "submitter" -> submitter,
        "flat_event_witnesses" -> Party.Array(flatWitnesses.toSeq: _*),
        "tree_event_witnesses" -> Party.Array(treeWitnesses.toSeq: _*),
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
        flatWitnesses: Set[Party],
        treeWitnesses: Set[Party],
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
          flatWitnesses = flatWitnesses,
          treeWitnesses = treeWitnesses,
        ) {
      val partial: Vector[NamedParameter] =
        base ++ Vector[NamedParameter](
          "contract_id" -> create.coid.coid,
          "template_id" -> create.coinst.template,
          "create_signatories" -> create.signatories.toArray[String],
          "create_observers" -> create.stakeholders.diff(create.signatories).toArray[String],
          "create_agreement_text" -> Some(create.coinst.agreementText).filter(_.nonEmpty),
        )
      override def applySerialization(
          lfValueTranslation: LfValueTranslation,
      ): Vector[NamedParameter] =
        partial ++ lfValueTranslation.serialize(eventId, create)
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
        flatWitnesses: Set[Party],
        treeWitnesses: Set[Party],
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
          flatWitnesses = flatWitnesses,
          treeWitnesses = treeWitnesses,
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
      override def applySerialization(
          lfValueTranslation: LfValueTranslation,
      ): Vector[NamedParameter] =
        partial ++ lfValueTranslation.serialize(eventId, exercise)
    }

  }

}
