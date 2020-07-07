// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.NamedParameter

import com.daml.ledger.EventId
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.{ApplicationId, CommandId, TransactionId, WorkflowId}
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

  // this unfortunate upper bound is here to get access to
  // [[Event.Specific#applySerialization]]; we would do away with it if
  // [[PartialParameters#applySerialization]] was statically determined
  final case class Event[+Specific <: Event.Specific](
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
      specific: Specific,
  ) extends PartialParameters {
    private[this] val eventId = EventId(transactionId, nodeId)
    private[this] val base: Vector[NamedParameter] =
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

    override def applySerialization(
        lfValueTranslation: LfValueTranslation): Vector[NamedParameter] =
      base ++ specific.applySerialization(transactionId, eventId, lfValueTranslation)
  }

  object Event {
    sealed abstract class Specific {
      private[Event] def applySerialization(
          transactionId: TransactionId,
          eventId: EventId,
          lfValueTranslation: LfValueTranslation,
      ): Vector[NamedParameter]
    }

    final class Created(
        create: Create,
    ) extends Specific {
      private val partial: Vector[NamedParameter] =
        Vector[NamedParameter](
          "contract_id" -> create.coid.coid,
          "template_id" -> create.coinst.template,
          "create_signatories" -> create.signatories.toArray[String],
          "create_observers" -> create.stakeholders.diff(create.signatories).toArray[String],
          "create_agreement_text" -> Some(create.coinst.agreementText).filter(_.nonEmpty),
          // set exercise event columns to NULL
          "exercise_consuming" -> Option.empty[Boolean],
          "exercise_choice" -> Option.empty[String],
          "exercise_argument" -> Option.empty[Array[Byte]],
          "exercise_result" -> Option.empty[Array[Byte]],
          "exercise_actors" -> Option.empty[Array[String]],
          "exercise_child_event_ids" -> Option.empty[Array[String]],
        )

      override private[Event] def applySerialization(
          transactionId: TransactionId,
          eventId: EventId,
          lfValueTranslation: LfValueTranslation,
      ): Vector[NamedParameter] =
        partial ++ lfValueTranslation.serialize(eventId, create)
    }

    final class Exercised(
        exercise: Exercise,
    ) extends Specific {
      private val partial: Vector[NamedParameter] =
        Vector[NamedParameter](
          "contract_id" -> exercise.targetCoid,
          "template_id" -> exercise.templateId,
          "exercise_consuming" -> exercise.consuming,
          "exercise_choice" -> exercise.choiceId,
          "exercise_actors" -> exercise.actingParties.toArray[String],
          // set create event columns to NULL
          "create_argument" -> Option.empty[Array[Byte]],
          "create_signatories" -> Option.empty[Array[String]],
          "create_observers" -> Option.empty[Array[String]],
          "create_agreement_text" -> Option.empty[String],
          "create_key_value" -> Option.empty[Array[Byte]],
        )
      override private[Event] def applySerialization(
          transactionId: TransactionId,
          eventId: EventId,
          lfValueTranslation: LfValueTranslation,
      ): Vector[NamedParameter] =
        (partial :+ ("exercise_child_event_ids" -> exercise.children
          .map(EventId(transactionId, _).toLedgerString)
          .toArray[String]: NamedParameter)) ++ lfValueTranslation.serialize(eventId, exercise)
    }
  }

  private implicit lazy val stringArrayParameterMetadata: anorm.ParameterMetaData[Array[String]] =
    new anorm.ParameterMetaData[Array[String]] {
      override def sqlType: String = "ARRAY"

      override def jdbcType: Int = java.sql.Types.ARRAY
    }
}
