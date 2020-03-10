// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import java.util.Date

import anorm.{BatchSql, NamedParameter}
import com.digitalasset.ledger.{ApplicationId, CommandId, TransactionId, WorkflowId}
import com.digitalasset.platform.events.EventIdFormatter.fromTransactionId
import com.digitalasset.platform.store.dao.LedgerDao
import com.digitalasset.platform.store.serialization.ValueSerializer.{serializeValue => serialize}
import com.digitalasset.platform.store.Conversions._

/**
  * Data access object for a table representing raw transactions nodes that
  * are going to be streamed off through the Ledger API. By joining these items
  * with a [[WitnessesTable]] events can be filtered based on their visibility to
  * a party.
  */
private[events] object EventsTable {

  private def cantSerialize(attribute: String, forContract: ContractId): String =
    s"Cannot serialize $attribute for ${forContract.coid}"

  private def serializeCreateArgOrThrow(node: Create): Array[Byte] =
    serialize(
      value = node.coinst.arg,
      errorContext = cantSerialize(attribute = "create argument", forContract = node.coid),
    )

  private def serializeNullableKeyOrThrow(node: Create): Option[Array[Byte]] =
    node.key.map(
      k =>
        serialize(
          value = k.key,
          errorContext = cantSerialize(attribute = "key", forContract = node.coid),
      ))

  private def serializeExerciseArgOrThrow(node: Exercise): Array[Byte] =
    serialize(
      value = node.chosenValue,
      errorContext = cantSerialize(attribute = "exercise argument", forContract = node.targetCoid),
    )

  private def serializeNullableExerciseResultOrThrow(node: Exercise): Option[Array[Byte]] =
    node.exerciseResult.map(exerciseResult =>
      serialize(
        value = exerciseResult,
        errorContext = cantSerialize(attribute = "exercise result", forContract = node.targetCoid),
    ))

  private val insertCreate: String =
    """insert into participant_events(
          |  event_id,
          |  event_offset,
          |  contract_id,
          |  transaction_id,
          |  workflow_id,
          |  ledger_effective_time,
          |  template_package_id,
          |  template_name,
          |  node_index,
          |  is_root,
          |  command_id,
          |  application_id,
          |  submitter,
          |  create_argument,
          |  create_signatories,
          |  create_observers,
          |  create_agreement_text,
          |  create_consumed_at,
          |  create_key_value
          |) values (
          |  {event_id},
          |  {event_offset},
          |  {contract_id},
          |  {transaction_id},
          |  {workflow_id},
          |  {ledger_effective_time},
          |  {template_package_id},
          |  {template_name},
          |  {node_index},
          |  {is_root},
          |  {command_id},
          |  {application_id},
          |  {submitter},
          |  {create_argument},
          |  {create_signatories},
          |  {create_observers},
          |  {create_agreement_text},
          |  null,
          |  {create_key_value}
          |)
          |""".stripMargin

  private def create(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      commandId: Option[CommandId],
      transactionId: TransactionId,
      nodeId: NodeId,
      submitter: Option[Party],
      roots: Set[NodeId],
      ledgerEffectiveTime: Date,
      offset: LedgerDao#LedgerOffset,
      create: Create,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "event_id" -> fromTransactionId(transactionId, nodeId).toString,
      "event_offset" -> offset,
      "contract_id" -> create.coid.coid.toString,
      "transaction_id" -> transactionId.toString,
      "workflow_id" -> workflowId.map(_.toString),
      "ledger_effective_time" -> ledgerEffectiveTime,
      "template_package_id" -> create.coinst.template.packageId.toString,
      "template_name" -> create.coinst.template.qualifiedName.toString,
      "node_index" -> nodeId.index,
      "is_root" -> roots(nodeId),
      "command_id" -> commandId.map(_.toString),
      "application_id" -> applicationId.map(_.toString),
      "submitter" -> submitter.map(_.toString),
      "create_argument" -> serializeCreateArgOrThrow(create),
      "create_signatories" -> create.signatories.map(_.toString).toArray,
      "create_observers" -> create.stakeholders.diff(create.signatories).map(_.toString).toArray,
      "create_agreement_text" -> Some(create.coinst.agreementText).filter(_.nonEmpty),
      "create_key_value" -> serializeNullableKeyOrThrow(create),
    )

  private val insertExercise =
    """insert into participant_events(
          |  event_id,
          |  event_offset,
          |  contract_id,
          |  transaction_id,
          |  workflow_id,
          |  ledger_effective_time,
          |  template_package_id,
          |  template_name,
          |  node_index,
          |  is_root,
          |  command_id,
          |  application_id,
          |  submitter,
          |  exercise_consuming,
          |  exercise_choice,
          |  exercise_argument,
          |  exercise_result,
          |  exercise_actors,
          |  exercise_child_event_ids
          |) values (
          |  {event_id},
          |  {event_offset},
          |  {contract_id},
          |  {transaction_id},
          |  {workflow_id},
          |  {ledger_effective_time},
          |  {template_package_id},
          |  {template_name},
          |  {node_index},
          |  {is_root},
          |  {command_id},
          |  {application_id},
          |  {submitter},
          |  {exercise_consuming},
          |  {exercise_choice},
          |  {exercise_argument},
          |  {exercise_result},
          |  {exercise_actors},
          |  {exercise_child_event_ids}
          |)
          |""".stripMargin

  private def exercise(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      commandId: Option[CommandId],
      transactionId: TransactionId,
      nodeId: NodeId,
      submitter: Option[Party],
      roots: Set[NodeId],
      ledgerEffectiveTime: Date,
      offset: LedgerDao#LedgerOffset,
      exercise: Exercise,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "event_id" -> fromTransactionId(transactionId, nodeId).toString,
      "event_offset" -> offset,
      "contract_id" -> exercise.targetCoid.toString,
      "transaction_id" -> transactionId.toString,
      "workflow_id" -> workflowId.map(_.toString),
      "ledger_effective_time" -> ledgerEffectiveTime,
      "template_package_id" -> exercise.templateId.packageId.toString,
      "template_name" -> exercise.templateId.qualifiedName.toString,
      "node_index" -> nodeId.index,
      "is_root" -> roots(nodeId),
      "command_id" -> commandId.map(_.toString),
      "application_id" -> applicationId.map(_.toString),
      "submitter" -> submitter.map(_.toString),
      "exercise_consuming" -> exercise.consuming,
      "exercise_choice" -> exercise.choiceId.toString,
      "exercise_argument" -> serializeExerciseArgOrThrow(exercise),
      "exercise_result" -> serializeNullableExerciseResultOrThrow(exercise),
      "exercise_actors" -> exercise.actingParties.map(_.toString).toArray,
      "exercise_child_event_ids" -> exercise.children
        .map(fromTransactionId(transactionId, _): String)
        .toArray,
    )

  private val updateArchived =
    """update participant_events set create_consumed_at={consumed_at} where contract_id={contract_id}"""

  private def archive(
      contractId: ContractId,
      consumedAt: LedgerDao#LedgerOffset,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "consumed_at" -> consumedAt,
      "contract_id" -> contractId.coid.toString,
    )

  sealed abstract case class PreparedBatches(
      creates: Option[BatchSql],
      exercises: Option[BatchSql],
      archives: Option[BatchSql],
  ) {
    final def isEmpty: Boolean = creates.isEmpty && exercises.isEmpty && archives.isEmpty
    final def foreach[U](f: BatchSql => U): Unit = {
      creates.foreach(f)
      exercises.foreach(f)
      archives.foreach(f)
    }
  }

  private final case class AccumulatingBatches(
      creates: Vector[Vector[NamedParameter]],
      exercises: Vector[Vector[NamedParameter]],
      archives: Vector[Vector[NamedParameter]],
  ) {

    def addCreate(create: Vector[NamedParameter]): AccumulatingBatches =
      copy(creates = creates :+ create)

    def addExercise(exercise: Vector[NamedParameter]): AccumulatingBatches =
      copy(exercises = exercises :+ exercise)

    def addArchive(archive: Vector[NamedParameter]): AccumulatingBatches =
      copy(archives = archives :+ archive)

    private def prepareNonEmpty(
        query: String,
        params: Vector[Vector[NamedParameter]],
    ): Option[BatchSql] =
      if (params.nonEmpty) Some(BatchSql(query, params.head, params.tail: _*)) else None

    def prepare: PreparedBatches =
      new PreparedBatches(
        prepareNonEmpty(insertCreate, creates),
        prepareNonEmpty(insertExercise, exercises),
        prepareNonEmpty(updateArchived, archives),
      ) {}

  }

  private object AccumulatingBatches {
    val empty: AccumulatingBatches = AccumulatingBatches(
      creates = Vector.empty,
      exercises = Vector.empty,
      archives = Vector.empty,
    )
  }

  /**
    * @throws RuntimeException If a value cannot be serialized into an array of bytes
    */
  @throws[RuntimeException]
  def prepareBatchInsert(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      commandId: Option[CommandId],
      submitter: Option[Party],
      roots: Set[NodeId],
      ledgerEffectiveTime: Date,
      offset: LedgerDao#LedgerOffset,
      transaction: Transaction,
  ): PreparedBatches =
    transaction
      .fold(AccumulatingBatches.empty) {
        case (batches, (nodeId, node: Create)) =>
          batches.addCreate(
            create(
              applicationId = applicationId,
              workflowId = workflowId,
              commandId = commandId,
              transactionId = transactionId,
              nodeId = nodeId,
              submitter = submitter,
              roots = roots,
              ledgerEffectiveTime = ledgerEffectiveTime,
              offset = offset,
              create = node,
            )
          )
        case (batches, (nodeId, node: Exercise)) =>
          val batchWithExercises =
            batches.addExercise(
              exercise(
                applicationId = applicationId,
                workflowId = workflowId,
                commandId = commandId,
                transactionId = transactionId,
                nodeId = nodeId,
                submitter = submitter,
                roots = roots,
                ledgerEffectiveTime = ledgerEffectiveTime,
                offset = offset,
                exercise = node,
              )
            )
          if (node.consuming) {
            batchWithExercises.addArchive(
              archive(
                contractId = node.targetCoid,
                consumedAt = offset,
              )
            )
          } else {
            batchWithExercises
          }
        case (batches, _) =>
          batches // ignore any event which is not a create or an exercise
      }
      .prepare

}
