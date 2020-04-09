// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.serialization.KeyHasher.{hashKey => hash}
import com.daml.platform.store.serialization.ValueSerializer.{serializeValue => serialize}

private[events] object ContractsTable {

  private val insertQuery =
    "insert into participant_contracts(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash) values ({contract_id}, {template_id}, {create_argument}, {create_ledger_effective_time}, {create_key_hash})"
  private def insertion(
      contractId: ContractId,
      templateId: Identifier,
      createArgument: Value,
      createLedgerEffectiveTime: Option[Instant],
      key: Option[Key],
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "contract_id" -> contractId,
      "template_id" -> templateId,
      "create_argument" -> serialize(
        value = createArgument,
        errorContext = s"Cannot serialize create argument for $contractId",
      ),
      "create_ledger_effective_time" -> createLedgerEffectiveTime,
      "create_key_hash" -> key.map(hash),
    )

  private val deleteQuery = s"delete from participant_contracts where contract_id = {contract_id}"
  private def deletion(contractId: ContractId): Vector[NamedParameter] =
    Vector[NamedParameter]("contract_id" -> contractId)

  sealed abstract case class PreparedBatches(
      insertions: Option[BatchSql],
      deletions: Option[BatchSql],
      nonTransient: Set[ContractId],
  ) {
    final def isEmpty: Boolean = insertions.isEmpty && deletions.isEmpty
    final def foreach[U](f: BatchSql => U): Unit = {
      insertions.foreach(f)
      deletions.foreach(f)
    }
  }

  private case class AccumulatingBatches(
      insertions: Map[ContractId, Vector[NamedParameter]],
      deletions: Vector[Vector[NamedParameter]],
  ) {

    def insert(contractId: ContractId, insertion: Vector[NamedParameter]): AccumulatingBatches =
      copy(insertions = insertions.updated(contractId, insertion))

    def delete(deletion: Vector[NamedParameter]): AccumulatingBatches =
      copy(deletions = deletions :+ deletion)

    private def prepareNonEmpty(
        query: String,
        params: Vector[Vector[NamedParameter]],
    ): Option[BatchSql] =
      if (params.nonEmpty) Some(BatchSql(query, params.head, params.tail: _*)) else None

    def prepare: PreparedBatches =
      new PreparedBatches(
        insertions = prepareNonEmpty(insertQuery, insertions.valuesIterator.toVector),
        deletions = prepareNonEmpty(deleteQuery, deletions),
        nonTransient = insertions.keySet,
      ) {}

  }

  private object AccumulatingBatches {
    def withDivulgedContracts(
        divulgedContracts: Iterable[(ContractId, Contract)]
    ): AccumulatingBatches =
      AccumulatingBatches(
        insertions = divulgedContracts.iterator.map {
          case (contractId, contract) =>
            contractId -> insertion(
              contractId = contractId,
              templateId = contract.template,
              createArgument = contract.arg,
              createLedgerEffectiveTime = None,
              key = None,
            )
        }.toMap,
        deletions = Vector.empty,
      )
  }

  def prepareBatchInsert(
      ledgerEffectiveTime: Instant,
      transaction: Transaction,
      divulgedContracts: Iterable[(ContractId, Contract)],
  ): PreparedBatches = {
    transaction
      .fold(AccumulatingBatches.withDivulgedContracts(divulgedContracts)) {
        case (batches, (_, node: Create)) =>
          batches.copy(
            insertions = batches.insertions.updated(
              key = node.coid,
              value = insertion(
                contractId = node.coid,
                templateId = node.coinst.template,
                createArgument = node.coinst.arg,
                createLedgerEffectiveTime = Some(ledgerEffectiveTime),
                key = node.key.map(k => Key.assertBuild(node.coinst.template, k.key.value)),
              )
            )
          )
        case (batches, (_, node: Exercise)) if node.consuming =>
          if (batches.insertions.contains(node.targetCoid))
            batches.copy(
              insertions = batches.insertions - node.targetCoid,
            )
          else
            batches.copy(
              deletions = batches.deletions :+ deletion(node.targetCoid)
            )
        case (batches, _) =>
          batches // ignore any event which is neither a create nor a consuming exercise
      }
      .prepare
  }

}
