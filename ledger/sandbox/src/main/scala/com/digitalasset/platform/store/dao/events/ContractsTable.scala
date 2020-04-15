// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.{BatchSql, NamedParameter}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.serialization.KeyHasher.{hashKey => hash}
import com.daml.platform.store.serialization.ValueSerializer.{serializeValue => serialize}

private[events] object ContractsTable {

  private val insertContractQuery =
    "insert into participant_contracts(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders) values ({contract_id}, {template_id}, {create_argument}, {create_ledger_effective_time}, {create_key_hash}, {create_stakeholders})"
  private def insertContractQuery(
      contractId: ContractId,
      templateId: Identifier,
      createArgument: Value,
      createLedgerEffectiveTime: Option[Instant],
      stakeholders: Set[Party],
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
      "create_stakeholders" -> stakeholders.toArray[String],
      "create_key_hash" -> key.map(hash),
    )

  private val deleteContractQuery =
    s"delete from participant_contracts where contract_id = {contract_id}"
  private def deleteContract(contractId: ContractId): Vector[NamedParameter] =
    Vector[NamedParameter]("contract_id" -> contractId)

  case class PreparedBatches private (
      insertions: Option[(Set[ContractId], BatchSql)],
      deletions: Option[(Set[ContractId], BatchSql)],
  )

  private case class AccumulatingBatches(
      insertions: Map[ContractId, Vector[NamedParameter]],
      deletions: Map[ContractId, Vector[NamedParameter]],
  ) {

    def insert(contractId: ContractId, insertion: Vector[NamedParameter]): AccumulatingBatches =
      copy(insertions = insertions.updated(contractId, insertion))

    // If the batch contains the contractId, remove the insertion.
    // Otherwise, add a delete. This prevents the insertion of transient contracts.
    def delete(contractId: ContractId, deletion: => Vector[NamedParameter]): AccumulatingBatches =
      if (insertions.contains(contractId))
        copy(insertions = insertions - contractId)
      else
        copy(deletions = deletions.updated(contractId, deletion))

    private def prepareNonEmpty(
        query: String,
        contractIdToParameters: Map[ContractId, Vector[NamedParameter]],
    ): Option[(Set[ContractId], BatchSql)] = {
      if (contractIdToParameters.nonEmpty) {
        val contractIds = contractIdToParameters.keySet
        val parameters = contractIdToParameters.valuesIterator.toSeq
        val batch = BatchSql(query, parameters.head, parameters.tail: _*)
        Some(contractIds -> batch)
      } else {
        None
      }
    }

    def prepare: PreparedBatches =
      PreparedBatches(
        insertions = prepareNonEmpty(insertContractQuery, insertions),
        deletions = prepareNonEmpty(deleteContractQuery, deletions),
      )

  }

  def prepareBatchInsert(
      ledgerEffectiveTime: Instant,
      transaction: Transaction,
      divulgedContracts: Iterable[(ContractId, Contract)],
  ): PreparedBatches = {

    // Add the locally created contracts, ensuring that _transient_
    // contracts are not inserted in the first place
    val locallyCreatedContracts =
      transaction
        .fold(AccumulatingBatches(Map.empty, Map.empty)) {
          case (batches, (_, node: Create)) =>
            batches.insert(
              contractId = node.coid,
              insertion = insertContractQuery(
                contractId = node.coid,
                templateId = node.coinst.template,
                createArgument = node.coinst.arg,
                createLedgerEffectiveTime = Some(ledgerEffectiveTime),
                stakeholders = node.stakeholders,
                key = node.key.map(k => Key.assertBuild(node.coinst.template, k.key.value)),
              )
            )
          case (batches, (_, node: Exercise)) if node.consuming =>
            batches.delete(
              contractId = node.targetCoid,
              deletion = deleteContract(node.targetCoid),
            )
          case (batches, _) =>
            batches // ignore any event which is neither a create nor a consuming exercise
        }

    // Divulged contracts are inserted _after_ locally created contracts to make sure they are
    // not skipped if consumed in this transaction due to the logic that prevents the insertion
    // of transient contracts (a divulged contract _must_ be inserted, regardless of whether it's
    // consumed or not).
    val divulgedContractsInsertions =
      divulgedContracts.iterator.collect {
        case (contractId, contract) if !locallyCreatedContracts.insertions.contains(contractId) =>
          contractId -> insertContractQuery(
            contractId = contractId,
            templateId = contract.template,
            createArgument = contract.arg,
            createLedgerEffectiveTime = None,
            stakeholders = Set.empty,
            key = None,
          )
      }.toMap

    locallyCreatedContracts
      .copy(insertions = locallyCreatedContracts.insertions ++ divulgedContractsInsertions)
      .prepare

  }

}
