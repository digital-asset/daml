// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import com.daml.ledger.participant.state.v1.DivulgedContract
import anorm.SqlParser.int
import anorm.{BatchSql, NamedParameter, SqlStringInterpolation, ~}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.events.RawBatch.PartialParameters

import scala.util.{Failure, Success, Try}

private[events] sealed abstract class ContractsTable extends PostCommitValidationData {

  protected val insertContractQuery: String

  private val deleteContractQuery =
    s"delete from participant_contracts where contract_id = {contract_id}"
  private def deleteContract(contractId: ContractId): Vector[NamedParameter] =
    Vector[NamedParameter]("contract_id" -> contractId)

  private case class AccumulatingBatches(
      insertions: Map[ContractId, PartialParameters],
      deletions: Map[ContractId, Vector[NamedParameter]],
      transientContracts: Set[ContractId],
  ) {

    def insert(contractId: ContractId, insertion: PartialParameters): AccumulatingBatches =
      copy(insertions = insertions.updated(contractId, insertion))

    // If the batch contains the contractId, remove the insertion.
    // Otherwise, add a delete. This prevents the insertion of transient contracts.
    def delete(contractId: ContractId, deletion: => Vector[NamedParameter]): AccumulatingBatches =
      if (insertions.contains(contractId))
        copy(
          insertions = insertions - contractId,
          transientContracts = transientContracts + contractId,
        )
      else
        copy(deletions = deletions.updated(contractId, deletion))

    private def prepareRawNonEmpty(
        query: String,
        contractIdToParameters: Map[ContractId, PartialParameters],
    ): Option[(Set[ContractId], RawBatch)] = {
      if (contractIdToParameters.nonEmpty) {
        val contractIds = contractIdToParameters.keySet
        val parameters = contractIdToParameters.valuesIterator.toVector
        val batch = new RawBatch(query, parameters)
        Some(contractIds -> batch)
      } else {
        None
      }
    }

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

    def prepare: RawBatches =
      new RawBatches(
        insertions = prepareRawNonEmpty(insertContractQuery, insertions),
        deletions = prepareNonEmpty(deleteContractQuery, deletions),
        transientContracts = transientContracts,
      )

  }

  final class RawBatches private[ContractsTable] (
      val insertions: Option[(Set[ContractId], RawBatch)],
      val deletions: Option[(Set[ContractId], BatchSql)],
      val transientContracts: Set[ContractId],
  ) {
    def applySerialization(): SerializedBatches =
      new SerializedBatches(
        insertions = insertions.map { case (ids, rawBatch) => (ids, rawBatch.applySerialization()) },
        deletions = deletions,
        transientContracts = transientContracts,
      )
  }

  final class SerializedBatches private[ContractsTable] (
      val insertions: Option[(Set[ContractId], Vector[Vector[NamedParameter]])],
      val deletions: Option[(Set[ContractId], BatchSql)],
      val transientContracts: Set[ContractId],
  ) {
    def applyBatching(): PreparedBatches =
      new PreparedBatches(
        insertions = insertions.map {
          case (ids, params) => (ids, BatchSql(insertContractQuery, params.head, params.tail: _*))
        },
        deletions = deletions,
        transientContracts = transientContracts,
      )
  }

  final class PreparedBatches(
      val insertions: Option[(Set[ContractId], BatchSql)],
      val deletions: Option[(Set[ContractId], BatchSql)],
      val transientContracts: Set[ContractId],
  )

  def prepareBatchInsert(
      ledgerEffectiveTime: Instant,
      transaction: Transaction,
      divulgedContracts: Iterable[DivulgedContract],
  ): RawBatches = {

    // Add the locally created contracts, ensuring that _transient_
    // contracts are not inserted in the first place
    val locallyCreatedContracts =
      transaction
        .fold(AccumulatingBatches(Map.empty, Map.empty, Set.empty)) {
          case (batches, (_, node: Create)) =>
            batches.insert(
              contractId = node.coid,
              insertion = new RawBatch.Contract(
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
        case contract if !locallyCreatedContracts.insertions.contains(contract.contractId) =>
          contract.contractId -> new RawBatch.Contract(
            contractId = contract.contractId,
            templateId = contract.contractInst.template,
            createArgument = contract.contractInst.arg,
            createLedgerEffectiveTime = None,
            stakeholders = Set.empty,
            key = None,
          )
      }.toMap

    locallyCreatedContracts
      .copy(insertions = locallyCreatedContracts.insertions ++ divulgedContractsInsertions)
      .prepare

  }

  override final def lookupContractKeyGlobally(key: Key)(
      implicit connection: Connection): Option[ContractId] =
    SQL"select participant_contracts.contract_id from participant_contracts where create_key_hash = ${key.hash}"
      .as(contractId("contract_id").singleOpt)

  override final def lookupMaximumLedgerTime(ids: Set[ContractId])(
      implicit connection: Connection): Try[Option[Instant]] =
    if (ids.isEmpty) {
      Failure(ContractsTable.emptyContractIds)
    } else {
      SQL"select max(create_ledger_effective_time) as max_create_ledger_effective_time, count(*) as num_contracts from participant_contracts where participant_contracts.contract_id in ($ids)"
        .as(
          (instant("max_create_ledger_effective_time").? ~ int("num_contracts")).single
            .map {
              case result ~ numContracts if numContracts == ids.size => Success(result)
              case _ => Failure(ContractsTable.notFound(ids))
            })
    }

}

private[events] object ContractsTable {

  def apply(dbType: DbType): ContractsTable =
    dbType match {
      case DbType.Postgres => Postgresql
      case DbType.H2Database => H2Database
    }

  object Postgresql extends ContractsTable {
    override protected val insertContractQuery: String =
      "insert into participant_contracts(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders) values ({contract_id}, {template_id}, {create_argument}, {create_ledger_effective_time}, {create_key_hash}, {create_stakeholders}) on conflict do nothing"
  }

  object H2Database extends ContractsTable {
    override protected val insertContractQuery: String =
      s"merge into participant_contracts using dual on contract_id = {contract_id} when not matched then insert (contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders) values ({contract_id}, {template_id}, {create_argument}, {create_ledger_effective_time}, {create_key_hash}, {create_stakeholders})"
  }

  private def emptyContractIds: Throwable =
    new IllegalArgumentException(
      "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
    )

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
    )

}
