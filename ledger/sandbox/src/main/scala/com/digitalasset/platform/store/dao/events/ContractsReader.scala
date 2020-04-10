// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.SqlParser.{binaryStream, str}
import anorm.{RowParser, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.serialization.KeyHasher.{hashKey => hash}
import com.daml.platform.store.serialization.ValueSerializer.{deserializeValue => deserialize}

import scala.concurrent.Future

/**
  * @see [[ContractsTable]]
  */
private[dao] final class ContractsReader(dispatcher: DbDispatcher) extends ContractStore {

  import ContractsReader._

  override def lookupActiveContract(
      submitter: Party,
      contractId: ContractId,
  ): Future[Option[Contract]] =
    dispatcher.executeSql("lookup_active_contract") { implicit connection =>
      println(s"looking up ${contractId.coid} as $submitter")
      SQL"select participant_contracts.contract_id, template_id, create_argument from #$contractsTable where contract_witness = $submitter and participant_contracts.contract_id = $contractId"
        .as(contractRowParser.singleOpt)
    }

  override def lookupContractKey(
      submitter: Party,
      key: Key,
  ): Future[Option[ContractId]] =
    dispatcher.executeSql("lookup_contract_by_key") { implicit connection =>
      SQL"select participant_contracts.contract_id from #$contractsTable where contract_witness = $submitter and create_key_hash = ${hash(key)}"
        .as(contractId("contract_id").singleOpt)
    }

  override def lookupMaximumLedgerTime(ids: Set[ContractId]): Future[Instant] =
    if (ids.isEmpty) {
      Future.failed(emptyContractIds)
    } else {
      dispatcher.executeSql("lookup_maximum_ledger_time") { implicit connection =>
        SQL"select max_create_ledger_effective_time from (select max(create_ledger_effective_time) as max_create_ledger_effective_time, count(*) as num_contracts from participant_contracts where participant_contracts.contract_id in ($ids)) as let_aggregation where num_contracts = ${ids.size}"
          .as(instant("max_create_ledger_effective_time").singleOpt)
          .getOrElse(throw notFound(ids))
      }
    }

}

object ContractsReader {

  // The contracts table _does not_ store agreement texts as they are
  // unnecessary for interpretation and validation. The contracts returned
  // from this table will _always_ have an empty agreement text.
  private val contractRowParser: RowParser[Contract] =
    str("contract_id") ~ str("template_id") ~ binaryStream("create_argument") map {
      case contractId ~ templateId ~ createArgument =>
        Contract(
          template = Identifier.assertFromString(templateId),
          arg = deserialize(
            stream = createArgument,
            errorContext = s"Failed to deserialize create argument for contract $contractId",
          ),
          agreementText = ""
        )
    }

  private val contractsTable = "participant_contracts natural join participant_contract_witnesses"

  private def emptyContractIds: Throwable =
    new IllegalArgumentException(
      "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
    )

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has been found: ${contractIds.mkString(", ")}"
    )

}
