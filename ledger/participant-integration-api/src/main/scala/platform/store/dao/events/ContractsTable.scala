// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.int
import anorm.{SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.PartyDetails
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.JdbcLedgerDao

import scala.util.{Failure, Success, Try}

private[events] object ContractsTable extends PostCommitValidationData {

  override final def lookupContractKeyGlobally(
      key: Key
  )(implicit connection: Connection): Option[ContractId] =
    SQL"select participant_contracts.contract_id from participant_contracts where create_key_hash = ${key.hash}"
      .as(contractId("contract_id").singleOpt)

  override final def lookupMaximumLedgerTime(
      ids: Set[ContractId]
  )(implicit connection: Connection): Try[Option[Instant]] =
    if (ids.isEmpty) {
      Failure(ContractsTable.emptyContractIds)
    } else {
      SQL"select max(create_ledger_effective_time) as max_create_ledger_effective_time, count(*) as num_contracts from participant_contracts where participant_contracts.contract_id in ($ids)"
        .as(
          (instant("max_create_ledger_effective_time").? ~ int("num_contracts")).single
            .map {
              case result ~ numContracts if numContracts == ids.size => Success(result)
              case _ => Failure(ContractsTable.notFound(ids))
            }
        )
    }

  override final def lookupParties(parties: Seq[Party])(implicit
      connection: Connection
  ): List[PartyDetails] =
    JdbcLedgerDao.selectParties(parties).map(JdbcLedgerDao.constructPartyDetails)

  private def emptyContractIds: Throwable =
    new IllegalArgumentException(
      "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
    )

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
    )
}
