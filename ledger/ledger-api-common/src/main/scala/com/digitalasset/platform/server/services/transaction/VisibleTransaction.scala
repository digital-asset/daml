// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Ref.{LedgerName, Party}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.platform.server.services.transaction.TransactionFiltration.RichTransactionFilter
import com.digitalasset.platform.common.{PlatformTypes => P}

/** Contains all data that's necessary to assemble a transaction for the API */
final case class VisibleTransaction(
    transaction: P.GenTransaction[LedgerName, AbsoluteContractId],
    meta: TransactionMeta,
    disclosureByNodeId: Map[LedgerName, Set[Party]]) {

  private type MapStringSet[T] = Map[String, Set[T]]

  /* FixMe(RH): Drop or uncomment
  def disclosureByNodeIdStr: MapStringSet[String] = disclosureByNodeId.map {
    case (k, v) => k -> v.map(_.underlyingString)
  }*/
}

object VisibleTransaction {

  def toVisibleTransaction(
      transactionFilter: TransactionFilter,
      transactionWitMeta: TransactionWithMeta): Option[VisibleTransaction] =
    transactionFilter
      .filter(transactionWitMeta.transaction, identity[LedgerName])
      .map(
        VisibleTransaction(
          transactionWitMeta.transaction,
          removeConfidentialMeta(transactionWitMeta, transactionFilter.filtersByParty.keySet),
          _))

  private def removeConfidentialMeta(
      transaction: TransactionWithMeta,
      requestingParties: Set[Party]): TransactionMeta =
    transaction.meta.submitter match {
      case Some(submitter) if !requestingParties.contains(submitter) =>
        transaction.meta.copy(submitter = None, commandId = None)
      case _ => transaction.meta
    }
}
