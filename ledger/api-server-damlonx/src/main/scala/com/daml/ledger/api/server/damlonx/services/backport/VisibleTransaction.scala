// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services.backport

import com.digitalasset.daml.lf.data.Ref.{LedgerString, Party}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.platform.server.services.transaction.TransactionFiltration.RichTransactionFilter
import com.digitalasset.platform.common.{PlatformTypes => P}
import com.digitalasset.platform.server.services.transaction.{TransactionMeta, TransactionWithMeta}

/** Contains all data that's necessary to assemble a transaction for the API */
final case class VisibleTransaction(
    transaction: P.GenTransaction[LedgerString, AbsoluteContractId],
    meta: TransactionMeta,
    disclosureByNodeId: Map[LedgerString, Set[Party]]) {

  private type MapStringSet[T] = Map[String, Set[T]]

}

object VisibleTransaction {

  def toVisibleTransaction(
      transactionFilter: TransactionFilter,
      transactionWitMeta: TransactionWithMeta): Option[VisibleTransaction] =
    transactionFilter
      .filter(transactionWitMeta.transaction)
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
