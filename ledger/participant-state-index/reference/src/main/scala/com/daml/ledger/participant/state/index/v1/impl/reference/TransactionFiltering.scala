// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import com.daml.ledger.participant.state.v1.{CommittedTransaction, NodeId, Party}
import com.digitalasset.daml.lf.data.Ref.SimpleString
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.platform.server.services.transaction.TransactionFiltration.RichTransactionFilter

case class TransactionFiltering(filter: TransactionFilter) {
  val richTxFilter = RichTransactionFilter(filter)

  def visibleNodes(tx: CommittedTransaction): Map[NodeId, Set[Party]] =
    richTxFilter
      .filter[NodeId, Value.AbsoluteContractId, Value.VersionedValue[Value.AbsoluteContractId]](
        tx,
        n => n.index.toString)
      .getOrElse(Map.empty)
      .map {
        // FIXME(JM): Refactor transaction filtering to not go via strings
        case (k, v) =>
          Value.NodeId.unsafeFromIndex(k.toInt) -> v.map(p =>
            SimpleString.assertFromString(p.underlyingString))
      }
}
