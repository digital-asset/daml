// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl.reference

import com.daml.ledger.participant.state.v1.{CommittedTransaction, NodeId}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.api.domain.TransactionFilter
import com.digitalasset.platform.server.services.transaction.TransactionFiltration.RichTransactionFilter
import scalaz.syntax.std.map._

case class TransactionFiltering(filter: TransactionFilter) {
  val richTxFilter = RichTransactionFilter(filter)

  def visibleNodes(tx: CommittedTransaction): Map[NodeId, Set[Party]] =
    richTxFilter
      .filter[NodeId, Value.AbsoluteContractId, Value.VersionedValue[Value.AbsoluteContractId]](tx)
      .getOrElse(Map.empty)
      .mapKeys(k =>
        // FIXME(JM): Refactor transaction filtering to not go via strings
        Value.NodeId.unsafeFromIndex(k.index))

}
