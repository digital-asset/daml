// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.ledger.TransactionId

final class UnexpectedNodeException(val nodeId: NodeId, val transactionId: TransactionId)
    extends Throwable {
  override def getMessage: String =
    s"Cannot index node ${nodeId.index} in transaction $transactionId: it's neither a create nor an exercise"
}
