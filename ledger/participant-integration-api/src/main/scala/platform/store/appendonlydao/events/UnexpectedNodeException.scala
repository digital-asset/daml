// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

final class UnexpectedNodeException(val nodeId: NodeId, val transactionId: TransactionId)
    extends Throwable {
  override def getMessage: String =
    s"Cannot index node ${nodeId.index} in transaction $transactionId: it's neither a create nor an exercise"
}
