// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services.backport

import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.ledger.api.v1.transaction.TreeEvent

final case class TransactionTreeNodes(
    eventsById: Map[LedgerString, TreeEvent],
    rootEventIds: List[LedgerString])
