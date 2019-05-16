// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.ledger.api.v1.transaction.TreeEvent

final case class TransactionTreeNodes(
    eventsById: Map[LedgerName, TreeEvent],
    rootEventIds: List[LedgerName])
