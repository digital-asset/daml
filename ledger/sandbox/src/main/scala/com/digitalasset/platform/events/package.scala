// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.lf.types.Ledger

package object events {

  type TransactionIdWithIndex = Ledger.EventId
  val TransactionIdWithIndex: Ledger.EventId.type = Ledger.EventId

}
