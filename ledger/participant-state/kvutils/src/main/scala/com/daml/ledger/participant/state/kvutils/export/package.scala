// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

package object export {

  type WriteItem = (Key, Value)

  type WriteSet = Seq[WriteItem]

}
