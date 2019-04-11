// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1.impl

import com.daml.ledger.participant.state.v1.{Update, Offset}

/**
  * This package contains the reference in-memory implementation of the
  * participant state index service.
  */
package object reference {

  trait UpdateConsumer {
    def offer(updateId: Offset, update: Update, newState: IndexState): Unit
  }
}
