// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on

import com.daml.ledger.resources.ResourceOwner
import com.daml.platform.akkastreams.dispatcher.Dispatcher

package object memory {
  type Index = Int

  private[memory] val StartIndex: Index = 0

  private[memory] def dispatcherOwner: ResourceOwner[Dispatcher[Index]] =
    Dispatcher.owner(
      name = "in-memory-key-value-participant-state",
      zeroIndex = StartIndex,
      headAtInitialization = StartIndex,
    )

  private[memory] val RunnerName = "In-Memory Ledger"
}
