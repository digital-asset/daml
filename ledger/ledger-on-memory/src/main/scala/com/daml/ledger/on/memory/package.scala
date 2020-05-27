// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on

import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerWriter}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.resources.ResourceOwner

package object memory {
  type Index = Int

  type KeyValueLedger = LedgerReader with LedgerWriter

  private[memory] val StartIndex: Index = 0

  private[memory] def dispatcherOwner: ResourceOwner[Dispatcher[Index]] =
    ResourceOwner.forCloseable(
      () =>
        Dispatcher(
          "in-memory-key-value-participant-state",
          zeroIndex = StartIndex,
          headAtInitialization = StartIndex,
      ))
}
