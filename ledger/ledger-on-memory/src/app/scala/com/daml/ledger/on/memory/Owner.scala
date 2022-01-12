// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.app.{Config, ConfigProvider, Runner}
import com.daml.ledger.resources.ResourceOwner

object Owner {
  // Utily if you want to spin this up as a library.
  def apply(config: Config[Unit]): ResourceOwner[Unit] =
    for {
      dispatcher <- dispatcherOwner
      sharedState = InMemoryState.empty
      factory = new InMemoryLedgerFactory(dispatcher, sharedState)
      runner <- new Runner(RunnerName, factory, ConfigProvider.ForUnit).owner(config)
    } yield runner
}
