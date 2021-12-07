// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.app.{Config, ConfigProvider}
import scopt.OptionParser

private[memory] object InMemoryConfigProvider extends ConfigProvider[Unit] {
  override val defaultExtraConfig: Unit = ()

  override def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit = ()
}
