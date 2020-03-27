// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

final case class ExtraConfig(
    maxInboundMessageSize: Int
)

object ExtraConfig {
  val defaultMaxInboundMessageSize: Int = 4 * 1024 * 1024
  val default = ExtraConfig(defaultMaxInboundMessageSize)
}
