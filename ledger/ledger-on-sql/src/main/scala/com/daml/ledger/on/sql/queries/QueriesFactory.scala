// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql.queries

import java.sql.Connection

import com.daml.ledger.participant.state.kvutils.KVOffsetBuilder

trait QueriesFactory {
  def apply(offsetBuilder: KVOffsetBuilder, connection: Connection): Queries
}
