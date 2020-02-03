// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing.postgresql

import com.digitalasset.ledger.api.testing.utils.Resource

private class PostgresResource extends Resource[PostgresFixture] with PostgresAround {

  override def value: PostgresFixture = postgresFixture

  override def setup(): Unit = {
    startEphemeralPostgres()
  }

  override def close(): Unit = {
    stopAndCleanUpPostgres()
  }
}

object PostgresResource {
  def apply(): Resource[PostgresFixture] = new PostgresResource
}
