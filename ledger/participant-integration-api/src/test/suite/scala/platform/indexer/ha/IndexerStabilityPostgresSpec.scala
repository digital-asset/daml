// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import com.daml.testing.postgresql.PostgresAroundEach

final class IndexerStabilityPostgresSpec extends IndexerStabilitySpec with PostgresAroundEach {

  override def jdbcUrl: String = postgresDatabase.url

  override def lockIdSeed: Int =
    1000 // it does not matter in case of Postgres: it is always a different instance
}
