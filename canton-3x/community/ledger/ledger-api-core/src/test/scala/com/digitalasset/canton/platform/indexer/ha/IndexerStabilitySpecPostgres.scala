// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.ha

import com.digitalasset.canton.platform.store.testing.postgresql.PostgresAroundEach

final class IndexerStabilitySpecPostgres extends IndexerStabilitySpec with PostgresAroundEach {

  override def jdbcUrl: String = postgresDatabase.url

  override def lockIdSeed: Int =
    1000 // it does not matter in case of Postgres: it is always a different instance
}
