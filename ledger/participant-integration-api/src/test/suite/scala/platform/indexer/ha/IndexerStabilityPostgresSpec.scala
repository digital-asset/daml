// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import com.daml.testing.postgresql.PostgresAroundEach

final class IndexerStabilityPostgresSpec extends IndexerStabilitySpec with PostgresAroundEach {

  override def jdbcUrl: String = postgresDatabase.url
}
