// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.ha

import com.daml.testing.oracle.OracleAroundAll

final class IndexerStabilityOracleSpec extends IndexerStabilitySpec with OracleAroundAll {

  override def jdbcUrl: String = oracleJdbcUrl
}
