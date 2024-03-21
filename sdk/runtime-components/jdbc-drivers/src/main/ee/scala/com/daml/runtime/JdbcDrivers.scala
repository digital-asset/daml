// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.runtime

object JdbcDrivers {
  val availableJdbcDriverNames: Set[String] =
    Set("org.postgresql.Driver", "oracle.jdbc.OracleDriver")
}
