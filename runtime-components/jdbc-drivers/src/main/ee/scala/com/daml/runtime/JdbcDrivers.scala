// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.runtime

object JdbcDrivers {
  val supportedJdbcDriverNames: Set[String] =
    Set("org.postgresql.Driver", "oracle.jdbc.OracleDriver")
}
