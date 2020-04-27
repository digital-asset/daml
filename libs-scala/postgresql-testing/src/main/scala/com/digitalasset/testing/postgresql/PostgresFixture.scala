// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import java.nio.file.Path

import com.daml.ports.Port

case class PostgresFixture(
    jdbcUrl: String,
    port: Port,
    tempDir: Path,
    dataDir: Path,
    confFile: Path,
    logFile: Path,
)
