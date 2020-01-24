// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.testing.postgresql

import java.nio.file.Path

case class PostgresFixture(
    jdbcUrl: String,
    port: Int,
    tempDir: Path,
    dataDir: Path,
    confFile: Path,
    logFile: Path,
)
