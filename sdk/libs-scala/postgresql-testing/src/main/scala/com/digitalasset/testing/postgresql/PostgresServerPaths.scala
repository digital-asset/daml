// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import java.nio.file.Path

case class PostgresServerPaths(
    root: Path,
    dataDir: Path,
    logFile: Path,
)
