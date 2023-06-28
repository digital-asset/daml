// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.integrationtest.CantonRunner

import java.nio.file.Path

object Edition {
  lazy val cantonJar: Path = CantonRunner.cantonPath
}
