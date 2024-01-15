// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.bazeltools.BazelRunfiles.rlocation

import java.nio.file.{Path, Paths}

object Edition {
  lazy val cantonJar: Path = Paths.get(rlocation("test-common/canton/canton-ee_deploy.jar"))
}
