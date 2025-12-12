// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.testing.postgresql

import java.nio.file.{Path, Paths}

import com.daml.bazeltools.BazelRunfiles.{exe, rlocation}

private case class Tool private[postgresql] (name: String) {
  def path: Path = rlocation(Tool.binPath.resolve(name + exe))
}

private[postgresql] object Tool {
  private val binPath = Paths.get("external", "postgresql_dev_env", "bin")

  val createdb: Tool = Tool("createdb")
  val dropdb: Tool = Tool("dropdb")
  val initdb: Tool = Tool("initdb")
  val pg_ctl: Tool = Tool("pg_ctl")
}
