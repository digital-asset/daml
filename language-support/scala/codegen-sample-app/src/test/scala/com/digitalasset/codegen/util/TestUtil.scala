// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen.util

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import scalaz.{@@, Tag}

object TestUtil {
  sealed trait TestContextTag
  type TestContext = String @@ TestContextTag
  val TestContext: Tag.TagOf[TestContextTag] = Tag.of[TestContextTag]

  def requiredResource(path: String): File = {
    val f = new File(rlocation(path)).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }
}
