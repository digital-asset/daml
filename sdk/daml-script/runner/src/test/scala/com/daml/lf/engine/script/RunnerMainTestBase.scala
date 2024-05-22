// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import com.daml.bazeltools.BazelRunfiles
import java.nio.file.{Path, Paths}
import org.scalatest.Suite

trait RunnerMainTestBase extends RunnerTestBase {
  self: Suite =>
  protected val jwt: Path =
    BazelRunfiles.rlocation(Paths.get("daml-script/runner/src/test/resources/json-access.jwt"))
  protected val inputFile: String =
    BazelRunfiles.rlocation("daml-script/runner/src/test/resources/input.json")

  // Defines the size of `dars`
  // Should always match test_dar_count in the BUILD file
  val DAR_COUNT = 5

  // We use a different DAR for each test that asserts upload behaviour to avoid clashes
  val dars: Seq[Path] = (1 to DAR_COUNT).map(n =>
    BazelRunfiles.rlocation(Paths.get(s"daml-script/runner/test-script$n.dar"))
  )

  // DAR containing failingScript and successfulScript
  val failingDar: Path =
    BazelRunfiles.rlocation(Paths.get("daml-script/runner/failing-test-script.dar"))
}
