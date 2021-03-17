// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy
package testing
import org.scalatest.wordspec.AnyWordSpec

class ReplTest extends AnyWordSpec {

  val file = "/tmp/Main.dar"

  "Repl" should {

    "succeed " in
      assert(Repl.testAll(Repl.devCompilerConfig, file)._1)
  }
}
