// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.test.BenchtoolTestDar
import com.daml.platform.sandbox.fixture.SandboxFixture
import org.scalatest.Suite

trait BenchtoolSandboxFixture extends SandboxFixture {
  self: Suite =>

  override protected def packageFiles: List[File] = List(
    new File(rlocation(BenchtoolTestDar.path))
  )

}
