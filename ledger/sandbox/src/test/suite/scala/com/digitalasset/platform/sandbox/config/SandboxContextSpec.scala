// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import java.io.File

import com.digitalasset.platform.sandbox.{TestDar, TestHelpers}
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest.{Matchers, WordSpec}

class SandboxContextSpec extends WordSpec with Matchers with TestHelpers {

  "SandboxContext" should {
    "parse command line arguments" in {
      val port = Array("--port", "6865")
      val time = Array("--static-time")
      val dar = Array(TestDar.dalfFile.toString)

      val dalfFileName = "ledger/sandbox/Test.dar"
      val dalf = Array(dalfFileName)

      val Some(ctx) = SandboxContext(port ++ time ++ dar ++ dalf)
      val config = ctx.config
      config.port shouldEqual 6865
      config.damlPackageContainer.files should contain theSameElementsAs List(
        TestDar.dalfFile,
        new File(dalfFileName))
      config.timeProviderType shouldEqual TimeProviderType.Static
    }
  }
}
