// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import com.digitalasset.canton.BaseTestWordSpec
import os.Path

class AmmoniteCacheLockTest extends BaseTestWordSpec {

  private lazy val testDir = Path(File.newTemporaryDirectory().deleteOnExit().path)

  "concurrent access to ammonite cache" should {
    "work in the happy case" in {
      val lock = AmmoniteCacheLock.create(logger, testDir, isRepl = false)

      lock.lockFile.value.exists() shouldBe true

      lock.release()

      lock.lockFile.value.exists() shouldBe false

    }

    "reuses previous cache" in {

      val lock = AmmoniteCacheLock.create(logger, testDir, isRepl = false)
      lock.release()

      val lock2 = AmmoniteCacheLock.create(logger, testDir, isRepl = false)

      lock.lockFile should not be empty
      lock.lockFile shouldBe lock2.lockFile

    }

    "prevent concurrent access" in {
      val lock = AmmoniteCacheLock.create(logger, testDir, isRepl = false)
      val lock2 = AmmoniteCacheLock.create(logger, testDir, isRepl = false)

      lock2.lockFile.value.exists() shouldBe true
      lock.lockFile.value.exists() shouldBe true
      lock.lockFile.value should not be lock2.lockFile.value

      lock2.release()
      lock.release()

    }

  }

}
