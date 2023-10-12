// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Paths

final class PathUtilsTest extends AnyWordSpec with Matchers {

  import PathUtils.getFilenameWithoutExtension
  import Paths.get as path

  "getFilenameWithoutExtension" should {
    "extract filename without extension" when {
      "getting an absolute path" in {
        getFilenameWithoutExtension(path("/dir1/dir2/file.txt")) shouldBe "file"
      }
      "getting a relative path" in {
        getFilenameWithoutExtension(path("dir1/dir2/file.txt")) shouldBe "file"
      }
      "getting a path relative to the current directory" in {
        getFilenameWithoutExtension(path("./dir1/dir2/file.txt")) shouldBe "file"
      }
      "getting a path relative to the parent directory" in {
        getFilenameWithoutExtension(path("../dir1/dir2/file.txt")) shouldBe "file"
      }
      "getting just a filename" in {
        getFilenameWithoutExtension(path("file.txt")) shouldBe "file"
      }
      "getting a file starting with a dot" in {
        getFilenameWithoutExtension(path(".file.txt")) shouldBe ".file"
      }
      "getting a file with multiple dots" in {
        getFilenameWithoutExtension(path("/d.ir.1/di.r.2/file.a.b.c.txt")) shouldBe "file.a.b.c"
      }
    }
    "leave the filename untouched" when {
      "getting an empty path" in {
        PathUtils.getFilenameWithoutExtension(path("")) shouldBe ""
      }
      "getting a filename without extension" in {
        getFilenameWithoutExtension(path("file")) shouldBe "file"
      }
    }
  }
}
