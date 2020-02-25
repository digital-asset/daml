// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen.util

import java.io.File
import java.net.{InetAddress, ServerSocket}

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import scalaz.{@@, Tag}

import scala.util.Try

object TestUtil {
  sealed trait TestContextTag
  type TestContext = String @@ TestContextTag
  val TestContext = Tag.of[TestContextTag]

  def findOpenPort(): Try[Int] = Try {
    val socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress)
    val result = socket.getLocalPort
    socket.close()
    result
  }

  def requiredResource(path: String): File = {
    val f = new File(rlocation(path)).getAbsoluteFile
    require(f.exists, s"File does not exist: $f")
    f
  }
}
