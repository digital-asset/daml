// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import java.io.File
import java.net.ServerSocket

import scala.util.{Failure, Success, Try}

object TestUtil {
  def findOpenPort(): Try[Int] = Try {
    val socket = new ServerSocket(0)
    val result = socket.getLocalPort
    socket.close()
    result
  }

  def requiredFile(fileName: String): Try[File] = {
    val file = new File(fileName)
    if (file.exists()) Success(file.getAbsoluteFile)
    else
      Failure(new IllegalStateException(s"File doest not exist: $fileName"))
  }
}
