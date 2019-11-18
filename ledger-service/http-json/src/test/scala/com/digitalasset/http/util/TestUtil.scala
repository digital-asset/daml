// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import java.io.{BufferedWriter, File, FileWriter}
import java.net.ServerSocket
import com.digitalasset.daml.lf.data.TryOps.Bracket.bracket

import scala.util.{Failure, Success, Try}

object TestUtil {
  def findOpenPort(): Try[Int] = Try {
    val socket = new ServerSocket(0)
    val result = socket.getLocalPort
    socket.close()
    result
  }

  def requiredFile(fileName: String): Try[File] = {
    val file = new File(fileName).getAbsoluteFile
    if (file.exists()) Success(file)
    else
      Failure(new IllegalStateException(s"File doest not exist: $fileName"))
  }

  def createDirectory(path: String): Try[File] = {
    val file = new File(path)
    val created = file.mkdirs
    if (created && file.isDirectory) Success(file)
    else Failure(new IllegalStateException(s"Cannot create directory: $path"))
  }

  def writeToFile(file: File, text: String): Try[File] =
    bracket(Try(new BufferedWriter(new FileWriter(file))))(x => Try(x.close())).flatMap { bw =>
      Try {
        bw.write(text)
        file
      }
    }
}
