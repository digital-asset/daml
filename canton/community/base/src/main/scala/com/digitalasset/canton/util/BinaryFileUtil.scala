// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.google.protobuf.ByteString

import java.io.*
import scala.concurrent.blocking

/** Write and read byte strings to files.
  */
object BinaryFileUtil {

  def writeByteStringToFile(outputFile: String, bytes: ByteString): Unit = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var bos: Option[BufferedOutputStream] = None
    try {
      bos = Some(new BufferedOutputStream(new FileOutputStream(outputFile)))
      bos.foreach { s =>
        blocking {
          s.write(bytes.toByteArray)
        }
      }
    } finally {
      bos.foreach(_.close())
    }
  }

  def readByteStringFromFile(inputFile: String): Either[String, ByteString] = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var bis: Option[BufferedInputStream] = None
    try {
      bis = Some(new BufferedInputStream(new FileInputStream(inputFile)))
      blocking {
        bis.map(ByteString.readFrom).toRight("Will not happen as otherwise it would throw")
      }
    } catch {
      case e: IOException =>
        val f = new java.io.File(inputFile)
        if (!f.exists())
          Left(s"No such file [${inputFile}].")
        else
          Left(
            s"File exists but cannot be read [${inputFile}]. ${ErrorUtil.messageWithStacktrace(e)}"
          )
    } finally {
      bis.foreach(_.close())
    }
  }

  def tryReadByteStringFromFile(inputFile: String): ByteString = readByteStringFromFile(inputFile)
    .fold(err => throw new IllegalArgumentException(s"Can not load ${inputFile}: $err"), identity)

}
