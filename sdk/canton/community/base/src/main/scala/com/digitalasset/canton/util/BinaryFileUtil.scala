// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.google.protobuf.ByteString

import java.io.*
import java.net.{HttpURLConnection, URI}
import java.nio.file.{Files, Path, StandardCopyOption}
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
          Left(s"No such file [$inputFile].")
        else
          Left(
            s"File exists but cannot be read [$inputFile]. ${ErrorUtil.messageWithStacktrace(e)}"
          )
    } finally {
      bis.foreach(_.close())
    }
  }

  def tryReadByteStringFromFile(inputFile: String): ByteString = readByteStringFromFile(inputFile)
    .fold(err => throw new IllegalArgumentException(s"Can not load $inputFile: $err"), identity)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def downloadFile(
      url: String,
      outputFile: String,
      headers: Map[String, String],
  ): Either[String, Unit] = {
    val conn = (new URI(url)).toURL.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    headers.foreach { case (k, v) => conn.setRequestProperty(k, v) }
    conn.setUseCaches(false)
    conn.setDoInput(true)
    val code = conn.getResponseCode
    if (code == HttpURLConnection.HTTP_OK) {
      val in = conn.getInputStream
      try {
        Files.copy(in, Path.of(outputFile), StandardCopyOption.REPLACE_EXISTING).discard
      } finally {
        in.close()
      }
      Right(())
    } else {
      Left(s"Failed to download file from $url. Response code: $code")
    }
  }

}
