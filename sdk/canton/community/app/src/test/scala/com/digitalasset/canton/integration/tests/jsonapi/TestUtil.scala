// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.typesafe.scalalogging.LazyLogging
import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.ByteString

import java.io.{BufferedWriter, File, FileWriter}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try, Using}

object TestUtil extends LazyLogging {

  def requiredFile(fileName: String): Try[File] = {
    val file = new File(fileName).getAbsoluteFile
    if (file.exists()) Success(file)
    else
      Failure(new IllegalStateException(s"File does not exist: $fileName"))
  }

  def writeToFile(file: File, text: String): Try[File] =
    Using(new BufferedWriter(new FileWriter(file))) { bw =>
      bw.write(text)
      file
    }

  def getResponseDataBytes(resp: HttpResponse, debug: Boolean = false)(implicit
      mat: Materializer,
      ec: ExecutionContext,
  ): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
  }

}
