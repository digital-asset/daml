// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import java.io.{BufferedWriter, File, FileWriter}

import akka.http.scaladsl.model.HttpResponse
import akka.stream.Materializer
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

object TestUtil extends LazyLogging {

  def requiredFile(fileName: String): Try[File] = {
    val file = new File(fileName).getAbsoluteFile
    if (file.exists()) Success(file)
    else
      Failure(new IllegalStateException(s"File doest not exist: $fileName"))
  }

  def writeToFile(file: File, text: String): Try[File] =
    Using(new BufferedWriter(new FileWriter(file))) { bw =>
      bw.write(text)
      file
    }

  def readFile(resourcePath: String): String =
    Try {
      val source = Source.fromResource(resourcePath)
      val content = source.getLines().mkString
      source.close
      content
    } match {
      case Success(value) => value
      // only needed for Scala 2.12; 2.13 does this itself
      case Failure(_: NullPointerException) =>
        throw new java.io.FileNotFoundException(s"resource '$resourcePath' not found in classpath")
      case Failure(ex) => throw ex
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
