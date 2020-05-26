// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import java.io.{BufferedWriter, File, FileWriter}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpHeader,
  HttpMethods,
  HttpRequest,
  HttpResponse,
  StatusCode,
  Uri
}
import akka.stream.Materializer
import akka.util.ByteString
import com.daml.lf.data.TryOps.Bracket.bracket
import com.typesafe.scalalogging.LazyLogging
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object TestUtil extends LazyLogging {
  def requiredFile(fileName: String): Try[File] = {
    val file = new File(fileName).getAbsoluteFile
    if (file.exists()) Success(file)
    else
      Failure(new IllegalStateException(s"File doest not exist: $fileName"))
  }

  def writeToFile(file: File, text: String): Try[File] =
    bracket(Try(new BufferedWriter(new FileWriter(file))))(x => Try(x.close())).flatMap { bw =>
      Try {
        bw.write(text)
        file
      }
    }

  def readFile(resourcePath: String): String =
    Try {
      val source = Source.fromResource(resourcePath)
      val content = source.getLines().mkString
      source.close
      content
    } match {
      case Success(value) => value
      case Failure(ex) => throw ex
    }

  def getResponseDataBytes(resp: HttpResponse, debug: Boolean = false)(
      implicit mat: Materializer,
      ec: ExecutionContext): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    if (debug) fb.foreach(x => logger.info(s"---- response data: $x"))
    fb
  }

  def postRequest(uri: Uri, json: JsValue, headers: List[HttpHeader] = Nil)(
      implicit as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer): Future[(StatusCode, JsValue)] = {
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/json`, json.prettyPrint))
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => {
          (resp.status, body.parseJson)
        })
      }
  }

  def postJsonStringRequest(uri: Uri, jsonString: String, headers: List[HttpHeader])(
      implicit as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer): Future[(StatusCode, JsValue)] = {
    logger.info(s"postJson: ${uri.toString} json: ${jsonString: String}")
    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = headers,
          entity = HttpEntity(ContentTypes.`application/json`, jsonString))
      )
      .flatMap { resp =>
        val bodyF: Future[String] = getResponseDataBytes(resp, debug = true)
        bodyF.map(body => (resp.status, body.parseJson))
      }
  }

  def postJsonRequest(uri: Uri, json: JsValue, headers: List[HttpHeader])(
      implicit as: ActorSystem,
      ec: ExecutionContext,
      mat: Materializer): Future[(StatusCode, JsValue)] =
    postJsonStringRequest(uri, json.prettyPrint, headers)
}
