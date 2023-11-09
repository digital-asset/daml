// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.{ExceptionHandler, StandardRoute}
import org.slf4j.Logger
import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

import scala.util.control.NonFatal

object Result {

  final case class Success[A](result: A, status: Int)

  final case class Failure(error: String, status: Int)

  trait JsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {

    protected implicit def successFormat[A: JsonFormat]: RootJsonFormat[Success[A]] =
      jsonFormat2(Success[A])

    protected implicit val failureFormat: RootJsonFormat[Failure] =
      jsonFormat2(Failure)

    protected def rejectBadInput(error: String): StandardRoute =
      complete(StatusCodes.BadRequest, Failure(error, status = 400))

    protected def logAndReport(logger: Logger)(error: String): ExceptionHandler =
      ExceptionHandler { case NonFatal(exception) =>
        logger.error("An exception occurred on the server while processing a request", exception)
        complete(StatusCodes.InternalServerError, Failure(error, status = 500))
      }

  }

}
