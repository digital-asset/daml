// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonrepudiation.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import org.slf4j.Logger

import scala.util.control.NonFatal

package object v1 {

  def logAndReport(logger: Logger)(error: String): ExceptionHandler =
    ExceptionHandler { case NonFatal(exception) =>
      logger.error("An exception occurred on the server while processing a request", exception)
      complete(HttpResponse(StatusCodes.InternalServerError, entity = error))
    }

}
