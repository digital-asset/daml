// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.HealthService
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.json.v2.{DocumentationEndpoints, Endpoints}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import org.apache.pekko.stream.Materializer
import sttp.model.StatusCode
import sttp.tapir
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.{AnyEndpoint, statusCode}

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

class JsHealthService(
    healthService: HealthService,
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    val authInterceptor: AuthInterceptor,
    val materializer: Materializer,
) extends Endpoints {

  def endpoints() = List(
    withServerLogic(
      JsHealthService.livez,
      livez,
    ),
    withServerLogic(
      JsHealthService.readyz,
      readyz,
    ),
  )

  private def livez(@unused caller: CallerContext): TracedInput[Unit] => Future[
    Either[JsCantonError, Unit]
  ] =
    _ => Future.successful(Right(()))

  private def readyz(@unused caller: CallerContext): TracedInput[Unit] => Future[
    Either[JsCantonError, (StatusCode, String)]
  ] =
    _ =>
      healthService
        .ready()
        .map(response => (response.getStatusCode, response.getResponseBody))
        .resultToRight
}

object JsHealthService extends DocumentationEndpoints {
  import Endpoints.*

  private val livez = baseEndpoint
    .errorOut(statusCode.and(jsonBody[JsCantonError]))
    .in(sttp.tapir.stringToPath("livez"))
    .get
    .out(tapir.emptyOutput.description("OK: service is alive"))
    .description("Checks if the service is alive")

  private val readyz = baseEndpoint
    .errorOut(statusCode.and(jsonBody[JsCantonError]))
    .in(sttp.tapir.stringToPath("readyz"))
    .get
    .out(statusCode.and(tapir.stringBody.description("OK: readiness message")))
    .description("Checks if the service is ready to serve requests")

  override def documentation: Seq[AnyEndpoint] = Seq(livez, readyz)
}
