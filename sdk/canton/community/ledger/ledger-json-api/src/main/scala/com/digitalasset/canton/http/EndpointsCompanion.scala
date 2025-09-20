// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.logging.LoggingContextOf
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.base.error.utils.ErrorDetails.ErrorDetail
import com.digitalasset.canton.http.json.SprayJson
import com.digitalasset.canton.http.util.Logging.{
  InstanceUUID,
  RequestID,
  extendWithRequestIdLogCtx,
}
import com.digitalasset.canton.ledger.service.Grpc.StatusEnvelope
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.NoTracing
import com.google.rpc.{Code as GrpcCode, Status}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.RouteResult.Complete
import org.apache.pekko.http.scaladsl.server.{RequestContext, Route}
import org.apache.pekko.util.ByteString
import scalaz.Show
import spray.json.JsValue

import scala.concurrent.Future
import scala.util.control.NonFatal

import util.GrpcHttpErrorCodes.*

object EndpointsCompanion extends NoTracing {

  sealed abstract class Error extends Product with Serializable

  final case class InvalidUserInput(message: String) extends Error

  final case class Unauthorized(message: String) extends Error

  final case class ServerError(message: Throwable) extends Error

  final case class ParticipantServerError(
      grpcStatus: GrpcCode,
      description: String,
      details: Seq[ErrorDetail],
  ) extends Error

  object ParticipantServerError {
    def apply(status: Status): ParticipantServerError =
      ParticipantServerError(
        com.google.rpc.Code.forNumber(status.getCode),
        status.getMessage,
        ErrorDetails.from(status),
      )
  }

  final case class NotFound(message: String) extends Error

  object Error {
    implicit val ShowInstance: Show[Error] = Show shows {
      case InvalidUserInput(e) => s"Endpoints.InvalidUserInput: ${e: String}"
      case ParticipantServerError(grpcStatus, description, _) =>
        s"Endpoints.ParticipantServerError: $grpcStatus: $description"
      case ServerError(e) => s"Endpoints.ServerError: ${e.getMessage: String}"
      case Unauthorized(e) => s"Endpoints.Unauthorized: ${e: String}"
      case NotFound(e) => s"Endpoints.NotFound: ${e: String}"
    }

    def fromThrowable: PartialFunction[Throwable, Error] = {
      case StatusEnvelope(status) => ParticipantServerError(status)
      case NonFatal(t) => ServerError(t)
    }
  }

  def notFound(
      logger: TracedLogger
  )(implicit lc: LoggingContextOf[InstanceUUID]): Route = (ctx: RequestContext) =>
    extendWithRequestIdLogCtx(implicit lc =>
      Future.successful(
        Complete(
          httpResponseError(NotFound(s"${ctx.request.method}, uri: ${ctx.request.uri}"), logger)
        )
      )
    )

  def httpResponseError(
      error: Error,
      logger: TracedLogger,
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): HttpResponse = {
    import com.digitalasset.canton.http.json.JsonProtocol.*
    val resp = errorResponse(error, logger)
    httpResponse(resp.status, SprayJson.encodeUnsafe(resp))
  }

  def errorResponse(
      error: Error,
      logger: TracedLogger,
  )(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): ErrorResponse = {
    def mkErrorResponse(
        status: StatusCode,
        error: String,
        ledgerApiError: Option[LedgerApiError] = None,
    ) =
      ErrorResponse(
        errors = List(error),
        status = status,
        ledgerApiError = ledgerApiError,
      )
    error match {
      case InvalidUserInput(e) => mkErrorResponse(StatusCodes.BadRequest, e)
      case ParticipantServerError(grpcStatus, description, details) =>
        val ledgerApiError =
          LedgerApiError(
            code = grpcStatus.getNumber,
            message = description,
            details = details.map(ErrorDetail.fromErrorUtils),
          )
        mkErrorResponse(
          grpcStatus.asPekkoHttpForJsonApi,
          s"$grpcStatus: $description",
          Some(ledgerApiError),
        )
      case ServerError(reason) =>
        logger.error(s"Internal server error occured, ${lc.makeString}", reason)
        mkErrorResponse(StatusCodes.InternalServerError, "HTTP JSON API Server Error")
      case Unauthorized(e) => mkErrorResponse(StatusCodes.Unauthorized, e)
      case NotFound(e) => mkErrorResponse(StatusCodes.NotFound, e)
    }
  }

  def httpResponse(status: StatusCode, data: JsValue): HttpResponse =
    HttpResponse(
      status = status,
      entity = HttpEntity.Strict(ContentTypes.`application/json`, format(data)),
    )

  def format(a: JsValue): ByteString = ByteString(a.compactPrint)
}
