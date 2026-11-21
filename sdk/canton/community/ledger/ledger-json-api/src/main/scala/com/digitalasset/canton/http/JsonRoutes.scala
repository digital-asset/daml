// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.logging.LoggingContextOf
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import com.digitalasset.canton.http.json.v2.V2Routes
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server
import org.apache.pekko.http.scaladsl.server.Directives.{extractClientIP, *}
import org.apache.pekko.http.scaladsl.server.RouteResult.*
import org.apache.pekko.http.scaladsl.server.{Directive, Directive0, PathMatcher, Route}
import scalaz.EitherT
import spray.json.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import headers.`Content-Type`
import EndpointsCompanion.*
import util.Logging.{InstanceUUID, RequestID, extendWithRequestIdLogCtx}

class JsonRoutes(
    healthService: HealthService,
    v2Routes: V2Routes,
    shouldLogHttpBodies: Boolean,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with NoTracing {

  // Limit logging of bodies to content with size of less than 10 KiB.
  // Reason is that a char of an UTF-8 string consumes 1 up to 4 bytes such that the string length
  // with this limit will be 2560 chars up to 10240 chars. This can hold already the whole cascade
  // of import statements in this file, which I would consider already as very big string to log.
  private final val maxBodySizeForLogging = Math.pow(2, 10) * 10

  private def responseToRoute(res: Future[HttpResponse]): Route = _ => res map Complete.apply
  private def mkRequestLogMsg(request: HttpRequest, remoteAddress: RemoteAddress) =
    s"Incoming ${request.method.value} request on ${request.uri} from $remoteAddress"

  private def mkResponseLogMsg(statusCode: StatusCode) =
    s"Responding to client with HTTP $statusCode"

  // Always put this directive after a path to ensure
  // that you don't log request bodies multiple times (simply because a matching test was made multiple times).
  // TL;DR JUST PUT THIS THING AFTER YOUR FINAL PATH MATCHING
  private def logRequestResponseHelper(
      logIncomingRequest: (HttpRequest, RemoteAddress) => HttpRequest,
      logResponse: HttpResponse => HttpResponse,
  ): Directive0 =
    extractClientIP flatMap { remoteAddress =>
      mapRequest(request => logIncomingRequest(request, remoteAddress)) & mapRouteResultFuture {
        responseF =>
          for {
            response <- responseF
            transformedResponse <- response match {
              case Complete(httpResponse) =>
                Future.successful(Complete(logResponse(httpResponse)))
              case _ =>
                Future.failed(
                  new RuntimeException(
                    """Logging the request & response should never happen on routes which get rejected.
                    |Make sure to place the directive only at places where a match is guaranteed (e.g. after the path directive).""".stripMargin
                  )
                )
            }
          } yield transformedResponse
      }
    }

  private def logJsonRequestAndResult(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 = {
    def logWithHttpMessageBodyIfAvailable(
        httpMessage: HttpMessage,
        msg: String,
        kind: String,
    ): httpMessage.Self =
      if (
        httpMessage
          .header[`Content-Type`]
          .map(_.contentType)
          .contains(ContentTypes.`application/json`)
      ) {
        def logWithBodyInCtx(body: com.daml.logging.entries.LoggingValue) =
          withEnrichedLoggingContext(
            LoggingContextOf.label[RequestEntity],
            s"${kind}_body" -> body,
          )
            .run(implicit lc => logger.info(s"$msg, ${lc.makeString}"))
        httpMessage.entity.contentLengthOption match {
          case Some(length) if length < maxBodySizeForLogging =>
            import org.apache.pekko.stream.scaladsl.*
            httpMessage
              .transformEntityDataBytes(
                Flow.fromFunction { it =>
                  try logWithBodyInCtx(it.utf8String.parseJson)
                  catch {
                    case NonFatal(ex) =>
                      logger.error(s"Failed to log message body, ${lc.makeString}: ", ex)
                  }
                  it
                }
              )
          case other =>
            val reason = other
              .map(length => s"size of $length is too big for logging")
              .getOrElse {
                if (httpMessage.entity.isChunked())
                  "is chunked & overall size is unknown"
                else
                  "size is unknown"
              }
            logWithBodyInCtx(s"omitted because $kind body $reason")
            httpMessage.self
        }
      } else {
        logger.info(s"$msg, ${lc.makeString}")
        httpMessage.self
      }
    logRequestResponseHelper(
      (request, remoteAddress) =>
        logWithHttpMessageBodyIfAvailable(
          request,
          mkRequestLogMsg(request, remoteAddress),
          "request",
        ),
      httpResponse =>
        logWithHttpMessageBodyIfAvailable(
          httpResponse,
          mkResponseLogMsg(httpResponse.status),
          "response",
        ),
    )
  }

  def logRequestAndResultSimple(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Directive0 =
    logRequestResponseHelper(
      (request, remoteAddress) => {
        logger.info(s"${mkRequestLogMsg(request, remoteAddress)}, ${lc.makeString}")
        request
      },
      httpResponse => {
        logger.info(s"${mkResponseLogMsg(httpResponse.status)}, ${lc.makeString}")
        httpResponse
      },
    )
  val logRequestAndResultFn: LoggingContextOf[InstanceUUID with RequestID] => Directive0 =
    if (shouldLogHttpBodies) lc => logJsonRequestAndResult(lc)
    else lc => logRequestAndResultSimple(lc)

  def logRequestAndResult(implicit lc: LoggingContextOf[InstanceUUID with RequestID]): Directive0 =
    logRequestAndResultFn(lc)

  def all(implicit
      lc0: LoggingContextOf[InstanceUUID]
  ): Route = extractRequest apply { _ =>
    implicit val lc: LoggingContextOf[InstanceUUID with RequestID] =
      extendWithRequestIdLogCtx(identity)(lc0)
    val markThroughputAndLogProcessingTime: Directive0 = Directive { (fn: Unit => Route) =>
      val t0 = System.nanoTime
      fn(()).andThen { res =>
        res.onComplete(_ =>
          logger.trace(s"Processed request after ${System.nanoTime() - t0}ns, ${lc.makeString}")
        )
        res
      }
    }
    def path[L](pm: PathMatcher[L]) =
      server.Directives.path(pm) & markThroughputAndLogProcessingTime & logRequestAndResult
    concat(
      path("livez") apply responseToRoute(Future.successful(HttpResponse(status = StatusCodes.OK))),
      path("readyz") apply responseToRoute(healthService.ready().map(_.toHttpResponse)),
      v2Routes.combinedRoutes,
    )
  }
}

object JsonRoutes {
  type ET[A] = EitherT[Future, Error, A]
}
