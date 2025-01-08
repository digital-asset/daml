// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.health

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpResponse, ResponseEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.DebuggingDirectives

class HttpHealthServer(
    service: DependenciesHealthService,
    address: String,
    port: Port,
    protected override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit system: ActorSystem)
    extends FlagCloseableAsync
    with NamedLogging {

  private val binding = {
    import TraceContext.Implicits.Empty.*
    timeouts.unbounded.await(s"Binding the health server")(
      Http().newServerAt(address, port.unwrap).bind(HttpHealthServer.route(service))
    )
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    List[AsyncOrSyncCloseable](
      AsyncCloseable("binding", binding.unbind(), timeouts.shutdownNetwork)
    )
  }
}

object HttpHealthServer {

  /** Routes for powering the health server.
    * Provides:
    *   GET /health => calls check and returns:
    *     200 if healthy
    *     503 if unhealthy
    *     500 if the check fails
    */
  @VisibleForTesting
  private[health] def route(service: DependenciesHealthService): Route = {
    def renderStatus(status: Seq[ComponentStatus]): ResponseEntity = HttpEntity(
      status.map(_.toString).mkString("\n")
    )
    implicit val _marshaller: ToResponseMarshaller[(ServingStatus, Seq[ComponentStatus])] =
      Marshaller.opaque {
        case (ServingStatus.SERVING, status) =>
          HttpResponse(status = StatusCodes.OK, entity = renderStatus(status))
        case (ServingStatus.NOT_SERVING, status) =>
          HttpResponse(status = StatusCodes.ServiceUnavailable, entity = renderStatus(status))
        case (_, status) =>
          HttpResponse(status = StatusCodes.InternalServerError, entity = renderStatus(status))
      }

    get {
      path("health") {
        DebuggingDirectives.logRequest("health-request") {
          DebuggingDirectives.logRequestResult("health-request-response") {
            complete((service.getState, service.dependencies.map(_.toComponentStatus)))
          }
        }
      }
    }
  }
}
