// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.RouteResult.{Complete, Rejected}
import org.apache.pekko.http.scaladsl.server.directives.ContentTypeResolver.Default
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.util.Logging.InstanceUUID
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import scalaz.syntax.show.*

import scala.concurrent.{ExecutionContext, Future}

object StaticContentEndpoints {
  def all(config: StaticContentConfig, loggerFactory: NamedLoggerFactory)(implicit
      asys: ActorSystem,
      lc: LoggingContextOf[InstanceUUID],
      ec: ExecutionContext,
  ): Route = (ctx: RequestContext) =>
    new StaticContentRouter(config, loggerFactory)
      .andThen(
        _ map Complete
      )
      .applyOrElse[HttpRequest, Future[RouteResult]](
        ctx.request,
        _ => Future(Rejected(Seq.empty[Rejection])),
      )
}

private class StaticContentRouter(
    config: StaticContentConfig,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    asys: ActorSystem,
    lc: LoggingContextOf[InstanceUUID],
) extends PartialFunction[HttpRequest, Future[HttpResponse]]
    with NamedLogging
    with NoTracing {

  private val pathPrefix: Uri.Path = Uri.Path("/" + config.prefix)

  logger.warn(s"StaticContentRouter configured: ${config.shows}, ${lc.makeString}")
  logger.warn(
    s"DO NOT USE StaticContentRouter IN PRODUCTION, CONSIDER SETTING UP REVERSE PROXY!!!, ${lc.makeString}"
  )

  private val fn =
    org.apache.pekko.http.scaladsl.server.Route.toFunction(
      Directives.rawPathPrefix(Slash ~ config.prefix)(
        Directives.getFromDirectory(config.directory.getAbsolutePath)
      )
    )

  override def isDefinedAt(x: HttpRequest): Boolean =
    x.uri.path.startsWith(pathPrefix)

  override def apply(x: HttpRequest): Future[HttpResponse] =
    fn(x)
}
