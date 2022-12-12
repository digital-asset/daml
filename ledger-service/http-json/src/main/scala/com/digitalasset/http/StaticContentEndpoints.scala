// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RouteResult.{Complete, Rejected}
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import akka.http.scaladsl.server.{Directives, Rejection, RequestContext, Route, RouteResult}
import com.daml.http.util.Logging.InstanceUUID
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import scalaz.syntax.show._

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.immutable.Seq

object StaticContentEndpoints {
  def all(config: StaticContentConfig)(implicit
      asys: ActorSystem,
      lc: LoggingContextOf[InstanceUUID],
      ec: ExecutionContext,
  ): Route = (ctx: RequestContext) =>
    new StaticContentRouter(config)
      .andThen(
        _ map Complete
      )
      .applyOrElse[HttpRequest, Future[RouteResult]](
        ctx.request,
        _ => Future(Rejected(Seq.empty[Rejection])),
      )
}

private class StaticContentRouter(config: StaticContentConfig)(implicit
    asys: ActorSystem,
    lc: LoggingContextOf[InstanceUUID],
) extends PartialFunction[HttpRequest, Future[HttpResponse]] {

  private[this] val logger = ContextualizedLogger.get(getClass)

  private val pathPrefix: Uri.Path = Uri.Path("/" + config.prefix)

  logger.warn(s"StaticContentRouter configured: ${config.shows}")
  logger.warn("DO NOT USE StaticContentRouter IN PRODUCTION, CONSIDER SETTING UP REVERSE PROXY!!!")

  private val fn =
    akka.http.scaladsl.server.Route.toFunction(
      Directives.rawPathPrefix(Slash ~ config.prefix)(
        Directives.getFromDirectory(config.directory.getAbsolutePath)
      )
    )

  override def isDefinedAt(x: HttpRequest): Boolean =
    x.uri.path.startsWith(pathPrefix)

  override def apply(x: HttpRequest): Future[HttpResponse] =
    fn(x)
}
