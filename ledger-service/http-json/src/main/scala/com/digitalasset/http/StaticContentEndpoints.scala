// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import akka.http.scaladsl.server.{Directives, RoutingLog}
import akka.http.scaladsl.settings.{ParserSettings, RoutingSettings}
import akka.stream.Materializer
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future

object StaticContentEndpoints extends StrictLogging {
  def all(config: StaticContentConfig)(
      implicit
      routingSettings: RoutingSettings,
      parserSettings: ParserSettings,
      materializer: Materializer,
      routingLog: RoutingLog): HttpRequest PartialFunction Future[HttpResponse] =
    new StaticContentRouter(config)
}

private class StaticContentRouter(config: StaticContentConfig)(
    implicit
    routingSettings: RoutingSettings,
    parserSettings: ParserSettings,
    materializer: Materializer,
    routingLog: RoutingLog)
    extends PartialFunction[HttpRequest, Future[HttpResponse]] {

  private val pathPrefix: Uri.Path = Uri.Path("/" + config.prefix)

  private val fn =
    akka.http.scaladsl.server.Route.asyncHandler(
      Directives.rawPathPrefix(Slash ~ config.prefix)(
        Directives.getFromDirectory(config.directory.getAbsolutePath)
      ))

  override def isDefinedAt(x: HttpRequest): Boolean =
    x.uri.path.startsWith(pathPrefix)

  override def apply(x: HttpRequest): Future[HttpResponse] =
    fn(x)
}
