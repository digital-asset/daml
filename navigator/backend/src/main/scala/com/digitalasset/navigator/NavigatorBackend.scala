// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator

import java.nio.file.{Files, Paths}
import java.util.UUID

import scala.io.Source

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

object NavigatorBackend extends UIBackend {

  private val configFile = "frontend-config.js"
  override def customEndpoints: Set[CustomEndpoint[_]] = Set()
  override def customRoutes: List[Route] = List(frontendConfigRoute)
  override def applicationInfo: ApplicationInfo = ApplicationInfo(
    id = s"Navigator-${UUID.randomUUID().toString}",
    name = "Navigator",
    version = Source.fromResource("COMPONENT-VERSION").mkString("").trim(),
    revision = Source.fromResource(".git-revision").mkString("").trim()
  )
  override def banner: Option[String] =
    Some(
      Source
        .fromResource("banner.txt")
        .getLines
        .mkString("\n") + "\nVersion " + applicationInfo.version)

  /** Frontend config file */
  private val frontendConfigRoute: Route = {
    path("api" / "config") {
      if (Files.exists(Paths.get(configFile)))
        getFromFile(configFile)
      else
        complete(StatusCodes.NotFound)
    }
  }
}
