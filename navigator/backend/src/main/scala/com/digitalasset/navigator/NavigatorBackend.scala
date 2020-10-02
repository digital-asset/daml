// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import java.nio.file.{Files, Paths}
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.daml.buildinfo.BuildInfo

object NavigatorBackend extends UIBackend {

  private val configFile = "frontend-config.js"
  override def customEndpoints: Set[CustomEndpoint[_]] = Set()
  override def customRoutes: List[Route] = List(frontendConfigRoute)
  override def applicationInfo: ApplicationInfo = ApplicationInfo(
    id = s"Navigator-${UUID.randomUUID().toString}",
    name = "Navigator",
    version = BuildInfo.Version,
  )
  override def banner: Option[String] =
    Some(
      raw"""   _  __          _           __
        |  / |/ /__ __  __(_)__ ____ _/ /____  ____
        | /    / _ `/ |/ / / _ `/ _ `/ __/ _ \/ __/
        |/_/|_/\_,_/|___/_/\_, /\_,_/\__/\___/_/
        |                 /___/
        |Version """.stripMargin + applicationInfo.version
    )

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
