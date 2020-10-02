// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test.config

import com.typesafe.scalalogging.LazyLogging
import scopt.OptionParser

final case class Arguments(
    navigatorPort: Int = 7500,
    damlPath: String = "",
    navConfPAth: String = "",
    navigatorDir: String = "",
    startComponents: Boolean = false,
    browserStackUser: String = "",
    browserStackKey: String = ""
)

object Arguments extends LazyLogging {

  private val argumentParser =
    new OptionParser[Arguments]("navigator-test") {

      opt[Int]("navigator-port")
        .text("the port the tests will run against, 7500 by default")
        .action((navigatorPort, arguments) => arguments.copy(navigatorPort = navigatorPort))

      opt[String]("daml-path")
        .text(
          "the path to the daml file picked up by the sandbox - if sandbox command is not provided")
        .action((damlPath, arguments) => arguments.copy(damlPath = damlPath))

      opt[String]("nav-conf-path")
        .text(
          "the path to the navigator config file picked up by the backend - if navigator command is not provided")
        .action((navConfPAth, arguments) => arguments.copy(navConfPAth = navConfPAth))

      opt[String]("navigator-dir")
        .text("the folder to run the frontend from if navigator command is not provided")
        .action((navigatorDir, arguments) => arguments.copy(navigatorDir = navigatorDir))

      opt[String]("browserstack-user")
        .text("username to run BrowserStack Automate tests")
        .action(
          (browserStackUser, arguments) => arguments.copy(browserStackUser = browserStackUser))

      opt[String]("browserstack-key")
        .text("api key to run BrowserStack Automate tests")
        .action((browserStackKey, arguments) => arguments.copy(browserStackKey = browserStackKey))

    }

  def parse(arguments: Array[String]): Option[Arguments] = {
    argumentParser.parse(arguments, Arguments()) match {
      case None => None
      case Some(args) =>
        if (args.navigatorDir != "" && args.navConfPAth != "" && args.damlPath != "") {
          Some(args.copy(startComponents = true))
        } else {
          Some(args)
        }
    }
  }

  def showUsage(): Unit =
    argumentParser.showUsage()
}
