// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.extractor.config.ConfigParser
import com.typesafe.scalalogging.StrictLogging

object Main extends App with StrictLogging {

  logger.info("Starting DAML Extractor...")
  logger.trace("Parsing config...")

  private val (config, target) = ConfigParser.parse(args).getOrElse {
    logger.error("Failed to parse config, exiting...")
    sys.exit(1)
  }

  logger.trace(s"Parsed config: ${config}")

  val runner =
    new Extractor(config, target)()

  runner.run()
}
