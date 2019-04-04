// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.extractor.config.ConfigParser
import com.digitalasset.extractor.logging.Logging

object Main extends App with Logging {

  log.info("Starting DAML Extractor...")
  log.trace("Parsing config...")

  private val (config, target) = ConfigParser.parse(args).getOrElse {
    log.error("Failed to parse config, exiting...")
    sys.exit(1)
  }

  log.trace(s"Parsed config: ${config}")

  val runner = new Extractor(config, target)

  runner.run()
}
