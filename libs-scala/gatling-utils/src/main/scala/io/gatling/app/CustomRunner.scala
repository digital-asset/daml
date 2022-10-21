// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package io.gatling.app

import java.nio.file.{FileSystems, Path}
import akka.actor.ActorSystem
import com.daml.scalautil.Statement.discard
import com.typesafe.scalalogging.StrictLogging
import io.gatling.app.ConfigOverrides
import io.gatling.app.Runner
import io.gatling.commons.util.DefaultClock
import io.gatling.core.config.GatlingConfiguration
import io.netty.channel.EventLoopGroup

import scala.util.Try

object CustomRunner extends StrictLogging {

  // Copies the io.gatling.app.Gatling start method, which is Copyright 2011-2019
  // GatlingCorp (https://gatling.io) under the Apache 2.0 license (http://www.apache.org/licenses/LICENSE-2.0)
  // This derivation returns the results directory of the run for additional post-processing.
  def runWith(
      system: ActorSystem,
      eventLoop: EventLoopGroup,
      overrides: ConfigOverrides,
      mbSimulation: Option[SimulationClass] = None,
  ): Try[(Int, Path)] = {
    logger.trace("Starting")

    // workaround for deadlock issue, see https://github.com/gatling/gatling/issues/3411
    discard { FileSystems.getDefault }

    val configuration = GatlingConfiguration.load(overrides)
    logger.trace("Configuration loaded")

    val runResult = Try {
      new Runner(system, eventLoop, new DefaultClock, configuration).run(mbSimulation)
    }

    runResult map { res =>
      val status = new RunResultProcessor(configuration).processRunResult(res).code
      (status, configuration.core.directory.results.resolve(res.runId))
    }
  }
}
