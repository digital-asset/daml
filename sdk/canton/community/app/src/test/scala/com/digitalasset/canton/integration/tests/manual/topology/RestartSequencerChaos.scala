// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.Future
import scala.concurrent.duration.*

class RestartSequencersChaos(val logger: TracedLogger)
    extends TopologyOperations
    with IdleTimeSupport {

  override def name: String = "RestartSequencersChaos"
  override def companion: TopologyOperationsCompanion = RestartSequencersChaos

  def restartSequencer(
      node: LocalSequencerReference
  )(implicit errorLoggingContext: ErrorLoggingContext, env: TestConsoleEnvironment) =
    whenNodeReadyAndActive(node, 15.seconds) {
      withOperation_("restarting node")(s"${node.name}") {
        node.stop()
        node.start()
      }
    }

  def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*

    sequencers.local.parTraverse_(sequencer => Future(restartSequencer(sequencer)))
  }
}

object RestartSequencersChaos extends TopologyOperationsCompanion {
  override def acceptableLogEntries: Seq[String] = Seq(
    "Is the server running",
    "Failed to process request RequestRefused",
    "Failed to send result to sequencer for request",
    "periodic acknowledgement failed",
    "Sequencing result message timed out",
    "shutdown did not complete gracefully in allotted 3 seconds",
    "Failed to submit submission due to Error(RequestRefused(ShuttingDown(Sequencer shutting down)))",
    "Failed to send responses: RequestRefused(ShuttingDown(Sequencer shutting down))",
    "The operation 'insert block' has failed with an exception",
    "Now retrying operation 'insert block'",
    "Failed to acknowledge clean timestamp (usually because sequencer is down)",
  )
}
