// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.Future
import scala.concurrent.duration.*

class RestartParticipantsChaos(val logger: TracedLogger)
    extends TopologyOperations
    with IdleTimeSupport {

  override def name: String = "RestartParticipantsChaos"
  override def companion: TopologyOperationsCompanion = RestartParticipantsChaos

  def restartParticipant(
      node: LocalParticipantReference
  )(implicit errorLoggingContext: ErrorLoggingContext, env: TestConsoleEnvironment): Unit = {
    import env.*
    whenNodeReadyAndActive(node, 5.seconds) {
      withOperation_("restarting node")(s"${node.name}") {
        val registeredSynchronizers = node.synchronizers.list_registered()
        val connectedSynchronizers =
          node.synchronizers.list_connected().map(_.synchronizerAlias)
        node.stop()
        node.start()
        registeredSynchronizers.foreach { case (synchronizerConfig, _, _) =>
          if (connectedSynchronizers.contains(synchronizerConfig.synchronizerAlias)) {
            node.synchronizers.connect_by_config(synchronizerConfig)
          }
        }
        node.health.ping(node.id, 10.seconds)
      }
    }
  }

  def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*

    participants.local.parTraverse_(participant => Future(restartParticipant(participant)))

  }
}

object RestartParticipantsChaos extends TopologyOperationsCompanion {
  override def acceptableLogEntries: Seq[String] = Seq(
    "Failed to submit",
    "Ledger subscription PerformanceRunner",
    "The operation 'restartable-PerformanceRunner'",
    "Now retrying operation 'restartable-PerformanceRunner'",
  )
}
