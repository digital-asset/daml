// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.console.LocalMediatorReference
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.Future
import scala.concurrent.duration.*

class RestartMediatorsChaos(val logger: TracedLogger)
    extends TopologyOperations
    with IdleTimeSupport {

  override def name: String = "RestartMediatorsChaos"
  override def companion: TopologyOperationsCompanion = RestartMediatorsChaos

  def restartMediator(
      node: LocalMediatorReference
  )(implicit errorLoggingContext: ErrorLoggingContext, env: TestConsoleEnvironment) =
    whenNodeReadyAndActive(node, 20.seconds) {
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

    mediators.local
      .parTraverse_(mediator => Future(restartMediator(mediator)))
  }
}

object RestartMediatorsChaos extends TopologyOperationsCompanion {
  override def acceptableNonRetryableLogEntries: Seq[String] = Seq(
    "MEDIATOR_INVALID_MESSAGE"
  )

  override def acceptableLogEntries: Seq[String] = Seq(
    "The operation 'class com.digitalasset.canton.synchronizer.mediator.store.DbFinalizedResponseStore",
    "Now retrying operation 'class",
  )
}
