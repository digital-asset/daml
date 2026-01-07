// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.crypto.{KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.Future
import scala.concurrent.duration.*

private[topology] class KeyRotationViaOwnerToKeyChaos(val logger: TracedLogger)
    extends TopologyOperations
    with IdleTimeSupport {

  override def name: String = "KeyRotationViaOwnerToKeyChaos"

  override def companion: TopologyOperationsCompanion = KeyRotationChaos

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*
    participants.local.parTraverse_(participant => Future(keyRotationViaOwnerToKey(participant)))
  }
  def keyRotationViaOwnerToKey(node: ParticipantReference)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Unit = {
    import env.*
    whenNodeReadyAndActive(node, KeyRotationChaos.idleTime) {
      withOperation_("keyRotationOwnerToKey")(s"rotating key for node ${node.name}") {

        val currentKey = getCurrentKey(node, KeyPurpose.Signing)

        val newKey =
          node.keys.secret.generate_signing_key(usage = SigningKeyUsage.ProtocolOnly)

        node.topology.owner_to_key_mappings.rotate_key(
          node.id.member,
          currentKey,
          newKey,
          None,
        )

        node.health.ping(node.id, timeout = 60.seconds)
      }
    }
  }

}
