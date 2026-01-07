// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.crypto.{KeyPurpose, SigningKeyUsage}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllButNamespaceDelegations
import com.digitalasset.canton.util.FutureInstances.*

import scala.concurrent.Future
import scala.concurrent.duration.*

private[topology] class KeyRotationViaNamespaceDelegationChaos(val logger: TracedLogger)
    extends TopologyOperations
    with IdleTimeSupport {

  override def name: String = "KeyRotationChaos"

  override def companion: TopologyOperationsCompanion = KeyRotationChaos

  override def runTopologyChanges()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Future[Unit] = {
    import env.*
    participants.local
      .parTraverse_(participant => Future(keyRotation(participant)))
  }

  def keyRotation(node: ParticipantReference)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
  ): Unit = {
    import env.*
    whenNodeReadyAndActive(node, KeyRotationChaos.idleTime) {
      withOperation_("keyRotationViaNamespaceDelegation")(s"rotating key for node ${node.name}") {
        // create a new namespace intermediate key
        val intermediateKey =
          node.keys.secret.generate_signing_key(usage = SigningKeyUsage.NamespaceOnly)

        // Create a namespace delegation for the intermediate key with the namespace root key
        node.topology.namespace_delegations.propose_delegation(
          node.namespace,
          intermediateKey,
          CanSignAllButNamespaceDelegations,
        )

        val newKey =
          node.keys.secret.generate_signing_key(usage = SigningKeyUsage.ProtocolOnly)
        val currentKey = getCurrentKey(node, KeyPurpose.Signing)

        node.topology.owner_to_key_mappings.add_key(
          key = newKey.fingerprint,
          purpose = KeyPurpose.Signing,
          signedBy = Seq(intermediateKey.fingerprint, newKey.fingerprint),
        )

        node.topology.owner_to_key_mappings.remove_key(
          key = currentKey.fingerprint,
          purpose = KeyPurpose.Signing,
          signedBy = Seq(intermediateKey.fingerprint),
        )

        // Remove the previous intermediate key
        node.topology.namespace_delegations.propose_revocation(
          node.namespace,
          intermediateKey,
        )
      }
      node.health.ping(node.id, timeout = 60.seconds)
    }
  }
}

object KeyRotationChaos extends TopologyOperationsCompanion {

  val idleTime: FiniteDuration = 25.seconds

  override def acceptableLogEntries: Seq[String] = Seq(
    "periodic acknowledgement failed",
    "Request failed for sequencer",
  )

  override def acceptableNonRetryableLogEntries: Seq[String] = Seq(
    // Wrong key
    "used to generate signature is not a valid key for"
  )
}
