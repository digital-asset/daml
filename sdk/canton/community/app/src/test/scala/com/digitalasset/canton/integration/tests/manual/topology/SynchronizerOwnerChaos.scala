// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalInstanceReference, SequencerReference}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.topology.transaction.DecentralizedNamespaceDefinition

/*
This class provides helpers to add and remove synchronizer owners from a synchronizer namespace and to
modify the synchronizer owner threshold.
The chaos keeps its own lists of potential and current owners. Nodes move between these
lists in exclusive operations, where each will only be touched by a single op at a time.
All operations first run through the list of the required authorizers of a transaction and
propose a change. After that, current synchronizer definition is read until a change is observed.
Depending on if the change is in line with the desired effect, the operation is considered
a success or a failure. This is reflected in the corresponding changes to current and
potential owners.
Add synchronizer owner:
- Picks a node from the potential owner pool and adds it to the synchronizer owners
Remove synchronizer owner:
- Picks a node from the current owner pool and removes it from the synchronizer owners
- If needed, it also reduces the threshold so that it isn't greater than the number of owners
Modify synchronizer owner threshold:
- Change the current threshold by incrementing it modulo the number of current owners
Possible extensions:
- add an operation that augments the notion of all nodes by scanning the environment
- setup with multiple live mediators
- setup with multiple synchronizers
- add and remove an owner in a single operation
- add/remove multiple owners in a single operation
 */
private[topology] class SynchronizerOwnerChaos(
    val operationConcurrency: Int,
    val logger: TracedLogger,
) extends NamespaceChaos {

  override def name: String = "SynchronizerOwnerChaos"

  override val namespacePurpose: String = "synchronizer"

  import TopologyOperations.*

  override def initialization()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = {
    val initializedNodes = env.nodes.local.filter(_.is_initialized).toList
    val (_, namespace) = getNamespaceDefinition(env.sequencer1)
    val (owners, nonOwners) =
      initializedNodes.partition(node => namespace.owners.contains(node.namespace))
    allNodes.set(initializedNodes)
    currentOwnerNodes.set(owners)
    potentialOwners.set(nonOwners)
  }

  override def getNamespaceDefinition(
      sequencerToConnectTo: SequencerReference,
      node: Option[LocalInstanceReference] = None,
  ): (PositiveInt, DecentralizedNamespaceDefinition) =
    node
      .getOrElse(sequencerToConnectTo)
      .topology
      .decentralized_namespaces
      .list(store = sequencerToConnectTo.synchronizer_id)
      .filter(_.item.namespace == sequencerToConnectTo.synchronizer_id.namespace)
      .map(element => element.context.serial -> element.item)
      .loneElement("get namespace definition")

}
