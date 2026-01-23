// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.digitalasset.canton.BaseTest.eventually
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{LocalInstanceReference, SequencerReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  ParticipantPermission,
}
import com.digitalasset.canton.topology.{Namespace, PartyId, UniqueIdentifier}
import org.scalatest.Inside.inside

import java.util.concurrent.atomic.AtomicReference

/*
This class provides helpers to add and remove participants from a decentralized party namespace
and to modify the decentralized party owner threshold.
The chaos keeps its own lists of potential and current owners. Nodes move between these
lists in exclusive operations, where each will only be touched by a single op at a time.
All operations first run through the list of the required authorizers of a transaction and
propose a change. After that, current namespace definition is read until a change is observed.
Depending on if the change is in line with the desired effect, the operation is considered
a success or a failure. This is reflected in the corresponding changes to current and
potential owners.
Add decentralized party owner:
- Picks a node from the potential owner pool and adds it to the decentralized party owners
Remove decentralized party owner:
- Picks a node from the current owner pool and removes it from the decentralized party owners
- If needed, it also reduces the threshold so that it isn't greater than the number of owners
Modify decentralized party owner threshold:
- Change the current threshold by incrementing it modulo the number of current owners
Possible extensions:
- add an operation that augments the notion of all nodes by scanning the environment
- add and remove an owner in a single operation
- add/remove multiple owners in a single operation
 */
private[topology] class DecentralizedPartyChaos(
    val operationConcurrency: Int,
    partyHint: String,
    participantName: String,
    val logger: TracedLogger,
) extends NamespaceChaos {

  override def name: String = "DecentralizedPartyChaos"

  override def namespacePurpose: String = "party"

  override lazy val reservations: Reservations = Reservations(exclusiveParties = Seq(partyHint))

  protected val decentralizedNamespace = new AtomicReference[Option[Namespace]](None)
  protected val decentralizedParty = new AtomicReference[Option[PartyId]](None)

  import TopologyOperations.*

  override def initialization()(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = {
    val owners: List[LocalInstanceReference] =
      env.participants.local.filter(_.is_initialized).toList
    val nonOwners =
      List[LocalInstanceReference]() ++ env.sequencers.local.filter(_.is_initialized).toList ++
        env.mediators.local.filter(_.is_initialized).toList
    val namespace = createNewNamespace(owners)

    decentralizedParty.set(Some(allocatePartyInNamespace(partyHint, owners, namespace)))
    allNodes.set(owners ++ nonOwners)
    currentOwnerNodes.set(owners)
    potentialOwners.set(nonOwners)
    decentralizedNamespace.set(Some(namespace))
  }

  override def finalAssertions(transactionProgress: TransactionProgress)(implicit
      loggingContext: ErrorLoggingContext,
      env: TestConsoleEnvironment,
      globalReservations: Reservations,
  ): Unit = {
    val participant = env.lp(participantName)
    inside(decentralizedParty.get) { case Some(partyId) =>
      participant.ledger_api.state.acs.of_party(partyId) should not be empty
    }.discard
  }

  protected def createNewNamespace(
      owners: Seq[LocalInstanceReference]
  )(implicit env: TestConsoleEnvironment): Namespace = {
    import env.*
    val decentralizedNamespace = owners
      .map(
        _.topology.decentralized_namespaces
          .propose_new(
            owners = owners.map(_.namespace).toSet,
            threshold = PositiveInt.tryCreate(1),
            store = daId,
          )
          .transaction
          .mapping
      )
      .toSet
      .loneElement(s"create new namespace for ${owners.map(_.id.namespace)}")
    owners.foreach { owner =>
      utils.retry_until_true(
        owner.topology.decentralized_namespaces
          .list(daId, filterNamespace = decentralizedNamespace.namespace.filterString)
          .exists(_.context.signedBy.forgetNE.toSet == owners.map(_.fingerprint).toSet)
      )
    }
    decentralizedNamespace.namespace
  }

  protected def allocatePartyInNamespace(
      name: String,
      owners: Seq[LocalInstanceReference],
      namespace: Namespace,
  )(implicit env: TestConsoleEnvironment): PartyId = {
    import env.*

    val partyId = PartyId(UniqueIdentifier.tryCreate(name, namespace))

    val participant = lp(participantName)

    owners.foreach(
      _.topology.party_to_participant_mappings.propose(
        party = partyId,
        newParticipants = List(participant.id -> ParticipantPermission.Submission),
        threshold = PositiveInt.tryCreate(1),
        store = daId,
      )
    )

    eventually() {
      owners.foreach(_.parties.list().map(_.party) should contain(partyId))
    }
    partyId
  }

  override def getNamespaceDefinition(
      sequencerToConnectTo: SequencerReference,
      node: Option[LocalInstanceReference] = None,
  ): (PositiveInt, DecentralizedNamespaceDefinition) = {
    val namespace = decentralizedNamespace
      .get()
      .getOrElse(throw new IllegalStateException(s"Namespace not set in $name"))
    node
      .getOrElse(sequencerToConnectTo)
      .topology
      .decentralized_namespaces
      .list(store = sequencerToConnectTo.synchronizer_id)
      .filter(_.item.namespace == namespace)
      .map(element => element.context.serial -> element.item)
      .loneElement(s"get namespace definition for `$namespace`")
  }

}
