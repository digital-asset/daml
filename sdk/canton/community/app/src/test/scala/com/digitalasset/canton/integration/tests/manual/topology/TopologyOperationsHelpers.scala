// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.topology

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{InstanceReference, SequencerReference}
import com.digitalasset.canton.crypto.{KeyPurpose, PublicKey}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}

import scala.language.implicitConversions

trait TopologyOperationsHelpers {

  import TopologyOperations.*

  implicit protected def physicalToLogical(synchronizerId: PhysicalSynchronizerId): SynchronizerId =
    synchronizerId.logical

  protected def getSynchronizerOwners(
      synchronizerId: PhysicalSynchronizerId,
      node: InstanceReference,
  )(implicit env: TestConsoleEnvironment): NonEmpty[Set[InstanceReference]] = {
    import env.*

    val ownerNamespaces = node.topology.decentralized_namespaces
      .list(
        store = synchronizerId,
        filterNamespace = synchronizerId.namespace.filterString,
        timeQuery = TimeQuery.Snapshot(environment.clock.now),
      )
      .flatMap(_.item.owners)
      .toSet

    val owners = ownerNamespaces.flatMap { namespace =>
      nodes.all.collectFirst {
        case instanceRef if instanceRef.maybeId.exists(_.namespace == namespace) => instanceRef
      }
    }

    NonEmpty
      .from(owners)
      .getOrElse(
        throw new IllegalStateException(
          s"No synchronizer owners found for synchronizer $synchronizerId"
        )
      )
  }

  protected def getSynchronizerSequencers(
      synchronizerId: PhysicalSynchronizerId,
      synchronizersMember: InstanceReference,
  )(implicit
      env: TestConsoleEnvironment
  ): (NonEmpty[Seq[SequencerReference]], PositiveInt) = {
    import env.*

    val sequencerSynchronizerState = synchronizersMember.topology.sequencers
      .list(
        store = synchronizerId,
        timeQuery = TimeQuery.Snapshot(environment.clock.now),
      )
      .loneElement(s"get sequencers for synchronizer `$synchronizerId`")
      .item

    val sequencerNodes = sequencerSynchronizerState.active.map { sequencerId =>
      sequencers.all
        .find(_.maybeId.contains(sequencerId))
        .getOrElse(
          throw new IllegalStateException(
            s"No sequencer $sequencerId found on synchronizer $synchronizerId"
          )
        )
    }

    (sequencerNodes, sequencerSynchronizerState.threshold)
  }

  protected def getCurrentKey(node: InstanceReference, purpose: KeyPurpose): PublicKey =
    node.topology.owner_to_key_mappings
      .list(
        store = Some(TopologyStoreId.Authorized),
        filterKeyOwnerUid = node.id.filterString,
      )
      .find(_.item.member == node.id)
      .getOrElse(sys.error(s"No keys found for $node"))
      .item
      .keys
      .find(_.purpose == purpose)
      .getOrElse(sys.error(s"No ${purpose.name} keys found for $node"))
}
