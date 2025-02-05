// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.SingleUseCell

import scala.collection.concurrent.TrieMap

/** Read-only interface to the current map of which synchronizers we're connected to. */
trait ConnectedSynchronizersLookup {
  def get(synchronizerId: SynchronizerId): Option[ConnectedSynchronizer]

  def isConnected(synchronizerId: SynchronizerId): Boolean = get(synchronizerId).nonEmpty

  def snapshot: collection.Map[SynchronizerId, ConnectedSynchronizer]
}

object ConnectedSynchronizersLookup {
  def create(
      connected: TrieMap[SynchronizerId, ConnectedSynchronizer]
  ): ConnectedSynchronizersLookup =
    new ConnectedSynchronizersLookup {
      override def get(synchronizerId: SynchronizerId): Option[ConnectedSynchronizer] =
        connected.get(synchronizerId)

      override def snapshot: collection.Map[SynchronizerId, ConnectedSynchronizer] =
        connected.readOnlySnapshot()
    }
}

class ConnectedSynchronizersLookupContainer extends ConnectedSynchronizersLookup {

  private val delegateCell: SingleUseCell[ConnectedSynchronizersLookup] =
    new SingleUseCell[ConnectedSynchronizersLookup]

  def registerDelegate(delegate: ConnectedSynchronizersLookup): Unit =
    delegateCell
      .putIfAbsent(delegate)
      .foreach(_ => throw new IllegalStateException("Already registered delegate"))

  private def tryGetDelegate: ConnectedSynchronizersLookup =
    delegateCell.getOrElse(
      throw new IllegalStateException("Not yet registered")
    )

  override def get(synchronizerId: SynchronizerId): Option[ConnectedSynchronizer] =
    tryGetDelegate.get(synchronizerId)

  override def snapshot: collection.Map[SynchronizerId, ConnectedSynchronizer] =
    tryGetDelegate.snapshot
}
