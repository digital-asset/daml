// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.participant.store.AcsInspection
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.util.SingleUseCell

/** Read-only interface to the current map of which synchronizers we're connected to. */
trait ConnectedSynchronizersLookup {
  def get(synchronizerId: SynchronizerId): Option[ConnectedSynchronizer]
  def get(synchronizerId: PhysicalSynchronizerId): Option[ConnectedSynchronizer]

  def psidFor(synchronizerId: SynchronizerId): Option[PhysicalSynchronizerId] =
    get(synchronizerId).map(_.psid)

  def getAcsInspection(synchronizerId: SynchronizerId): Option[AcsInspection]

  def isConnected(synchronizerId: PhysicalSynchronizerId): Boolean = get(synchronizerId).nonEmpty
  def isConnected(synchronizerId: SynchronizerId): Boolean

  def isConnectedToAny: Boolean

  def snapshot: Map[PhysicalSynchronizerId, ConnectedSynchronizer]
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

  override def get(synchronizerId: PhysicalSynchronizerId): Option[ConnectedSynchronizer] =
    tryGetDelegate.get(synchronizerId)

  override def getAcsInspection(synchronizerId: SynchronizerId): Option[AcsInspection] =
    tryGetDelegate.getAcsInspection(synchronizerId)

  override def isConnected(synchronizerId: SynchronizerId): Boolean =
    tryGetDelegate.isConnected(synchronizerId)

  override def isConnectedToAny: Boolean = tryGetDelegate.isConnectedToAny

  override def snapshot: Map[PhysicalSynchronizerId, ConnectedSynchronizer] =
    tryGetDelegate.snapshot
}
