// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.SingleUseCell

import scala.collection.concurrent.TrieMap

/** Read-only interface to the current map of which domains we're connected to. */
trait ConnectedDomainsLookup {
  def get(synchronizerId: SynchronizerId): Option[SyncDomain]

  def isConnected(synchronizerId: SynchronizerId): Boolean = get(synchronizerId).nonEmpty

  def snapshot: collection.Map[SynchronizerId, SyncDomain]
}

object ConnectedDomainsLookup {
  def create(connected: TrieMap[SynchronizerId, SyncDomain]): ConnectedDomainsLookup =
    new ConnectedDomainsLookup {
      override def get(synchronizerId: SynchronizerId): Option[SyncDomain] =
        connected.get(synchronizerId)

      override def snapshot: collection.Map[SynchronizerId, SyncDomain] =
        connected.readOnlySnapshot()
    }
}

class ConnectedDomainsLookupContainer extends ConnectedDomainsLookup {

  private val delegateCell: SingleUseCell[ConnectedDomainsLookup] =
    new SingleUseCell[ConnectedDomainsLookup]

  def registerDelegate(delegate: ConnectedDomainsLookup): Unit =
    delegateCell
      .putIfAbsent(delegate)
      .foreach(_ => throw new IllegalStateException("Already registered delegate"))

  private def tryGetDelegate: ConnectedDomainsLookup =
    delegateCell.getOrElse(
      throw new IllegalStateException("Not yet registered")
    )

  override def get(synchronizerId: SynchronizerId): Option[SyncDomain] =
    tryGetDelegate.get(synchronizerId)

  override def snapshot: collection.Map[SynchronizerId, SyncDomain] =
    tryGetDelegate.snapshot
}
