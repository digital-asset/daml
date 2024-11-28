// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.util.SingleUseCell

import scala.collection.concurrent.TrieMap

/** Read-only interface to the current map of which domains we're connected to. */
trait ConnectedDomainsLookup {
  def get(domainId: DomainId): Option[SyncDomain]

  def isConnected(domainId: DomainId): Boolean = get(domainId).nonEmpty

  def snapshot: collection.Map[DomainId, SyncDomain]
}

object ConnectedDomainsLookup {
  def create(connected: TrieMap[DomainId, SyncDomain]): ConnectedDomainsLookup =
    new ConnectedDomainsLookup {
      override def get(domainId: DomainId): Option[SyncDomain] = connected.get(domainId)

      override def snapshot: collection.Map[DomainId, SyncDomain] = connected.readOnlySnapshot()
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

  override def get(domainId: DomainId): Option[SyncDomain] =
    tryGetDelegate.get(domainId)

  override def snapshot: collection.Map[DomainId, SyncDomain] =
    tryGetDelegate.snapshot
}
