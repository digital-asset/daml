// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.topology.DomainId

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
