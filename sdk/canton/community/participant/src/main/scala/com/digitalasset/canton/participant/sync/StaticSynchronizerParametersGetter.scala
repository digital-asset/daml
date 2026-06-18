// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion

trait StaticSynchronizerParametersGetter {
  def staticSynchronizerParameters(
      synchronizerId: PhysicalSynchronizerId
  ): Option[StaticSynchronizerParameters]

  /** Return the latest [[com.digitalasset.canton.topology.PhysicalSynchronizerId]] known for the
    * given [[com.digitalasset.canton.topology.SynchronizerId]].
    *
    * Use cases are:
    *   - submission of a transaction (phase 1)
    *   - repair service
    *   - inspection services
    */
  def latestKnownPSId(synchronizerId: SynchronizerId): Option[PhysicalSynchronizerId]

  /** Return the latest [[com.digitalasset.canton.version.ProtocolVersion]] known for the given
    * [[com.digitalasset.canton.topology.SynchronizerId]].
    *
    * Use cases are:
    *   - submission of a transaction (phase 1)
    *   - repair service
    *   - inspection services
    */
  def latestKnownProtocolVersion(synchronizerId: SynchronizerId): Option[ProtocolVersion] =
    latestKnownPSId(synchronizerId).map(_.protocolVersion)
}
