// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.topology.SynchronizerId

/** Used to bootstrap one or more synchronizers at the start of a test.
  */
trait NetworkBootstrap {
  def bootstrap(): Unit
}

/** A data container to hold useful information for initialized synchronizers
  */
final case class InitializedSynchronizer(
    synchronizerId: SynchronizerId,
    staticSynchronizerParameters: StaticSynchronizerParameters,
    synchronizerOwners: Set[InstanceReference],
)
