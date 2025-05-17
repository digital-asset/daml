// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}

trait HasSynchronizerId {
  def synchronizerId: SynchronizerId
}

trait HasPhysicalSynchronizerId {
  def synchronizerId: PhysicalSynchronizerId
}
