// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.memory

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.availability.data.{
  AvailabilityStore,
  AvailabilityStoreTest,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem

class InMemoryAvailabilityStoreTest extends AvailabilityStoreTest {
  override def createStore(): AvailabilityStore[PekkoModuleSystem.PekkoEnv] =
    new InMemoryAvailabilityStore()
}
