// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.console.{InstanceReference, SequencerReference}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.PhysicalSynchronizerId

trait OnboardsNewSequencerNode {

  protected val isBftSequencer: Boolean = false

  protected def setUpAdditionalConnections(
      existingSequencer: SequencerReference,
      newSequencer: SequencerReference,
  ): Unit = ()

  protected def onboardNewSequencer(
      synchronizerId: PhysicalSynchronizerId,
      newSequencerReference: SequencerReference,
      existingSequencerReference: SequencerReference,
      synchronizerOwners: Set[InstanceReference],
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Split into 2 branches for nicer documentation
    if (isBftSequencer) {
      // user-manual-entry-begin: DynamicallyOnboardBftSequencer
      bootstrap
        .onboard_new_sequencer(
          synchronizerId.logical,
          newSequencerReference,
          existingSequencerReference,
          synchronizerOwners,
          isBftSequencer = true,
        )
      // user-manual-entry-end: DynamicallyOnboardBftSequencer
    } else {
      bootstrap
        .onboard_new_sequencer(
          synchronizerId.logical,
          newSequencerReference,
          existingSequencerReference,
          synchronizerOwners,
        )
    }

    setUpAdditionalConnections(existingSequencerReference, newSequencerReference)

    // user-manual-entry-begin: DynamicallyOnboardBftSequencer-wait-for-initialized
    newSequencerReference.health.wait_for_initialized()
    // user-manual-entry-end: DynamicallyOnboardBftSequencer-wait-for-initialized
  }
}
