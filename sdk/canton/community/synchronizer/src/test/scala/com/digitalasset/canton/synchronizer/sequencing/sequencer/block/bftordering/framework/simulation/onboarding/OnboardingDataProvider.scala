// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation.onboarding

import com.digitalasset.canton.topology.SequencerId

trait OnboardingDataProvider[T] {
  def provide(forSequencerId: SequencerId): T
}

object EmptyOnboardingDataProvider extends OnboardingDataProvider[Unit] {
  override def provide(forSequencerId: SequencerId): Unit = ()
}
