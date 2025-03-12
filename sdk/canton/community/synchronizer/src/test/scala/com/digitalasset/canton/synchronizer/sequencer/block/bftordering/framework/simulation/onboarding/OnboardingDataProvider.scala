// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

trait OnboardingDataProvider[T] {
  def provide(forNode: BftNodeId): T
}

object EmptyOnboardingDataProvider extends OnboardingDataProvider[Unit] {
  override def provide(forNode: BftNodeId): Unit = ()
}
