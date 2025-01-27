// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.crypto.{SigningPrivateKey, SigningPublicKey}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime

final case class SimulationTopologyData(
    onboardingTime: TopologyActivationTime,
    signingPublicKey: SigningPublicKey,
    signingPrivateKey: SigningPrivateKey,
)
