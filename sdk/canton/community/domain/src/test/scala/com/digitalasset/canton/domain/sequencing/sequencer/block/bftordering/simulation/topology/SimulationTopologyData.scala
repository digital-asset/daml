// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.crypto.{SigningPrivateKey, SigningPublicKey}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.TopologyActivationTime

final case class SimulationTopologyData(
    onboardingTime: TopologyActivationTime,
    signingPublicKey: SigningPublicKey,
    signingPrivateKey: SigningPrivateKey,
)
