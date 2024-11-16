// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.crypto.{SigningPrivateKey, SigningPublicKey}
import com.digitalasset.canton.topology.processing.EffectiveTime

final case class SimulationOnboardingInformation(
    onboardingTime: EffectiveTime,
    signingPublicKey: SigningPublicKey,
    signingPrivateKey: SigningPrivateKey,
)
