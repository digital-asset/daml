// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation

/** Client configured options for how to process topology transactions
  *
  * @param useTimeProofsForApproximateTime
  *   Whether the node will use time proofs to observe when an effective time has been reached. If
  *   false, the node will use a more efficient method that leverages the `GetTime` sequencer API.
  */
final case class TopologyClientConfig(
    useTimeProofsForApproximateTime: Boolean = true
) extends UniformCantonConfigValidation

object TopologyClientConfig {
  implicit val sequencerClientConfigCantonConfigValidator
      : CantonConfigValidator[TopologyClientConfig] =
    CantonConfigValidatorDerivation[TopologyClientConfig]
}
