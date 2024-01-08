// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.config.{BatchingConfig, LocalNodeParametersConfig, ProtocolConfig}
import com.digitalasset.canton.version.ProtocolVersion

/** Various parameters for non-standard sequencer settings
  *
  * @param willCorruptYourSystemDevVersionSupport if true, then dev version will be turned on, but we will brick this sequencer node if it is used for production.
  * @param dontWarnOnDeprecatedPV if true, then this sequencer will not emit a warning when configured to use protocol version 2.0.0.
  * @param maxBurstFactor how forgiving the rate limit is in case of bursts (so rate limit starts after observing an initial burst of factor * max_rate commands)
  * @param skipTopologyManagerSignatureValidation
  */
final case class SequencerNodeParameterConfig(
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    devVersionSupport: Boolean = true,
    dontWarnOnDeprecatedPV: Boolean = false,
    initialProtocolVersion: ProtocolVersion = ProtocolVersion.latest,
    maxBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.5),
    skipTopologyManagerSignatureValidation: Boolean = false,
    batching: BatchingConfig = BatchingConfig(),
) extends ProtocolConfig
    with LocalNodeParametersConfig
