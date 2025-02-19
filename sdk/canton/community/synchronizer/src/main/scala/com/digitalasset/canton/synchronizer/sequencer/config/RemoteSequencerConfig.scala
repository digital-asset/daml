// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  ClientConfig,
  FullClientConfig,
  NodeConfig,
  SequencerApiClientConfig,
  UniformCantonConfigValidation,
}

final case class RemoteSequencerConfig(
    adminApi: FullClientConfig,
    publicApi: SequencerApiClientConfig,
    grpcHealth: Option[FullClientConfig] = None,
    token: Option[String] = None,
) extends NodeConfig
    with UniformCantonConfigValidation {
  override def clientAdminApi: ClientConfig = adminApi
}

object RemoteSequencerConfig {
  implicit val remoteSequencerConfigCantonConfigValidator
      : CantonConfigValidator[RemoteSequencerConfig] =
    CantonConfigValidatorDerivation[RemoteSequencerConfig]
}
