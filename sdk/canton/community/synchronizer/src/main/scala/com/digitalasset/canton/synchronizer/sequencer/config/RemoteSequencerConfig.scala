// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.{ClientConfig, NodeConfig, SequencerConnectionConfig}

final case class RemoteSequencerConfig(
    adminApi: ClientConfig,
    publicApi: SequencerConnectionConfig.Grpc,
    grpcHealth: Option[ClientConfig] = None,
    token: Option[String] = None,
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}
