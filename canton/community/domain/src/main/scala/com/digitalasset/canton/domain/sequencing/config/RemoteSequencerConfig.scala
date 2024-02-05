// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.{ClientConfig, NodeConfig, SequencerConnectionConfig}

final case class RemoteSequencerConfig(
    adminApi: ClientConfig,
    publicApi: SequencerConnectionConfig.Grpc,
    grpcHealth: Option[ClientConfig] = None,
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}
