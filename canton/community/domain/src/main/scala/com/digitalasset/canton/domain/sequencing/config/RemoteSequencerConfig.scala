// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.{ClientConfig, NodeConfig, SequencerConnectionConfig}

sealed trait RemoteSequencerConfig {
  def publicApi: SequencerConnectionConfig.Grpc

  def grpcHealth: Option[ClientConfig]
}

object RemoteSequencerConfig {
  final case class Grpc(
      adminApi: ClientConfig,
      publicApi: SequencerConnectionConfig.Grpc,
      grpcHealth: Option[ClientConfig] = None,
  ) extends RemoteSequencerConfig
      with NodeConfig {
    override def clientAdminApi: ClientConfig = adminApi
  }
}
