// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.{
  ClientConfig,
  FullClientConfig,
  NodeConfig,
  SequencerApiClientConfig,
}

final case class RemoteSequencerConfig(
    adminApi: FullClientConfig,
    publicApi: SequencerApiClientConfig,
    grpcHealth: Option[FullClientConfig] = None,
    token: Option[String] = None,
) extends NodeConfig {
  override def clientAdminApi: ClientConfig = adminApi
}
