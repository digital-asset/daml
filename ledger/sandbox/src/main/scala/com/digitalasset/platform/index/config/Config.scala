// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index.config

import java.io.File

import com.digitalasset.api.util.TimeProvider
import com.digitalasset.ledger.api.tls.TlsConfiguration

final case class Config(
    port: Int,
    portFile: Option[File],
    archiveFiles: List[File],
    maxInboundMessageSize: Int,
    timeProvider: TimeProvider, // enables use of non-wall-clock time in tests
    jdbcUrl: String,
    tlsConfig: Option[TlsConfiguration]
)

object Config {
  val DefaultMaxInboundMessageSize = 4194304
  def default: Config =
    new Config(0, None, List.empty, 4194304, TimeProvider.UTC, "", None)
}
