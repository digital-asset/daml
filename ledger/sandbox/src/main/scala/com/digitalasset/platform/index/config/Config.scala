// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index.config

import java.io.File

import com.digitalasset.ledger.api.tls.TlsConfiguration

final case class Config(
    port: Int,
    portFile: Option[File],
    archiveFiles: List[File],
    jdbcUrl: String,
    tlsConfig: Option[TlsConfiguration]
)

object Config {
  def default: Config =
    new Config(0, None, List.empty, "", None)
}
