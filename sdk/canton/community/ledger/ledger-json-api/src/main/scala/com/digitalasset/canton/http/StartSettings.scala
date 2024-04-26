// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.ledger.api.tls.TlsConfiguration

import java.nio.file.Path

// defined separately from Config so
//  1. it is absolutely lexically apparent what `import startSettings._` means
//  2. avoid incorporating other Config'd things into "the shared args to start"
trait StartSettings {
  val address: String
  val httpPort: Option[Int]
  val portFile: Option[Path]
  val httpsConfiguration:Option[TlsConfiguration]
  val wsConfig: Option[WebsocketConfig]
  val staticContentConfig: Option[StaticContentConfig]
  val debugLoggingOfHttpBodies: Boolean
}

object StartSettings {
  trait Default extends StartSettings {
    override val staticContentConfig: Option[StaticContentConfig] = None
  }
}
