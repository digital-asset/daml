// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.io.InputStream
import java.net.URL
import java.nio.file.Path

// This trait is not sealed so we can replace it with a fake in tests.
trait SecretsUrl {
  def openStream(): InputStream
}

object SecretsUrl {
  def fromString(string: String): SecretsUrl = new FromUrl(new URL(string))

  def fromPath(path: Path): SecretsUrl = new FromUrl(path.toUri.toURL)

  def fromUrl(url: URL): SecretsUrl = new FromUrl(url)

  final case class FromUrl(url: URL) extends SecretsUrl {
    override def openStream(): InputStream = url.openStream()
  }
}
