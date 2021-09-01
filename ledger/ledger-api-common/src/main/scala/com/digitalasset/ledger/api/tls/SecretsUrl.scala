// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.io.InputStream
import java.net.URL
import java.nio.file.{Files, Path}

// This trait is not sealed so we can replace it with a fake in tests.
trait SecretsUrl {
  def openStream(): InputStream
}

object SecretsUrl {
  def fromString(string: String): SecretsUrl = new FromUrl(new URL(string))

  def fromUrl(url: URL): SecretsUrl = new FromUrl(url)

  def fromPath(path: Path): SecretsUrl = new FromPath(path)

  private final class FromUrl(url: URL) extends SecretsUrl {
    override def openStream(): InputStream = url.openStream()
  }

  private final class FromPath(path: Path) extends SecretsUrl {
    override def openStream(): InputStream = Files.newInputStream(path)
  }
}
