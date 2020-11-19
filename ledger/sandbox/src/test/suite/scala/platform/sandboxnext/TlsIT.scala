// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import java.io.File

import org.scalatest.AsyncWordSpec
import org.scalatest.Matchers

class TlsIT extends AsyncWordSpec with Matchers with SandboxWithOCSPFixture {

  override val clientCrt: File = resource("client.crt")
  override val clientKey: File = resource("client.pem")

  "The Ledger API Server" should {
    "allow connections with valid TLS certificates" in {
      connect().map { client =>
        client.ledgerId should be(ledgerId)
      }
    }
  }

}
