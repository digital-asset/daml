// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import java.io.File

import org.scalatest.AsyncWordSpec
import org.scalatest.Matchers

class TlsRevokedCertificatesIT extends AsyncWordSpec with Matchers with SandboxWithOCSPFixture {

  override val clientCrt: File = resource("client-revoked.crt")
  override val clientKey: File = resource("client-revoked.pem")

  "The Ledger API Server" should {
    "not allow connections with revoked TLS certificates" in {
      connect().failed
        .collect {
          case com.daml.grpc.GrpcException.UNAVAILABLE() =>
            succeed
          case ex =>
            fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
        }
    }
  }

}
