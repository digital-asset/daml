// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.tls

import java.io.File
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.apiserver.LedgerApiServer
import io.netty.handler.ssl.ClientAuth
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AsyncWordSpec

class TlsSpec
    extends AsyncWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with TestResourceContext {
  import TlsSpec.resource

  val serverCrt = resource("server.crt")
  val serverKey = resource("server.pem")
  val caCrt = resource("ca.crt")
  val clientCrt = resource("client.crt")
  val clientKey = resource("client.pem")

  def tlsEnabledFixture(clientCrt: Option[File], clientKey: Option[File], clientAuth: ClientAuth) =
    TlsFixture(tlsEnabled = true, serverCrt, serverKey, caCrt, clientCrt, clientKey, clientAuth)

  classOf[LedgerApiServer].getSimpleName when {
    "client authorization is set to none" should {
      "allow TLS connections with valid certificates" in {
        tlsEnabledFixture(Some(clientCrt), Some(clientKey), ClientAuth.NONE)
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections without certificates" in {
        tlsEnabledFixture(None, None, ClientAuth.NONE)
          .makeARequest()
          .map(_ => succeed)
      }
    }

    "client authorization is set to optional" should {
      "allow TLS connections with valid certificates" in {
        tlsEnabledFixture(Some(clientCrt), Some(clientKey), ClientAuth.OPTIONAL)
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections without certificates" in {
        tlsEnabledFixture(None, None, ClientAuth.OPTIONAL)
          .makeARequest()
          .map(_ => succeed)
      }
    }

    "client authorization is set to require" should {
      "allow TLS connections with valid certificates" in {
        tlsEnabledFixture(Some(clientCrt), Some(clientKey), ClientAuth.REQUIRE)
          .makeARequest()
          .map(_ => succeed)
      }

      "block TLS connections without certificates" in {
        tlsEnabledFixture(None, None, ClientAuth.REQUIRE)
          .makeARequest()
          .failed
          .collect {
            case com.daml.grpc.GrpcException.UNAVAILABLE() =>
              succeed
            case ex =>
              fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
          }
      }
    }
  }
}

object TlsSpec {

  protected def resource(src: String) =
    new File(rlocation("ledger/test-common/test-certificates/" + src))

}
