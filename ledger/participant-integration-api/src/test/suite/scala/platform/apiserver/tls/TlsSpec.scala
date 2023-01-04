// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.tls

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.apiserver.LedgerApiService
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
  val invalidClientCrt = resource("ca_alternative.crt")
  val invalidClientKey = resource("ca_alternative.pem")

  classOf[LedgerApiService].getSimpleName when {
    "client authorization is set to none" should {
      "allow TLS connections with valid certificates" in {
        assertResponseSuccess(Some(clientCrt), Some(clientKey), ClientAuth.NONE)
      }

      "allow TLS connections without certificates" in {
        assertResponseSuccess(None, None, ClientAuth.NONE)
      }

      "allow TLS connections with invalid certificates" in {
        assertResponseSuccess(Some(invalidClientCrt), Some(invalidClientKey), ClientAuth.NONE)
      }
    }

    "client authorization is set to optional" should {
      "allow TLS connections with valid certificates" in {
        assertResponseSuccess(Some(clientCrt), Some(clientKey), ClientAuth.OPTIONAL)
      }

      "allow TLS connections without certificates" in {
        assertResponseSuccess(None, None, ClientAuth.OPTIONAL)
      }

      "block TLS connections with invalid certificates" in {
        assertResponseUnavailable(
          Some(invalidClientCrt),
          Some(invalidClientKey),
          ClientAuth.OPTIONAL,
        )
      }
    }

    "client authorization is set to require" should {
      "allow TLS connections with valid certificates" in {
        assertResponseSuccess(Some(clientCrt), Some(clientKey), ClientAuth.REQUIRE)
      }

      "block TLS connections without certificates" in {
        assertResponseUnavailable(None, None, ClientAuth.REQUIRE)
      }

      "block TLS connections with invalid certificates" in {
        assertResponseUnavailable(
          Some(invalidClientCrt),
          Some(invalidClientKey),
          ClientAuth.REQUIRE,
        )
      }
    }
  }

  private def makeARequest(
      clientCrt: Option[File],
      clientKey: Option[File],
      clientAuth: ClientAuth,
  ) =
    TlsFixture(tlsEnabled = true, serverCrt, serverKey, caCrt, clientCrt, clientKey, clientAuth)
      .makeARequest()

  private def assertResponseSuccess(
      clientCrt: Option[File],
      clientKey: Option[File],
      clientAuth: ClientAuth,
  ) =
    makeARequest(clientCrt, clientKey, clientAuth).map(_ => succeed)

  private def assertResponseUnavailable(
      clientCrt: Option[File],
      clientKey: Option[File],
      clientAuth: ClientAuth,
  ) =
    makeARequest(clientCrt, clientKey, clientAuth).failed
      .collect {
        case com.daml.grpc.GrpcException.UNAVAILABLE() =>
          succeed
        case ex =>
          fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
      }

}

object TlsSpec {

  protected def resource(src: String) =
    new File(rlocation("ledger/test-common/test-certificates/" + src))

}
