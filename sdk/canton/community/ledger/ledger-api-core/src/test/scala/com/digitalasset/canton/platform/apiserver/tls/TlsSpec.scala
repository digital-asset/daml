// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.tls

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.resources.TestResourceContext
import com.digitalasset.canton.platform.apiserver.LedgerApiService
import com.digitalasset.canton.util.JarResourceUtils
import io.netty.handler.ssl.ClientAuth
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File

class TlsSpec
    extends AsyncWordSpec
    with TableDrivenPropertyChecks
    with TestResourceContext
    with BaseTest {
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
    TlsFixture(
      loggerFactory,
      tlsEnabled = true,
      serverCrt,
      serverKey,
      caCrt,
      clientCrt,
      clientKey,
      clientAuth,
    )
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
    JarResourceUtils.resourceFile("test-certificates/" + src)

}
