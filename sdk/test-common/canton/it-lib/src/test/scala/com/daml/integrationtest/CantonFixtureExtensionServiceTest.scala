// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package integrationtest

import com.daml.ports.LockedFreePort
import com.sun.net.httpserver.HttpServer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

final class CantonFixtureExtensionServiceTest
    extends AsyncWordSpec
    with CantonFixture
    with Matchers {

  private val lockedPort = LockedFreePort.find()
  private val versionRequests = new AtomicInteger(0)

  // An in-process stand-in for an extension service, up before Canton starts.
  private val server: HttpServer = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", lockedPort.port.value), 0)
    server.createContext(
      "/api/v1/version",
      exchange => {
        versionRequests.incrementAndGet()
        val body =
          """{"application": "canton-fixture-test", "version": "v1"}"""
            .getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, body.length.toLong)
        exchange.getResponseBody.write(body)
        exchange.close()
      },
    )
    server.start()
    server
  }

  override protected lazy val extensionServices: Seq[CantonConfig.ExtensionService] = Seq(
    CantonConfig.ExtensionService(
      extensionId = "test-extension",
      address = "127.0.0.1",
      port = lockedPort.port,
      validateOnStartup = true,
    )
  )

  override protected def afterAll(): Unit = {
    server.stop(0)
    lockedPort.unlock()
    super.afterAll()
  }

  "extensionServices" should {
    "configure the extension service on the participant" in {
      // Startup validation only lets Canton come up (giving us ports) after it
      // successfully pinged the configured service's version endpoint.
      ports should not be empty
      versionRequests.get() should be >= 1
    }
  }
}
