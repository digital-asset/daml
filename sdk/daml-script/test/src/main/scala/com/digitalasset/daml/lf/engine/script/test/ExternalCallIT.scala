// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.integrationtest.CantonConfig
import com.daml.ports.LockedFreePort
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.ScriptEngine.defaultCompilerConfig
import com.sun.net.httpserver.HttpServer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import scala.concurrent.Future

// Runs the external-call scripts against a Canton participant configured with
// an in-process test extension service, covering the success path and each
// external-call submit error.
final class ExternalCallIT extends AsyncWordSpec with AbstractScriptTest with Matchers {

  override protected lazy val timeMode: ScriptTimeMode = ScriptTimeMode.WallClock

  // External call is staged at LF 2.4 and its wire data exists from protocol version 36 on.
  override protected lazy val protocolVersion = CantonConfig.ProtocolVersion.Explicit("v36")

  // TODO[#23340]: remove hardcoding
  override lazy val darPath: Path = rlocation(
    Paths.get("daml-script/test/external-call-test-v2.4.dar")
  )
  override lazy val dar: CompiledDar = CompiledDar.read(darPath, defaultCompilerConfig)

  private val lockedPort = LockedFreePort.find()

  // An in-process stand-in for an extension service, up before Canton starts.
  private val server: HttpServer = {
    def respond(exchange: com.sun.net.httpserver.HttpExchange, status: Int, body: String): Unit = {
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      exchange.getResponseBody.write(bytes)
      exchange.close()
    }
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", lockedPort.port.value), 0)
    val _ = server.createContext(
      "/api/v1/version",
      exchange =>
        respond(exchange, 200, """{"application": "external-call-test", "version": "v1"}"""),
    )
    val _ = server.createContext(
      "/api/v1/external-call",
      exchange => {
        val input = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
        exchange.getRequestHeaders.getFirst("X-Daml-External-Function-Id") match {
          // Echo verbatim: the round-tripped output witnesses that
          // DA.ExternalCall's asciiToLower canonicalization reached the wire.
          case "echo" => respond(exchange, 200, input)
          case "fail" => respond(exchange, 400, "external-call test forced failure")
          case "invalid-output" => respond(exchange, 200, "not-hex")
          case other => respond(exchange, 404, s"unknown function id: $other")
        }
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

  private def runScript(name: String): Future[org.scalatest.Assertion] =
    for {
      clients <- scriptClients()
      _ <- run(clients, Ref.QualifiedName.assertFromString(s"ExternalCallTests:$name"), dar = dar)
    } yield succeed

  "external calls against a test extension service" should {
    "succeed end to end" in runScript("externalCallSuccess")
    "raise PreparationFailed on malformed input" in runScript("externalCallPreparationFailed")
    "raise ExecutionFailed when the service call fails" in runScript("externalCallExecutionFailed")
    "raise InvalidOutput on malformed service output" in runScript("externalCallInvalidOutput")
  }
}
