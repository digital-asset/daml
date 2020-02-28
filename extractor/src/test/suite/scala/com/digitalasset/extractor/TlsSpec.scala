// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import java.io.File
import java.nio.file.Files

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.extractor.config.SnapshotEndSetting
import com.digitalasset.extractor.services.ExtractorFixture
import com.digitalasset.extractor.targets.TextPrintTarget
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.sandbox.config.SandboxConfig
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TlsSpec
    extends FlatSpec
    with Suite
    with SuiteResourceManagementAroundAll
    with ExtractorFixture
    with Matchers {

  override protected def darFile = new File(rlocation("extractor/VeryLargeArchive.dar"))

  val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) = {
    val dir = Files.createTempDirectory("TlsIT")
    dir.toFile.deleteOnExit()
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      val target = dir.resolve(src)
      target.toFile.deleteOnExit()
      val stream = getClass.getClassLoader.getResourceAsStream("certificates/" + src)
      Files.copy(stream, target)
      Some(target.toFile)
    }
  }

  override protected def config: SandboxConfig =
    super.config
      .copy(tlsConfig = Some(TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt)))

  "Extractor" should "be able to connect with TLS parameters" in {
    val config = baseConfig.copy(
      ledgerPort = serverPort.value,
      tlsConfig = TlsConfiguration(enabled = true, clientCrt, clientPem, caCrt),
      to = SnapshotEndSetting.Head,
    )
    val extractor =
      new Extractor(config, TextPrintTarget)()

    Await.result(extractor.run(), Duration.Inf) // as with ExtractorFixture#run
    Await.result(extractor.shutdown(), Duration.Inf) // as with ExtractorFixture#kill
    succeed
  }
}
