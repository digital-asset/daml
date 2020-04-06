// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.config.SnapshotEndSetting
import com.daml.extractor.services.ExtractorFixture
import com.daml.extractor.targets.TextPrintTarget
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.platform.sandbox.config.SandboxConfig
import io.netty.handler.ssl.ClientAuth
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TlsNoClientAuthSpec
    extends FlatSpec
    with Suite
    with SuiteResourceManagementAroundAll
    with ExtractorFixture
    with Matchers {

  override protected def darFile = new File(rlocation("extractor/VeryLargeArchive.dar"))

  val List(serverCrt, serverPem, caCrt) = {
    List("server.crt", "server.pem", "ca.crt").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }

  override protected def config: SandboxConfig =
    super.config
      .copy(
        tlsConfig = Some(
          TlsConfiguration(
            enabled = true,
            serverCrt,
            serverPem,
            caCrt,
            clientAuth = ClientAuth.NONE)))

  "Extractor" should "be able to connect with TLS enabled but no client cert" in {
    val config = baseConfig.copy(
      ledgerPort = serverPort,
      tlsConfig = TlsConfiguration(enabled = true, None, None, caCrt),
      to = SnapshotEndSetting.Head,
    )
    val extractor =
      new Extractor(config, TextPrintTarget)()

    Await.result(extractor.run(), Duration.Inf) // as with ExtractorFixture#run
    Await.result(extractor.shutdown(), Duration.Inf) // as with ExtractorFixture#kill
    succeed
  }
}
