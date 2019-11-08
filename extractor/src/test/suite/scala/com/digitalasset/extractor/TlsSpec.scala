// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import java.io.File
import java.nio.file.{Files, Path}

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.extractor.config.SnapshotEndSetting
import com.digitalasset.extractor.services.ExtractorFixture
import com.digitalasset.extractor.targets.TextPrintTarget
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.sandbox.config.SandboxConfig
import org.apache.commons.io.FileUtils
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

  private def extractCerts: Path = {
    val dir = Files.createTempDirectory("TlsIT").toFile
    dir.deleteOnExit()
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").foreach { src =>
      val target = new File(dir, src)
      target.deleteOnExit()
      val stream = getClass.getClassLoader.getResourceAsStream("certificates/" + src)
      FileUtils.copyInputStreamToFile(stream, target)
    }
    dir.toPath
  }

  private lazy val certificatesPath = extractCerts
  private lazy val certificatesDirPrefix: String = certificatesPath.toString + File.separator

  private lazy val serverCertChainFilePath = Some(new File(certificatesDirPrefix, "server.crt"))
  private lazy val serverPrivateKeyFilePath = Some(new File(certificatesDirPrefix, "server.pem"))
  private lazy val trustCertCollectionFilePath = Some(new File(certificatesDirPrefix, "ca.crt"))
  private lazy val clientCertChainFilePath = Some(new File(certificatesDirPrefix, "client.crt"))
  private lazy val clientPrivateKeyFilePath = Some(new File(certificatesDirPrefix, "client.pem"))

  override protected def config: SandboxConfig =
    super.config.copy(
      tlsConfig = Some(
        TlsConfiguration(
          true,
          clientCertChainFilePath,
          clientPrivateKeyFilePath,
          trustCertCollectionFilePath)))

  "Extractor" should "be able to connect with TLS parameters" in {
    val config = baseConfig.copy(
      ledgerPort = getSandboxPort,
      tlsConfig = TlsConfiguration(
        true,
        serverCertChainFilePath,
        serverPrivateKeyFilePath,
        trustCertCollectionFilePath),
      to = SnapshotEndSetting.Head
    )
    val extractor =
      new Extractor(config, TextPrintTarget)()

    Await.result(extractor.run(), Duration.Inf) // as with ExtractorFixture#run
    Await.result(extractor.shutdown(), Duration.Inf) // as with ExtractorFixture#kill
    succeed
  }
}
