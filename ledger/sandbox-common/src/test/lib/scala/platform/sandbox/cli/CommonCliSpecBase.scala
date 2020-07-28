// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.cli

import java.io.File
import java.nio.file.Files

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.participant.state.v1
import com.daml.platform.sandbox.cli.CommonCliSpecBase._
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.ports.Port
import org.scalatest.{Assertion, Matchers, WordSpec}

import scala.collection.JavaConverters._

abstract class CommonCliSpecBase(
    protected val cli: SandboxCli,
    protected val requiredArgs: Array[String] = Array.empty,
    protected val expectedDefaultConfig: Option[SandboxConfig] = None,
) extends WordSpec
    with Matchers {

  private val defaultConfig = expectedDefaultConfig.getOrElse(cli.defaultConfig)

  "Cli" should {

    "return the input config when no arguments are specified (except any required arguments)" in {
      val config = cli.parse(requiredArgs)
      config shouldEqual Some(defaultConfig)
    }

    "return None when an archive file does not exist" in {
      val config = cli.parse(requiredArgs ++ Array(nonExistentArchive))
      config shouldEqual None
    }

    "return None when an archive file is not a ZIP" in {
      val config = cli.parse(requiredArgs ++ Array(invalidArchive))
      config shouldEqual None
    }

    "return a Config with sensible defaults when mandatory arguments are given" in {
      val expectedConfig = defaultConfig.copy(damlPackages = List(new File(archive)))
      val config = cli.parse(requiredArgs ++ Array(archive))
      config shouldEqual Some(expectedConfig)
    }

    "parse the address when given" in {
      val address = "myhost"
      checkOption(Array("-a", address), _.copy(address = Some(address)))
      checkOption(Array("--address", address), _.copy(address = Some(address)))
    }

    "parse the port when given" in {
      val port = "1234"
      checkOption(Array("-p", port), _.copy(port = Port(port.toInt)))
      checkOption(Array("--port", port), _.copy(port = Port(port.toInt)))
    }

    "parse the participant ID when given" in {
      val participantId = "myParticipant"
      checkOption(
        Array("--participant-id", participantId),
        _.copy(participantId = v1.ParticipantId.assertFromString("myParticipant")))
    }

    "parse the crt file when given" in {
      val crt = "mycrt"
      checkOption(
        Array("--crt", crt),
        _.copy(tlsConfig = Some(TlsConfiguration(enabled = true, Some(new File(crt)), None, None))))
    }

    "parse the cacrt file when given" in {
      val cacrt = "mycacrt"
      checkOption(
        Array("--cacrt", cacrt),
        _.copy(
          tlsConfig = Some(TlsConfiguration(enabled = true, None, None, Some(new File(cacrt))))))
    }

    "parse the pem file when given" in {
      val pem = "mypem"
      checkOption(
        Array("--pem", pem),
        _.copy(tlsConfig = Some(TlsConfiguration(enabled = true, None, Some(new File(pem)), None))))
    }

    "parse the eager package loading flag when given" in {
      checkOption(Array("--eager-package-loading"), _.copy(eagerPackageLoading = true))
    }
  }

  protected def checkOption(
      args: Array[String],
      expectedChange: SandboxConfig => SandboxConfig
  ): Assertion = {
    val expectedConfig = expectedChange(defaultConfig.copy(damlPackages = List(new File(archive))))
    val config = cli.parse(requiredArgs ++ args ++ Array(archive))
    config shouldEqual Some(expectedConfig)
  }
}

object CommonCliSpecBase {

  private val archive = rlocation("ledger/test-common/model-tests.dar")
  private val nonExistentArchive = "whatever.dar"
  private val invalidArchive = {
    val tempFile = Files.createTempFile("invalid-archive", ".dar.tmp")
    Files.write(tempFile, Seq("NOT A ZIP").asJava)
    tempFile.toFile.deleteOnExit()
    tempFile.toAbsolutePath.toString
  }

  val exampleJdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"

}
