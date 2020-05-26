// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.cli

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.lf.data.Ref
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import org.apache.commons.io.FileUtils
import org.scalatest.{Assertion, Matchers, WordSpec}

class CliSpec extends WordSpec with Matchers {

  private val archive = rlocation("ledger/test-common/Test-stable.dar")
  private val nonExistingArchive = "whatever.dar"
  private val invalidArchive = createTempFile().getAbsolutePath
  private val defaultConfig = SandboxConfig.default

  "Cli" should {

    "return the default Config when no arguments are specified" in {
      val config = Cli.parse(Array.empty)
      config shouldEqual Some(defaultConfig)
    }

    "return None when an archive file does not exist" in {
      val config = Cli.parse(Array(nonExistingArchive))
      config shouldEqual None
    }

    "return None when an archive file is not a ZIP" in {
      val config = Cli.parse(Array(invalidArchive))
      config shouldEqual None
    }

    "return a Config with sensible defaults when mandatory arguments are given" in {
      val expectedConfig = defaultConfig.copy(damlPackages = List(new File(archive)))
      val config = Cli.parse(Array(archive))
      config shouldEqual Some(expectedConfig)
    }

    "parse the port when given" in {
      val port = "1234"
      checkOption(Array("-p", port), _.copy(port = Port(port.toInt)))
      checkOption(Array("--port", port), _.copy(port = Port(port.toInt)))
    }

    "parse the address when given" in {
      val address = "myhost"
      checkOption(Array("-a", address), _.copy(address = Some(address)))
      checkOption(Array("--address", address), _.copy(address = Some(address)))
    }

    "apply static time when given" in {
      checkOption(Array("-s"), _.copy(timeProviderType = Some(TimeProviderType.Static)))
      checkOption(Array("--static-time"), _.copy(timeProviderType = Some(TimeProviderType.Static)))
    }

    "apply wall-clock time when given" in {
      checkOption(
        Array("--wall-clock-time"),
        _.copy(timeProviderType = Some(TimeProviderType.WallClock)))
      checkOption(Array("-w"), _.copy(timeProviderType = Some(TimeProviderType.WallClock)))
    }

    "return None when both static and wall-clock time are given" in {
      val config = Cli.parse(Array("--static-time", "--wall-clock-time"))
      config shouldEqual None
    }

    "parse the scenario when given" in {
      val scenario = "myscenario"
      checkOption(Array("--scenario", scenario), _.copy(scenario = Some(scenario)))
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

    "parse the ledger id when given" in {
      val ledgerId = "myledger"
      checkOption(
        Array("--ledgerid", ledgerId),
        _.copy(ledgerIdMode =
          LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString(ledgerId)))))
    }

    "parse the jdbcurl (deprecated) when given" in {
      val jdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"
      checkOption(Array("--jdbcurl", jdbcUrl), _.copy(jdbcUrl = Some(jdbcUrl)))
    }

    "parse the sql backend flag when given" in {
      val jdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"
      checkOption(Array("--sql-backend-jdbcurl", jdbcUrl), _.copy(jdbcUrl = Some(jdbcUrl)))
    }

    "parse the eager package loading flag when given" in {
      checkOption(Array("--eager-package-loading"), _.copy(eagerPackageLoading = true))
    }
  }

  private def checkOption(
      options: Array[String],
      expectedChange: SandboxConfig => SandboxConfig
  ): Assertion = {
    val expectedConfig = expectedChange(defaultConfig.copy(damlPackages = List(new File(archive))))

    val config =
      Cli.parse(options ++ Array(archive))

    config shouldEqual Some(expectedConfig)
  }

  private def createTempFile(): File = {
    val tempFile = File.createTempFile("invalid-archive", ".dar.tmp")
    FileUtils.writeByteArrayToFile(tempFile, "NOT A ZIP".getBytes)
    tempFile.deleteOnExit()
    tempFile
  }
}
