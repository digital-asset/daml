// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.cli

import java.io.{File}

import com.digitalasset.daml.bazeltools.BazelRunfiles.rlocation
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import org.apache.commons.io.FileUtils
import org.scalatest.{Matchers, WordSpec}

class CliSpec extends WordSpec with Matchers {

  private val archive = rlocation("ledger/test-common/Test-stable.dar")
  private val nonExistingArchive = "whatever.dar"
  private val invalidArchive = createTempFile.getAbsolutePath
  private val defaultConfig = SandboxConfig.default

  private def checkOption(
      options: Array[String],
      expectedChange: SandboxConfig => SandboxConfig) = {
    val expectedConfig = expectedChange(defaultConfig.copy(damlPackages = List(new File(archive))))

    val config =
      Cli.parse(options ++: Array(archive))

    config shouldEqual Some(expectedConfig)
  }

  private def createTempFile(): File = {
    val tempFile = File.createTempFile("invalid-archive", ".dar.tmp")
    FileUtils.writeByteArrayToFile(tempFile, "NOT A ZIP".getBytes)
    tempFile.deleteOnExit
    tempFile
  }

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
      checkOption(Array(s"-p", port), _.copy(port = port.toInt))
      checkOption(Array(s"--port", port), _.copy(port = port.toInt))
    }

    "parse the address when given" in {
      val address = "myhost"
      checkOption(Array(s"-a", address), _.copy(address = Some(address)))
      checkOption(Array(s"--address", address), _.copy(address = Some(address)))
    }

    "apply static time when given" in {
      checkOption(Array(s"-s"), _.copy(timeProviderType = TimeProviderType.Static))
      checkOption(Array(s"--static-time"), _.copy(timeProviderType = TimeProviderType.Static))
    }

    "apply wall-clock time when given" in {
      checkOption(
        Array(s"--wall-clock-time"),
        _.copy(timeProviderType = TimeProviderType.WallClock))
      checkOption(Array(s"-w"), _.copy(timeProviderType = TimeProviderType.WallClock))
    }

    "parse the scenario when given" in {
      val scenario = "myscenario"
      checkOption(Array(s"--scenario", scenario), _.copy(scenario = Some(scenario)))
    }

    "parse the crt file when given" in {
      val crt = "mycrt"
      checkOption(
        Array(s"--crt", crt),
        _.copy(tlsConfig = Some(TlsConfiguration(true, Some(new File(crt)), None, None))))
    }

    "parse the cacrt file when given" in {
      val cacrt = "mycacrt"
      checkOption(
        Array(s"--cacrt", cacrt),
        _.copy(tlsConfig = Some(TlsConfiguration(true, None, None, Some(new File(cacrt))))))
    }

    "parse the pem file when given" in {
      val pem = "mypem"
      checkOption(
        Array(s"--pem", pem),
        _.copy(tlsConfig = Some(TlsConfiguration(true, None, Some(new File(pem)), None))))
    }

    "parse the ledger id when given" in {
      val ledgerId = "myledger"
      checkOption(
        Array(s"--ledgerid", ledgerId),
        _.copy(ledgerIdMode =
          LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString(ledgerId)))))
    }

    "parse the jdbcurl (deprecated) when given" in {
      val jdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"
      checkOption(Array(s"--jdbcurl", jdbcUrl), _.copy(jdbcUrl = Some(jdbcUrl)))
    }

    "parse the sql backend flag when given" in {
      val jdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"
      checkOption(Array(s"--sql-backend-jdbcurl", jdbcUrl), _.copy(jdbcUrl = Some(jdbcUrl)))
    }

    "parse the eager package loading flag when given" in {
      checkOption(Array("--eager-package-loading"), _.copy(eagerPackageLoading = true))
    }
  }

}
