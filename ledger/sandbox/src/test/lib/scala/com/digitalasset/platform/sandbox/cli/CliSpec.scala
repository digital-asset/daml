package com.digitalasset.platform.sandbox.cli

import java.io.File

import com.digitalasset.ledger.client.configuration.TlsConfiguration
import com.digitalasset.platform.sandbox.config.LedgerIdMode.PreDefined
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import org.scalatest.{Matchers, WordSpec}

class CliSpec extends WordSpec with Matchers {

  private val archiveName = "whatever.dar"
  private val defaultConfig = SandboxConfig.default

  private def checkOption(
      options: Array[String],
      expectedChange: SandboxConfig => SandboxConfig) = {
    val expectedConfig = expectedChange(
      defaultConfig.copy(
        damlPackageContainer = defaultConfig.damlPackageContainer.withFile(new File(archiveName))))

    val config =
      Cli.parse(options ++: Array(archiveName))

    config shouldEqual Some(expectedConfig)

  }

  "Cli" should {

    "return None when required arguments are missing" in {
      val config = Cli.parse(Array.empty)
      config shouldEqual None
    }

    "return a Config with sensible defaults when mandatory arguments are given" in {
      val expectedConfig = defaultConfig.copy(
        damlPackageContainer = defaultConfig.damlPackageContainer.withFile(new File(archiveName)))

      val config = Cli.parse(Array(archiveName))

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

    "enable allow-dev when given" in {
      checkOption(
        Array(s"--allow-dev"),
        c => c.copy(damlPackageContainer = c.damlPackageContainer.allowDev))
    }

    "parse the ledger id when given" in {
      val ledgerId = "myledger"
      checkOption(Array(s"--ledgerid", ledgerId), _.copy(ledgerIdMode = PreDefined(ledgerId)))
    }

    "parse the jdbc url when given" in {
      val jdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"
      checkOption(Array(s"--jdbcurl", jdbcUrl), _.copy(jdbcUrl = Some(jdbcUrl)))
    }

  }

}
