// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.{Files, Paths}
import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.tls.{SecretsUrl, TlsConfiguration, TlsVersion}
import com.daml.ledger.test.ModelTestDar
import com.daml.lf.data.Ref
import com.daml.metrics.MetricsReporter
import com.daml.metrics.MetricsReporter.{Graphite, Prometheus}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.CliSpec._
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import com.daml.sandbox.Cli
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class CliSpec extends AnyWordSpec with Matchers {

  val requiredArgs: Array[String] = Array.empty
  private val cli = Cli
  private val defaultConfig = cli.defaultConfig

  "Cli" should {
    "parse the ledgerid when given" in {
      val ledgerId = "myledger"
      checkOption(
        Array("--ledgerid", ledgerId),
        _.copy(ledgerIdMode =
          LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString(ledgerId)))
        ),
      )
    }

    "parse the sql-backend-jdbcurl flag when given" in {
      val jdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"
      checkOption(Array("--sql-backend-jdbcurl", jdbcUrl), _.copy(jdbcUrl = Some(jdbcUrl)))
    }

    "parse the jdbcurl flag (deprecated) when given" in {
      checkOption(Array("--jdbcurl", exampleJdbcUrl), _.copy(jdbcUrl = Some(exampleJdbcUrl)))
    }

    "parse the contract-id-seeding mode when given" in {
      checkOption(Array("--contract-id-seeding", "strong"), _.copy(seeding = Seeding.Strong))
    }

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
        _.copy(participantId = Ref.ParticipantId.assertFromString("myParticipant")),
      )
    }

    "apply static time when given" in {
      checkOption(Array("-s"), _.copy(timeProviderType = Some(TimeProviderType.Static)))
      checkOption(Array("--static-time"), _.copy(timeProviderType = Some(TimeProviderType.Static)))
    }

    "apply wall-clock time when given" in {
      checkOption(
        Array("--wall-clock-time"),
        _.copy(timeProviderType = Some(TimeProviderType.WallClock)),
      )
      checkOption(Array("-w"), _.copy(timeProviderType = Some(TimeProviderType.WallClock)))
    }

    "return None when both static and wall-clock time are given" in {
      val config = cli.parse(requiredArgs ++ Array("--static-time", "--wall-clock-time"))
      config shouldEqual None
    }

    "parse the crt file when given" in {
      val crt = "mycrt"
      checkOption(
        Array("--crt", crt),
        _.copy(tlsConfig = Some(TlsConfiguration(enabled = true, Some(new File(crt)), None, None))),
      )
    }

    "parse the cacrt file when given" in {
      val cacrt = "mycacrt"
      checkOption(
        Array("--cacrt", cacrt),
        _.copy(
          tlsConfig = Some(TlsConfiguration(enabled = true, None, None, Some(new File(cacrt))))
        ),
      )
    }

    "parse the pem file when given" in {
      val pem = "mypem"
      checkOption(
        Array("--pem", pem),
        _.copy(tlsConfig = Some(TlsConfiguration(enabled = true, None, Some(new File(pem)), None))),
      )
    }

    "succeed when server's private key is encrypted and secret-url is provided" in {
      checkOption(
        Array(
          "--pem",
          "key.enc",
          "--tls-secrets-url",
          "http://aaa",
        ),
        _.copy(tlsConfig =
          Some(
            TlsConfiguration(
              enabled = true,
              secretsUrl = Some(SecretsUrl.fromString("http://aaa")),
              keyFile = Some(new File("key.enc")),
              keyCertChainFile = None,
              trustCertCollectionFile = None,
            )
          )
        ),
      )
    }

    "fail parsing a bogus TLS version" in {
      checkOptionFail(
        Array(
          "--min-tls-version",
          "111",
        )
      )
    }

    "succeed parsing a supported TLS version" in {
      checkOption(
        Array(
          "--min-tls-version",
          "1.3",
        ),
        _.copy(tlsConfig =
          Some(
            TlsConfiguration(
              enabled = true,
              minimumServerProtocolVersion = Some(TlsVersion.V1_3),
            )
          )
        ),
      )

    }

    "fail when server's private key is encrypted but secret-url is not provided" in {
      checkOptionFail(
        Array(
          "--pem",
          "key.enc",
        )
      )
    }

    "succeed when server's private key is in plaintext and secret-url is not provided" in {
      checkOption(
        Array(
          "--pem",
          "key.txt",
        ),
        _.copy(tlsConfig =
          Some(
            TlsConfiguration(
              enabled = true,
              secretsUrl = None,
              keyFile = Some(new File("key.txt")),
              keyCertChainFile = None,
              trustCertCollectionFile = None,
            )
          )
        ),
      )
    }

    "set certificate revocation checks property" in {
      checkOption(
        Array("--cert-revocation-checking", "true"),
        _.copy(tlsConfig =
          Some(
            TlsConfiguration(enabled = true, None, None, None, enableCertRevocationChecking = true)
          )
        ),
      )
    }

    "parse a console metrics reporter when given" in {
      checkOption(
        Array("--metrics-reporter", "console"),
        _.copy(metricsReporter = Some(MetricsReporter.Console)),
      )
    }

    "reject a console metrics reporter when it has extra information" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "console://foo"))
      config shouldEqual None
    }

    "reject a console metrics reporter when it's got trailing information without '//'" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "console:foo"))
      config shouldEqual None
    }

    "parse a CSV metrics reporter when given" in {
      checkOption(
        Array("--metrics-reporter", "csv:///path/to/file.csv"),
        _.copy(metricsReporter = Some(MetricsReporter.Csv(Paths.get("/path/to/file.csv")))),
      )
    }

    "reject a CSV metrics reporter when it has no information" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "csv"))
      config shouldEqual None
    }

    "reject a CSV metrics reporter when it has no path" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "csv://"))
      config shouldEqual None
    }

    "reject a CSV metrics reporter when it has a host" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "csv://hostname/path"))
      config shouldEqual None
    }

    "reject a CSV metrics reporter when it's missing '//'" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "csv:/path"))
      config shouldEqual None
    }

    "parse a Graphite metrics reporter when given" in {
      val expectedAddress = new InetSocketAddress("server", Graphite.defaultPort)
      checkOption(
        Array("--metrics-reporter", "graphite://server"),
        _.copy(metricsReporter = Some(MetricsReporter.Graphite(expectedAddress, None))),
      )
    }

    "parse a Graphite metrics reporter with a port when given" in {
      val expectedAddress = new InetSocketAddress("server", 9876)
      checkOption(
        Array("--metrics-reporter", "graphite://server:9876"),
        _.copy(metricsReporter = Some(MetricsReporter.Graphite(expectedAddress, None))),
      )
    }

    "parse a Graphite metrics reporter with a prefix when given" in {
      val expectedAddress = new InetSocketAddress("server", Graphite.defaultPort)
      checkOption(
        Array("--metrics-reporter", "graphite://server/prefix"),
        _.copy(metricsReporter = Some(MetricsReporter.Graphite(expectedAddress, Some("prefix")))),
      )
    }

    "parse a Graphite metrics reporter with a port and prefix when given" in {
      val expectedAddress = new InetSocketAddress("server", 9876)
      checkOption(
        Array("--metrics-reporter", "graphite://server:9876/prefix"),
        _.copy(metricsReporter = Some(MetricsReporter.Graphite(expectedAddress, Some("prefix")))),
      )
    }

    "reject a Graphite metrics reporter when it has no information" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "graphite"))
      config shouldEqual None
    }

    "reject a Graphite metrics reporter without a host" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "graphite://"))
      config shouldEqual None
    }

    "reject a Graphite metrics reporter without a host but with a prefix" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "graphite:///prefix"))
      config shouldEqual None
    }

    "reject a Graphite metrics reporter when it's missing '//'" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "graphite:server:1234"))
      config shouldEqual None
    }

    "parse a Prometheus metrics reporter when given" in {
      val expectedAddress = new InetSocketAddress("server", Prometheus.defaultPort)
      checkOption(
        Array("--metrics-reporter", "prometheus://server"),
        _.copy(metricsReporter = Some(MetricsReporter.Prometheus(expectedAddress))),
      )
    }

    "parse a Prometheus metrics reporter with a port when given" in {
      val expectedAddress = new InetSocketAddress("server", 9876)
      checkOption(
        Array("--metrics-reporter", "prometheus://server:9876"),
        _.copy(metricsReporter = Some(MetricsReporter.Prometheus(expectedAddress))),
      )
    }

    "reject a Prometheus metrics reporter when it has no information" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "prometheus"))
      config shouldEqual None
    }

    "reject a Prometheus metrics reporter without a host" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "prometheus://"))
      config shouldEqual None
    }

    "reject a Prometheus metrics reporter without a host but with a port" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "prometheus://:9876"))
      config shouldEqual None
    }

    "reject a Prometheus metrics reporter when it's missing '//'" in {
      val config = cli.parse(requiredArgs ++ Array("--metrics-reporter", "prometheus:server:1234"))
      config shouldEqual None
    }

    "parse the metrics reporting interval (java duration format) when given" in {
      checkOption(
        Array("--metrics-reporting-interval", "PT1M30S"),
        _.copy(metricsReportingInterval = 90.seconds),
      )
    }

    "parse the metrics reporting interval (scala duration format) when given" in {
      checkOption(
        Array("--metrics-reporting-interval", "1.5min"),
        _.copy(metricsReportingInterval = 90.seconds),
      )
    }

    "handle '--enable-user-management' flag correctly" in {
      checkOptionFail(
        Array("--enable-user-management")
      )
      checkOption(
        Array("--enable-user-management", "false"),
        _.withUserManagementConfig(_.copy(enabled = false)),
      )
      checkOption(
        Array("--enable-user-management", "true"),
        _.withUserManagementConfig(_.copy(enabled = true)),
      )
      checkOption(
        Array(),
        _.withUserManagementConfig(_.copy(enabled = true)),
      )
    }

    "handle '--user-management-max-cache-size' flag correctly" in {
      // missing cache size value
      checkOptionFail(
        Array("--user-management-max-cache-size")
      )
      // default
      checkOption(
        Array.empty,
        _.withUserManagementConfig(_.copy(maxCacheSize = 100)),
      )
      // custom value
      checkOption(
        Array(
          "--user-management-max-cache-size",
          "123",
        ),
        _.withUserManagementConfig(_.copy(maxCacheSize = 123)),
      )
    }

    "handle '--user-management-cache-expiry' flag correctly" in {
      // missing cache size value
      checkOptionFail(
        Array("--user-management-cache-expiry")
      )
      // default
      checkOption(
        Array.empty,
        _.withUserManagementConfig(_.copy(cacheExpiryAfterWriteInSeconds = 5)),
      )
      // custom value
      checkOption(
        Array(
          "--user-management-cache-expiry",
          "123",
        ),
        _.withUserManagementConfig(_.copy(cacheExpiryAfterWriteInSeconds = 123)),
      )
    }

    "handle '--user-management-max-users-page-size' flag correctly" in {
      // missing value
      checkOptionFail(
        Array("--user-management-max-users-page-size")
      )
      // default
      checkOption(
        Array.empty,
        _.withUserManagementConfig(_.copy(maxUsersPageSize = 1000)),
      )
      // custom value
      checkOption(
        Array(
          "--user-management-max-users-page-size",
          "123",
        ),
        _.withUserManagementConfig(_.copy(maxUsersPageSize = 123)),
      )
      // values in range [1, 99] are disallowed
      checkOptionFail(
        Array(
          "--user-management-max-users-page-size",
          "1",
        )
      )
      checkOptionFail(
        Array(
          "--user-management-max-users-page-size",
          "99",
        )
      )
      // negative values are disallowed
      checkOptionFail(
        Array(
          "--user-management-max-users-page-size",
          "-1",
        )
      )
    }
  }

  protected def checkOption(
      args: Array[String],
      expectedChange: SandboxConfig => SandboxConfig,
  ): Assertion = {
    val expectedConfig = expectedChange(defaultConfig.copy(damlPackages = List(new File(archive))))
    val config = cli.parse(requiredArgs ++ args ++ Array(archive))
    config shouldEqual Some(expectedConfig)
  }

  protected def checkOptionFail(args: Array[String]): Assertion = {
    val config = cli.parse(requiredArgs ++ args ++ Array(archive))
    config shouldEqual None
  }

}

object CliSpec {

  private val archive = rlocation(ModelTestDar.path)
  private val nonExistentArchive = "whatever.dar"
  private val invalidArchive = {
    val tempFile = Files.createTempFile("invalid-archive", ".dar.tmp")
    Files.write(tempFile, Seq("NOT A ZIP").asJava)
    tempFile.toFile.deleteOnExit()
    tempFile.toAbsolutePath.toString
  }

  val exampleJdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"

}
