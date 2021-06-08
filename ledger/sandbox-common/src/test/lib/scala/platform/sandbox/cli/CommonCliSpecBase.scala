// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.cli

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.participant.state.v1
import com.daml.metrics.MetricsReporter
import com.daml.metrics.MetricsReporter.Graphite
import com.daml.platform.sandbox.cli.CommonCliSpecBase._
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

abstract class CommonCliSpecBase(
    protected val cli: SandboxCli,
    protected val requiredArgs: Array[String] = Array.empty,
    protected val expectedDefaultConfig: Option[SandboxConfig] = None,
) extends AnyWordSpec
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
        _.copy(participantId = v1.ParticipantId.assertFromString("myParticipant")),
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

    "parse the eager package loading flag when given" in {
      checkOption(Array("--eager-package-loading"), _.copy(eagerPackageLoading = true))
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

    "parse the metrics reporting interval when given" in {
      checkOption(
        Array("--metrics-reporting-interval", "1.5m"),
        _.copy(metricsReportingInterval = 90.seconds),
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
}

object CommonCliSpecBase {

  private val archive = rlocation(com.daml.ledger.test.TestDars.fileNames("model"))
  private val nonExistentArchive = "whatever.dar"
  private val invalidArchive = {
    val tempFile = Files.createTempFile("invalid-archive", ".dar.tmp")
    Files.write(tempFile, Seq("NOT A ZIP").asJava)
    tempFile.toFile.deleteOnExit()
    tempFile.toAbsolutePath.toString
  }

  val exampleJdbcUrl = "jdbc:postgresql://localhost:5432/test?user=test"

}
