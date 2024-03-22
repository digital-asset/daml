// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.pureconfigutils

import com.daml.jwt.JwtVerifierBase
import com.daml.metrics.MetricsReporter
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.error.{ConfigReaderFailures, ConvertFailure}
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}

import java.nio.file.Paths
import scala.concurrent.duration._

class SharedConfigReadersTest extends AsyncWordSpec with Matchers {
  import SharedConfigReaders._

  case class SampleServiceConfig(
      server: HttpServerConfig,
      ledgerApi: LedgerApiConfig,
      metrics: MetricsConfig,
  )
  implicit val serviceConfigReader: ConfigReader[SampleServiceConfig] =
    deriveReader[SampleServiceConfig]

  case class DummyConfig(tokenVerifier: JwtVerifierBase)
  implicit val dummyCfgReader: ConfigReader[DummyConfig] = deriveReader[DummyConfig]

  "should be able to parse a sample config with shared config objects" in {
    val conf = """
      |{
      |  server {
      |    address = "127.0.0.1"
      |    port = 8890
      |    port-file = "port-file"
      |  }
      |  ledger-api {
      |    address = "127.0.0.1"
      |    port = 8098
      |  }
      |  metrics {
      |    reporter = "console"
      |    reporting-interval = 10s
      |  }
      |}
      |""".stripMargin

    val expectedConf = SampleServiceConfig(
      HttpServerConfig("127.0.0.1", 8890, Some(Paths.get("port-file"))),
      LedgerApiConfig("127.0.0.1", 8098),
      MetricsConfig(MetricsReporter.Console, 10.seconds),
    )

    ConfigSource.string(conf).load[SampleServiceConfig] shouldBe Right(expectedConf)
  }

  "should fail on loading unknown tokenVerifiers" in {
    val conf = """
                 |{
                 |  token-verifier {
                 |    type = "foo"
                 |    uri = "bar"
                 |  }
                 |}
                 |""".stripMargin

    val cfg = ConfigSource.string(conf).load[DummyConfig]
    inside(cfg) { case Left(ConfigReaderFailures(ex)) =>
      ex shouldBe a[ConvertFailure]
    }
  }

}
