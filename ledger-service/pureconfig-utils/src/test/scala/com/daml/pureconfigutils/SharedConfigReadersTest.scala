// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.pureconfigutils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.generic.semiauto.deriveReader
import pureconfig.{ConfigReader, ConfigSource}

class SharedConfigReadersTest extends AsyncWordSpec with Matchers {
  import SharedConfigReaders._

  case class SampleServiceConfig(
      server: HttpServerConfig,
      ledgerApi: LedgerApiConfig,
      metrics: MetricsConfig,
  )
  implicit val serviceConfigReader: ConfigReader[SampleServiceConfig] =
    deriveReader[SampleServiceConfig]

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

    ConfigSource.string(conf).load[SampleServiceConfig] match {
      case Right(_) => succeed
      case Left(ex) => fail(s"Failed to successfully parse service conf: ${ex.head.description}")
    }
  }

}
