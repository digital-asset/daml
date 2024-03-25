// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import org.apache.pekko.http.scaladsl.model.Uri
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.dbutils.JdbcConfig
import com.daml.lf.engine.trigger.TriggerServiceAppConf.CompilerConfigBuilder
import com.daml.lf.language.LanguageMajorVersion
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.error.{CannotReadFile, ConfigReaderFailures}
import com.daml.pureconfigutils.LedgerApiConfig
import com.daml.lf.speedy.Compiler

import java.nio.file.Paths
import scala.concurrent.duration._

class CliSpec extends AsyncWordSpec with Matchers {

  val minimalConf = TriggerServiceAppConf(ledgerApi = LedgerApiConfig("127.0.0.1", 5041))
  val confFile = "triggers/service/src/test-suite/resources/trigger-service.conf"
  def loadCli(file: String): Cli = {
    Cli.parse(Array("--config", file), Set()).getOrElse(fail("Could not load Cli on parse"))
  }

  "should pickup the config file provided" in {
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
  }

  "should be able to successfully load the config based on the file provided" in {
    val expectedAuthCfg = AuthorizationConfig(
      authInternalUri = Some(Uri("https://oauth2/internal-uri")),
      authExternalUri = Some(Uri("https://oauth2/external-uri")),
      authCallbackUri = Some(Uri("https://oauth2/callback-uri")),
      authRedirect = AuthClient.RedirectToLogin.Yes,
    )
    val expectedJdbcConfig = JdbcConfig(
      url = "jdbc:postgresql://localhost:5432/test?&ssl=true",
      driver = "org.postgresql.Driver",
      user = "postgres",
      password = "password",
      poolSize = 12,
      idleTimeout = 12.seconds,
      connectionTimeout = 90.seconds,
      tablePrefix = "foo",
      minIdle = 4,
    )
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
    val cfg = cli.loadFromConfigFile
    inside(cfg) { case Some(Right(c)) =>
      c shouldBe minimalConf.copy(
        darPaths = List(Paths.get("./my-app.dar")),
        portFile = Some(Paths.get("port-file")),
        authorization = expectedAuthCfg,
        triggerStore = Some(expectedJdbcConfig),
        timeProviderType = TimeProviderType.Static,
        compilerConfig = CompilerConfigBuilder.Dev,
        lfMajorVersion = LanguageMajorVersion.V1,
        initDb = true,
        ttl = 60.seconds,
        allowExistingSchema = true,
      )
    }
  }

  "should take default values on loading minimal config" in {
    val file =
      requiredResource("triggers/service/src/test-suite/resources/trigger-service-minimal.conf")
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
    val cfg = cli.loadFromConfigFile
    inside(cfg) { case Some(Right(c)) =>
      c shouldBe minimalConf
    }
  }

  "parse should raise error on non-existent config file" in {
    val cli = loadCli("missingFile.conf")
    cli.configFile should not be empty
    val cfg = cli.loadFromConfigFile
    inside(cfg) { case Some(Left(ConfigReaderFailures(head))) =>
      head shouldBe a[CannotReadFile]
    }

    // parseConfig for non-existent file should return a None
    Cli.parseConfig(
      Array(
        "--config",
        "missingFile.conf",
      ),
      Set(),
    ) shouldBe None
  }

  "should load config from cli args when no conf file is specified" in {
    Cli
      .parseConfig(
        Array(
          "--ledger-host",
          "localhost",
          "--ledger-port",
          "9999",
          "--dev-mode-unsafe",
        ),
        Set(),
      ) shouldBe Some(Cli.Default.copy(ledgerHost = "localhost", ledgerPort = 9999))
      .map(_.loadFromCliArgs)
      .map(_.copy(compilerConfig = Compiler.Config.Dev(LanguageMajorVersion.V1)))
  }

  "should fail to load config from cli args on missing required params" in {
    Cli
      .parseConfig(
        Array("--ledger-host", "localhost"),
        Set(),
      ) shouldBe None
  }

  "should fail to load config on supplying both cli args and config file" in {
    Cli
      .parseConfig(
        Array("--config", confFile, "--ledger-host", "localhost", "--ledger-port", "9999"),
        Set(),
      ) shouldBe None
  }
}
