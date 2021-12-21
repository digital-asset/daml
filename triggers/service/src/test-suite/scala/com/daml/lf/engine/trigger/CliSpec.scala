// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model.Uri
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.lf.speedy.Compiler
import com.daml.platform.services.time.TimeProviderType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import pureconfig.error.{CannotReadFile, ConfigReaderFailures}

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class CliSpec extends AsyncWordSpec with Matchers {
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
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
    cli.loadFromConfigFile match {
      case Left(ex) => fail(s"Failed to load config from config file: ${ex.head.description}")
      case Right(c) =>
        c.address shouldBe "127.0.0.1"
        c.port shouldBe Cli.DefaultHttpPort
        c.portFile shouldBe Some(Paths.get("port-file"))

        //ledger-api
        c.ledgerApi.address shouldBe "127.0.0.1"
        c.ledgerApi.port shouldBe 5041

        //authorization config
        c.authorization.authCallbackTimeout shouldBe Cli.DefaultAuthCallbackTimeout
        c.authorization.authCommonUri shouldBe None
        c.authorization.authCallbackUri shouldBe Some(Uri("https://oauth2/callback-uri"))
        c.authorization.authInternalUri shouldBe Some(Uri("https://oauth2/internal-uri"))
        c.authorization.authExternalUri shouldBe Some(Uri("https://oauth2/external-uri"))
        c.authorization.authRedirect shouldBe AuthClient.RedirectToLogin.Yes
        c.authorization.maxPendingAuthorizations shouldBe Cli.DefaultMaxAuthCallbacks

        //jdbc config
        c.triggerStore should not be empty
        val jdbcConfig = c.triggerStore.get
        jdbcConfig.url shouldBe "jdbc:postgresql://localhost:5432/test?&ssl=true"
        jdbcConfig.driver shouldBe "org.postgresql.Driver"
        jdbcConfig.user shouldBe "postgres"
        jdbcConfig.password shouldBe "password"
        jdbcConfig.poolSize shouldBe 12
        jdbcConfig.idleTimeout shouldBe FiniteDuration(12, TimeUnit.SECONDS)
        jdbcConfig.connectionTimeout shouldBe FiniteDuration(90, TimeUnit.SECONDS)
        jdbcConfig.tablePrefix shouldBe "foo"
        jdbcConfig.minIdle shouldBe 4

        //remaining
        c.maxInboundMessageSize shouldBe Cli.DefaultMaxInboundMessageSize
        c.maxHttpEntityUploadSize shouldBe Cli.DefaultMaxHttpEntityUploadSize
        c.maxRestartInterval shouldBe Cli.DefaultMaxRestartInterval
        c.minRestartInterval shouldBe Cli.DefaultMinRestartInterval
        c.httpEntityUploadTimeout shouldBe Cli.DefaultHttpEntityUploadTimeout
        c.timeProviderType shouldBe TimeProviderType.Static
        c.compilerConfig shouldBe Compiler.Config.Dev
        c.initDb shouldBe true
        c.ttl shouldBe FiniteDuration(60, TimeUnit.SECONDS)
        c.allowExistingSchema shouldBe true
    }
  }

  "should take default values on loading minimal config" in {
    val file =
      requiredResource("triggers/service/src/test-suite/resources/trigger-service-minimal.conf")
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
    cli.loadFromConfigFile match {
      case Left(ex) => fail(s"Failed to load config from config file: ${ex.head.description}")
      case Right(c) =>
        c.address shouldBe "127.0.0.1"
        c.port shouldBe Cli.DefaultHttpPort
        c.portFile shouldBe None

        //ledger-api
        c.ledgerApi.address shouldBe "127.0.0.1"
        c.ledgerApi.port shouldBe 5041

        //authorization config
        c.authorization.authCallbackTimeout shouldBe Cli.DefaultAuthCallbackTimeout
        c.authorization.authCommonUri shouldBe None
        c.authorization.authCallbackUri shouldBe None
        c.authorization.authInternalUri shouldBe None
        c.authorization.authExternalUri shouldBe None
        c.authorization.authRedirect shouldBe AuthClient.RedirectToLogin.No
        c.authorization.maxPendingAuthorizations shouldBe Cli.DefaultMaxAuthCallbacks

        //remaining
        c.triggerStore shouldBe None
        c.maxInboundMessageSize shouldBe Cli.DefaultMaxInboundMessageSize
        c.maxHttpEntityUploadSize shouldBe Cli.DefaultMaxHttpEntityUploadSize
        c.maxRestartInterval shouldBe Cli.DefaultMaxRestartInterval
        c.minRestartInterval shouldBe Cli.DefaultMinRestartInterval
        c.httpEntityUploadTimeout shouldBe Cli.DefaultHttpEntityUploadTimeout
        c.timeProviderType shouldBe TimeProviderType.WallClock
        c.compilerConfig shouldBe Compiler.Config.Default
        c.initDb shouldBe false
        c.ttl shouldBe FiniteDuration(30, TimeUnit.SECONDS)
        c.allowExistingSchema shouldBe false
    }
  }

  "parse should raise error on non-existent config file" in {
    val cli = loadCli("missingFile.conf")
    cli.configFile should not be empty
    val cfg = cli.loadFromConfigFile
    cfg match {
      case Right(_) => fail("Unexpected success trying to load missing config file")
      case Left(ex) =>
        ex shouldBe a[ConfigReaderFailures]
        ex.head shouldBe a[CannotReadFile]
    }

    //parseConfig for non-existent file should return a None
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
        Array("--ledger-host", "localhost", "--ledger-port", "9999"),
        Set(),
      ) shouldBe Some(Cli.Empty.copy(ledgerHost = "localhost", ledgerPort = 9999))
      .map(_.loadFromCliArgs)
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
