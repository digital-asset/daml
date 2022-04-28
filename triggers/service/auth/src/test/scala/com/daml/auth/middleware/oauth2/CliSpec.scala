// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import akka.http.scaladsl.model.Uri
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.jwt.JwksVerifier
import org.scalatest.Inside.inside
import pureconfig.error.{CannotReadFile, ConfigReaderFailures}

import java.nio.file.Paths
import scala.concurrent.duration._

class CliSpec extends AsyncWordSpec with Matchers {
  val minimalCfg = Config(
    oauthAuth = Uri("https://oauth2/uri"),
    oauthToken = Uri("https://oauth2/token"),
    callbackUri = Some(Uri("https://example.com/auth/cb")),
    clientId = sys.env.getOrElse("DAML_CLIENT_ID", "foo"),
    clientSecret = SecretString(sys.env.getOrElse("DAML_CLIENT_SECRET", "bar")),
    tokenVerifier = null,
  )
  val confFile = "triggers/service/auth/src/test/resources/oauth2-middleware.conf"
  def loadCli(file: String): Cli = {
    Cli.parse(Array("--config", file)).getOrElse(fail("Could not load Cli on parse"))
  }

  "should pickup the config file provided" in {
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
  }

  "should take default values on loading minimal config" in {
    val file =
      requiredResource("triggers/service/auth/src/test/resources/oauth2-middleware-minimal.conf")
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
    val cfg = cli.loadFromConfigFile
    inside(cfg) { case Some(Right(c)) =>
      c.copy(tokenVerifier = null) shouldBe minimalCfg
      // token verifier needs to be set.
      c.tokenVerifier shouldBe a[JwksVerifier]
    }
  }

  "should be able to successfully load the config based on the file provided" in {
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
    val cfg = cli.loadFromConfigFile
    inside(cfg) { case Some(Right(c)) =>
      c.copy(tokenVerifier = null) shouldBe minimalCfg.copy(
        port = 3000,
        maxLoginRequests = 10,
        loginTimeout = 60.seconds,
        cookieSecure = false,
        oauthAuthTemplate = Some(Paths.get("auth_template")),
        oauthTokenTemplate = Some(Paths.get("token_template")),
        oauthRefreshTemplate = Some(Paths.get("refresh_template")),
      )
      // token verifier needs to be set.
      c.tokenVerifier shouldBe a[JwksVerifier]
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
      )
    ) shouldBe None
  }

  "should load config from cli args on missing conf file " in {
    Cli
      .parseConfig(
        Array(
          "--oauth-auth",
          "file://foo",
          "--oauth-token",
          "file://bar",
          "--id",
          "foo",
          "--secret",
          "bar",
          "--auth-jwt-hs256-unsafe",
          "unsafe",
        )
      ) should not be empty
  }

  "should fail to load config from cli args on incomplete cli args" in {
    Cli
      .parseConfig(
        Array(
          "--oauth-auth",
          "file://foo",
          "--id",
          "foo",
          "--secret",
          "bar",
          "--auth-jwt-hs256-unsafe",
          "unsafe",
        )
      ) shouldBe None
  }
}
