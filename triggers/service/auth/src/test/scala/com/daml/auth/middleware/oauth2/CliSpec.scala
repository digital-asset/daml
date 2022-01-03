// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import akka.http.scaladsl.model.Uri
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.bazeltools.BazelRunfiles.requiredResource
import com.daml.jwt.JwksVerifier

import java.nio.file.Paths
import scala.concurrent.duration._

class CliSpec extends AsyncWordSpec with Matchers {
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
    cli.loadConfigFromFile match {
      case Left(ex) => fail(ex.msg)
      case Right(c) =>
        c.address shouldBe "127.0.0.1"
        c.port shouldBe Config.DefaultHttpPort
        c.callbackUri shouldBe Some(Uri("https://example.com/auth/cb"))
        c.maxLoginRequests shouldBe Config.DefaultMaxLoginRequests
        c.loginTimeout shouldBe Config.DefaultLoginTimeout
        c.cookieSecure shouldBe Config.DefaultCookieSecure
        c.oauthAuth shouldBe Uri("https://oauth2/uri")
        c.oauthToken shouldBe Uri("https://oauth2/token")

        c.oauthAuthTemplate shouldBe None
        c.oauthTokenTemplate shouldBe None
        c.oauthRefreshTemplate shouldBe None

        c.clientId shouldBe sys.env.getOrElse("DAML_CLIENT_ID", "foo")
        c.clientSecret shouldBe SecretString(sys.env.getOrElse("DAML_CLIENT_SECRET", "bar"))

        // token verifier needs to be set.
        c.tokenVerifier match {
          case _: JwksVerifier => succeed
          case _ => fail("expected JwksVerifier based on supplied config")
        }
    }
  }

  "should be able to successfully load the config based on the file provided" in {
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile should not be empty
    cli.loadConfigFromFile match {
      case Left(ex) => fail(ex.msg)
      case Right(c) =>
        c.address shouldBe "127.0.0.1"
        c.port shouldBe 3000
        c.callbackUri shouldBe Some(Uri("https://example.com/auth/cb"))
        c.maxLoginRequests shouldBe 10
        c.loginTimeout shouldBe FiniteDuration(60, SECONDS)
        c.cookieSecure shouldBe false
        c.oauthAuth shouldBe Uri("https://oauth2/uri")
        c.oauthToken shouldBe Uri("https://oauth2/token")

        c.oauthAuthTemplate shouldBe Some(Paths.get("auth_template"))
        c.oauthTokenTemplate shouldBe Some(Paths.get("token_template"))
        c.oauthRefreshTemplate shouldBe Some(Paths.get("refresh_template"))

        c.clientId shouldBe sys.env.getOrElse("DAML_CLIENT_ID", "foo")
        c.clientSecret shouldBe SecretString(sys.env.getOrElse("DAML_CLIENT_SECRET", "bar"))

        c.tokenVerifier match {
          case _: JwksVerifier => succeed
          case _ => fail("expected JwksVerifier based on supplied config")
        }

    }
  }

  "parse should raise error on non-existent config file" in {
    val cli = loadCli("missingFile.conf")
    cli.configFile should not be empty
    val cfg = cli.loadConfigFromFile
    cfg match {
      case Left(err) => err shouldBe a[ConfigParseError]
      case _ => fail("Expected a `ConfigParseError` on missing conf file")
    }

    //parseConfig for non-existent file should return a None
    Cli.parseConfig(
      Array(
        "--config-file",
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
