// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  def loadCli(file: String) = {
    Cli.parse(Array("--config", file)).getOrElse(fail())
  }

  "should pickup the config file provided" in {
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile.nonEmpty shouldBe true
  }

  "should take default values on loading minimal config" in {
    val file = requiredResource("triggers/service/auth/src/test/resources/oauth2-middleware-minimal.conf")
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile.nonEmpty shouldBe true
    cli.loadConfig match {
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
    cli.configFile.nonEmpty shouldBe true
    cli.loadConfig match {
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

        c.oauthAuthTemplate shouldBe Some(Paths.get("file://path/auth/template"))
        c.oauthTokenTemplate shouldBe Some(Paths.get("file://path/token/template"))
        c.oauthRefreshTemplate shouldBe Some(Paths.get("file://path/refresh/template"))

        c.clientId shouldBe sys.env.getOrElse("DAML_CLIENT_ID", "foo")
        c.clientSecret shouldBe SecretString(sys.env.getOrElse("DAML_CLIENT_SECRET", "bar"))

        c.tokenVerifier match {
          case _: JwksVerifier => succeed
          case _ => fail("expected JwksVerifier based on supplied config")
        }

    }
  }

  "should fail on non-existent config file" in {
    val cli = loadCli("missingFile.conf")
    val cfg = cli.loadConfig
    cli.configFile.nonEmpty shouldBe true
    cfg match {
      case Left(err) =>
        err.msg shouldBe "Unable to read file missingFile.conf (No such file or directory)."
      case Right(_) => fail()
    }
  }

  "should fail on missing config file option" in {
    val cli = Cli()
    val cfg = cli.loadConfig
    cfg shouldBe Left(MissingConfigError)
  }
}
