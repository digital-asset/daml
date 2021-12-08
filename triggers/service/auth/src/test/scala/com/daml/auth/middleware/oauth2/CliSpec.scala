// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import com.daml.bazeltools.BazelRunfiles.requiredResource

object CliSpec {
  def resourceFile(name: String) = {
    getClass.getClassLoader.getResource(name)
  }

}
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

  "should be able to successfully load the config based on the file provided" in {
    val file = requiredResource(confFile)
    val cli = loadCli(file.getAbsolutePath)
    cli.configFile.nonEmpty shouldBe true
    cli.loadConfig match {
      case Left(ex) => fail(ex.msg)
      case Right(c) =>
        c.address shouldBe "127.0.0.1"
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
