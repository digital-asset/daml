// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.cli

import ch.qos.logback.classic.Level
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayOutputStream, File}

class CliTest extends AnyWordSpec with BaseTest {
  "parse" can {
    "when no config files are provided" should {
      "fail horribly" in {
        val (result, _) = parse("")

        result shouldBe None
      }
    }

    "when a single config file is provided" should {
      "work fine" in {
        val (maybeCli: Option[Cli], _) = parse("--config some-file.conf")

        maybeCli.value.configFiles.map(_.getName) should contain only "some-file.conf"
      }
    }

    "when a bunch of config files are provided" should {
      "parse them all in order" in {
        val (maybeCli: Option[Cli], _) = parse("--config first.conf --config second.conf")

        maybeCli.value.configFiles.map(_.getName) shouldBe Seq("first.conf", "second.conf")
      }
    }

    "when config key values are provided" should {
      "parse them all" in {
        val (maybeCli: Option[Cli], _) =
          parse("-C canton.a.b=foo -C canton.c.d=bar,canton.e.f=baba -C canton.a.b=ignored")
        maybeCli.value.configMap shouldBe Map(
          "canton.a.b" -> "foo",
          "canton.c.d" -> "bar",
          "canton.e.f" -> "baba",
        )
      }
    }

    "when setting flags" should {
      "parse them all successfully" in {
        val (maybeCli: Option[Cli], _) =
          parse("--config first.conf -v --no-tty --auto-connect-local")
        val cli = maybeCli.value
        cli.levelCanton should contain(Level.DEBUG)
        cli.noTty && cli.autoConnectLocal shouldBe true
      }
      "parse logging flags successfully" in {
        val (maybeCli: Option[Cli], _) = parse(
          "--auto-connect-local -C canton.parameters.manual-start=yes --log-truncate --log-level-root=DEBUG --log-level-canton=DEBUG --log-level-stdout=ERROR --log-file-appender=rolling --log-file-name=log/wurst.log --log-file-rolling-history=20 --log-file-rolling-pattern=YYYY-mm-dd-HH --log-last-errors=false"
        )
        val cli = maybeCli.value
        cli.logFileAppender shouldBe LogFileAppender.Rolling
        cli.levelRoot shouldBe Some(Level.DEBUG)
        cli.levelCanton shouldBe Some(Level.DEBUG)
        cli.levelStdout shouldBe Level.ERROR
        cli.logTruncate shouldBe true
        cli.logFileName should contain("log/wurst.log")
        cli.logFileHistory should contain(20)
        cli.logFileRollingPattern should contain("YYYY-mm-dd-HH")
        cli.logLastErrors shouldBe false
        cli.autoConnectLocal shouldBe true
      }

      "ensure that command flags allow overriding profile definitions" in {
        val (maybeCli: Option[Cli], _) =
          parse(
            "--config first.conf --log-file-rolling-history=20 --log-profile=container --log-level-stdout=TRACE"
          )
        val cli = maybeCli.value
        cli.logFileHistory should contain(10) // should be overridden by log profile
        cli.levelStdout shouldBe Level.TRACE
      }

    }

    "daemon" should {
      "capture bootstrap script if provided" in {
        val (maybeCli: Option[Cli], _) =
          parse("daemon --bootstrap my/script.canton --config whatever.conf")

        val cli = maybeCli.value
        val script = cli.bootstrapScriptPath

        script.value.toString shouldBe "my/script.canton"
      }
    }

    "run" should {
      "fail if a script is not provided" in {
        val (result, _) = parse("run")

        result shouldBe None
      }
      "set the command if a script is available" in {
        val (maybeCli: Option[Cli], _) = parse("run some-file.sc --config some.conf")

        maybeCli.value.command match {
          case Some(Command.RunScript(file: File)) =>
            file.getPath shouldBe "some-file.sc"
          case _ => fail()
        }
      }
    }
  }

  private def parse(text: String): (Option[Cli], String) = {
    val args: Array[String] = if (!text.isEmpty) text.split(" ") else Array()
    val errOutput = new ByteArrayOutputStream()

    val result = Console.withErr(errOutput) { Cli.parse(args) }

    (result, errOutput.toString)
  }
}
