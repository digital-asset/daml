// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release

import better.files.File
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.logging.LogEntry
import org.scalatest.Assertion

import java.io.ByteArrayInputStream
import scala.sys.process.*

/** The `CliIntegrationTest` tests Canton command line options by instantiating a Canton binary in a
  * new process with the to-be-tested CLI options as arguments. Before being able to run these tests
  * locally, you need to execute `sbt bundle` and `sbt package`.
  */
abstract class CliIntegrationTest extends ReleaseArtifactIntegrationTestUtils {
  // turn off cache-dir to avoid compilation errors due to concurrent cache access
  protected lazy val cacheTurnOff =
    s"$resourceDir/config-snippets/disable-ammonite-cache.conf"

  protected lazy val simpleConf =
    "community/app/src/pack/examples/01-simple-topology/simple-topology.conf"

  // Message printed out by the bootstrap script if Canton is started successfully
  protected lazy val successMsg = "The last emperor is always the worst."
  private lazy val cantonShouldStartFlags =
    s"--verbose --no-tty --config $cacheTurnOff --bootstrap $resourceDir/scripts/bootstrap.canton"

  "Calling Canton" should {

    "print out the help message when using the --help flag" in { processLogger =>
      s"$cantonBin --help" ! processLogger
      checkOutput(
        processLogger,
        shouldContain = Seq("Usage: canton [daemon|run|generate|sandbox] [options] <args>..."),
      )
    }

    "print out the help message when using no flag" in { processLogger =>
      s"$cantonBin" ! processLogger
      checkOutput(
        processLogger,
        shouldContain = Seq("Usage: canton [daemon|run|generate|sandbox] [options] <args>..."),
        shouldSucceed = false,
      )
    }

    "successfully start and exit after using a run script" in { processLogger =>
      s"$cantonBin run $resourceDir/scripts/run.canton --config $simpleConf --verbose --no-tty" ! processLogger
      checkOutput(processLogger, shouldContain = Seq(successMsg), shouldSucceed = false)
    }

    "print out the Canton version when using the --version flag" in { processLogger =>
      s"$cantonBin --version" ! processLogger
      checkOutput(
        processLogger,
        shouldContain = Seq("Canton", "Daml Libraries", BuildInfo.stableProtocolVersions.toString),
      )
    }

    "successfully start a Canton node when using a mix of a --config and -C config" in {
      processLogger =>
        s"$cantonBin --config $simpleConf -C canton.participants.participant1.parameters.admin-workflow.bong-test-max-level=9000 $cantonShouldStartFlags" ! processLogger
        checkOutput(processLogger, shouldContain = Seq(successMsg))
    }

    "successfully start canton sandbox" in { processLogger =>
      s"$cantonBin sandbox --exit-after-bootstrap" ! processLogger
      checkOutput(processLogger, shouldContain = Seq("Canton sandbox is ready"))
    }

    "successfully start canton sandbox on bespoke ports" in { processLogger =>
      val portNames = List(
        "ledger-api-port",
        "admin-api-port",
        "json-api-port",
        "sequencer-public-port",
        "sequencer-admin-port",
        "mediator-admin-port",
      )
      val portNamesWithValues = portNames.zip(5500 until 5500 + portNames.length)
      val portsArgString = portNamesWithValues.foldLeft("") { (acc, elem) =>
        acc + s"--${elem._1} ${elem._2} "
      }
      val portsAsserts = portNamesWithValues.map(elem => s"port=\"${elem._2}\"")
      val sandboxLogName = "log/new-sandbox.log"
      Process(s"rm -f $sandboxLogName", Some(new java.io.File(cantonDir))) !;
      s"$cantonBin sandbox --exit-after-bootstrap $portsArgString --log-file-name $sandboxLogName" ! processLogger
      checkOutput(processLogger, Seq("Canton sandbox is ready"))
      val logFile = File(sandboxLogName)
      assert(logFile.exists)
      val contents = logFile.contentAsString
      portsAsserts.foreach(portLine => assert(contents.contains(portLine)))
    }

    "successfully start a Canton node when configured only using -C" in { processLogger =>
      s"""$cantonBin
          | -C canton.participants.participant1.storage.type=memory
          | -C canton.participants.participant1.admin-api.port=5012
          | -C canton.participants.participant1.ledger-api.port=5011
          | -C canton.sequencers.sequencer1.sequencer.config.storage.type=memory
          | -C canton.sequencers.sequencer1.sequencer.type=reference
          | -C canton.sequencers.sequencer1.storage.type=memory
          | $cantonShouldStartFlags""".stripMargin ! processLogger
      checkOutput(processLogger, shouldContain = Seq(successMsg))
    }

    "not shadow bootstrap script variables with the bootstrap script file name" in {
      processLogger =>
        s"$cantonBin --config $cacheTurnOff --config $simpleConf --no-tty --bootstrap $resourceDir/scripts/participant1.canton " ! processLogger

        checkOutput(processLogger, shouldContain = Seq(successMsg))
    }

    "change logging directory, log level and log format when using the appropriate CLI flags" in {
      processLogger =>
        s"$cantonBin --config $cacheTurnOff --log-truncate --log-file-appender flat --config $simpleConf --no-tty --bootstrap $resourceDir/scripts/bootstrap.canton --log-file-name log/new-name.log --log-level-canton DEBUG --log-encoder json" ! processLogger

        checkOutput(processLogger, shouldContain = Seq(successMsg))
        val logFile = File("log/new-name.log")
        assert(logFile.exists)
        val contents = logFile.contentAsString
        assert(contents.contains("\"level\":\"DEBUG\""))
        assert(contents.contains(",\"message\":\"Starting Canton version "))
    }

    "run with log last errors disabled" in { processLogger =>
      s"$cantonBin --log-last-errors=false --config $simpleConf $cantonShouldStartFlags" ! processLogger
      checkOutput(
        processLogger,
        shouldContain = Seq(successMsg),
      )
    }

    "log last errors in separate file" in { processLogger =>
      s"$cantonBin --config $cacheTurnOff --log-truncate --log-file-appender flat --config $simpleConf --no-tty --bootstrap $resourceDir/scripts/bootstrap-with-error.canton --log-file-name log/canton-without-debug.log" ! processLogger

      // Make sure the main log file does not contain debug-level log entries
      val logFile = File("log/canton-without-debug.log")
      val logContents = logFile.contentAsString
      assert(!logContents.contains("some logging debug event"))
      assert(logContents.contains("some logging error"))

      val lastErrorsLogFile = File("log/canton_errors.log")
      lastErrorsLogFile.lineCount shouldEqual 4
      val errorContents = lastErrorsLogFile.contentAsString
      // Errors file must include debug output
      forEvery(List("some logging debug event", "some logging error"))(errorContents.contains)
    }

    "dynamically set log level with log last errors enabled" in { processLogger =>
      s"$cantonBin --config $cacheTurnOff --log-truncate --log-file-appender flat --config $simpleConf --no-tty --bootstrap $resourceDir/scripts/bootstrap-with-error-dynamic.canton --log-file-name log/canton-partial-debug.log" ! processLogger

      val logFile = File("log/canton-partial-debug.log")
      val logContents = logFile.contentAsString

      assert(!logContents.contains("some logging debug event"))
      assert(logContents.contains("final logging debug event"))

      val lastErrorsLogFile = File("log/canton_errors.log")
      lastErrorsLogFile.lineCount shouldEqual 6
      val errorContents = lastErrorsLogFile.contentAsString
      // Errors file must include debug output
      forEvery(
        List(
          "some logging debug event",
          "some logging error",
          "final logging debug event",
          "final logging error",
        )
      )(errorContents.contains)
    }

    "run with log file appender off" in { processLogger =>
      s"$cantonBin --log-file-appender=off --config $simpleConf $cantonShouldStartFlags" ! processLogger
      checkOutput(
        processLogger,
        shouldContain = Seq(successMsg),
      )
    }

    "log number of threads at info level" in { processLogger =>
      Process("rm -f log/canton.log", Some(new java.io.File(cantonDir))) !

      val basicCommand =
        // user-manual-entry-begin: SetNumThreads
        "bin/canton -Dscala.concurrent.context.numThreads=12 --config examples/01-simple-topology/simple-topology.conf"
      // user-manual-entry-end: SetNumThreads
      val cmd = basicCommand + " --no-tty"

      val inputStream = new ByteArrayInputStream("exit\n".getBytes)

      Process(cmd, Some(new java.io.File(cantonDir))) #< inputStream ! processLogger

      val logLines = (File(cantonDir) / "log" / "canton.log").lines()

      val expectedLine =
        // user-manual-entry-begin: LogNumThreads
        "INFO  c.d.canton.environment.Environment - Deriving 12 as number of threads from '-Dscala.concurrent.context.numThreads'."
      // user-manual-entry-end: LogNumThreads

      forAtLeast(1, logLines)(_ should endWith(expectedLine))

      checkOutput(processLogger)
    }

    "turn a local config into a remote" in { processLogger =>
      s"$cantonBin generate remote-config --config $simpleConf " ! processLogger
      Seq(
        "remote-participant1.conf",
        "remote-participant2.conf",
        "remote-sequencer1.conf",
        "remote-mediator1.conf",
      ).foreach { check =>
        val fl = File(check)
        assert(fl.exists, s"$check is missing")
      }
    }

    "return failure exit code on script failure" when {
      def test(
          scriptFirstLine: String,
          isDaemon: Boolean,
          expectedExitCode: Int,
          expectedErrorLines: Seq[String],
          logFileName: String,
      )(
          extraOutputAssertion: String => Assertion = _ => succeed
      )(processLogger: FixtureParam): Unit =
        File.usingTemporaryFile(prefix = "script-", suffix = ".sc") { scriptFile =>
          scriptFile.appendLine(scriptFirstLine)

          val runModeArgs =
            if (isDaemon) Seq("daemon", "--bootstrap", scriptFile.toString)
            else Seq("run", scriptFile.toString)

          val exitCode = Process(
            Seq("bin/canton") ++ runModeArgs ++ Seq(
              // turn off cache-dir to avoid compilation errors due to concurrent cache access
              "--config",
              s"$repositoryRootFromCantonDir/$cacheTurnOff",
              "--debug",
              "--log-file-name",
              "log/" + logFileName,
              "-c",
              "demo/demo.conf",
            ),
            Some(new java.io.File(cantonDir)),
          ) ! processLogger

          val out = processLogger.output()
          logger.debug(s"The process has ended now with $exitCode")

          loggerFactory.assertLogsUnordered(
            out
              .split("\\n")
              .foreach(msg => if (msg.contains("ERROR")) logger.error(msg) else logger.debug(msg)),
            expectedErrorLines
              .map(expectedErrorLine =>
                (logEntry: LogEntry) => logEntry.errorMessage should include(expectedErrorLine)
              ) *,
          )

          exitCode shouldBe expectedExitCode
          expectedErrorLines.foreach(expectedLine => out should include(expectedLine))
          extraOutputAssertion(out)
        }

      "script (run) does not compile" in {
        test(
          scriptFirstLine = "I shall not compile",
          isDaemon = false,
          expectedExitCode = 1,
          expectedErrorLines = Seq("Script execution failed: Compilation Failed"),
          logFileName = "runDoesNotCompile.log",
        )(_ should include("not found: value I"))
      }

      "script (run) compiles but throws" in {
        test(
          scriptFirstLine = """throw new RuntimeException("some exception")""",
          isDaemon = false,
          expectedExitCode = 1,
          expectedErrorLines =
            Seq("Script execution failed: java.lang.RuntimeException: some exception"),
          logFileName = "runCompilesButThrows.log",
        )()
      }

      "script (daemon) does not compile" in {
        test(
          scriptFirstLine = "I shall not compile",
          isDaemon = true,
          expectedExitCode = 3, // Bootstrap scripts exit with 3
          expectedErrorLines = Seq("Bootstrap script terminated with an error"),
          logFileName = "daemonDoesNotCompile.log",
        )(_ should include("not found: value I"))
      }

      "script (daemon) compiles but throws" in {
        test(
          scriptFirstLine = """throw new RuntimeException("some exception")""",
          isDaemon = true,
          expectedExitCode = 3, // Bootstrap scripts exit with 3
          expectedErrorLines = Seq(
            "Bootstrap script terminated with an error: java.lang.RuntimeException: some exception"
          ),
          logFileName = "daemonCompilesButThrows.log",
        )()
      }
    }
  }
}

class CommunityCliIntegrationTest extends CliIntegrationTest with CommunityReleaseTest
