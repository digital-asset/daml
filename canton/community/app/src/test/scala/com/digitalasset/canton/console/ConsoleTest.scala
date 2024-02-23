// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import ammonite.runtime.Storage.InMemory
import ammonite.util.Colors
import com.digitalasset.canton.admin.api.client.commands.{
  GrpcAdminCommand,
  ParticipantAdminCommands,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{CantonCommunityConfig, ClientConfig, TestingConfigInternal}
import com.digitalasset.canton.console.CommandErrors.GenericCommandError
import com.digitalasset.canton.console.HeadlessConsole.{
  CompileError,
  HeadlessConsoleError,
  RuntimeError,
}
import com.digitalasset.canton.domain.mediator.MediatorNodeBootstrapX
import com.digitalasset.canton.domain.sequencing.SequencerNodeBootstrapX
import com.digitalasset.canton.environment.*
import com.digitalasset.canton.metrics.OnDemandMetricsReader.NoOpOnDemandMetricsReader$
import com.digitalasset.canton.participant.{ParticipantNodeBootstrapX, ParticipantNodeX}
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.{BaseTest, ConfigStubs}
import io.grpc.stub.AbstractStub
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{anyString, eq as isEq}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayOutputStream
import java.nio.file.Paths

class ConsoleTest extends AnyWordSpec with BaseTest {

  lazy val DefaultConfig: CantonCommunityConfig = CantonCommunityConfig(
    sequencers = Map(
      InstanceName.tryCreate("s1") -> ConfigStubs.sequencerx,
      InstanceName.tryCreate("s2") -> ConfigStubs.sequencerx,
      InstanceName.tryCreate("s-3") -> ConfigStubs.sequencerx,
    ),
    mediators = Map(
      InstanceName.tryCreate("m1") -> ConfigStubs.mediatorx,
      InstanceName.tryCreate("m2") -> ConfigStubs.mediatorx,
      InstanceName.tryCreate("m-3") -> ConfigStubs.mediatorx,
    ),
    participants = Map(
      InstanceName.tryCreate("p1") -> ConfigStubs.participant
        .copy(adminApi = ConfigStubs.adminApi), // for testing admin api
      InstanceName.tryCreate("p2") -> ConfigStubs.participant,
      InstanceName.tryCreate("new") -> ConfigStubs.participant,
      InstanceName.tryCreate("p-4") -> ConfigStubs.participant,
    ),
  )

  lazy val NameClashConfig: CantonCommunityConfig = CantonCommunityConfig(
    participants = Map(
      // Reserved keyword
      InstanceName.tryCreate("participantsX") -> ConfigStubs.participant,
      // Name collision
      InstanceName.tryCreate("s1") -> ConfigStubs.participant,
    ),
    sequencers = Map(
      InstanceName.tryCreate("s1") -> ConfigStubs.sequencerx
    ),
  )

  abstract class TestEnvironment(val config: CantonCommunityConfig = DefaultConfig) {
    val environment: CommunityEnvironment = mock[CommunityEnvironment]
    val participants: ParticipantNodes[
      ParticipantNodeBootstrapX,
      ParticipantNodeX,
      config.ParticipantConfigType,
    ] =
      mock[
        ParticipantNodes[ParticipantNodeBootstrapX, ParticipantNodeX, config.ParticipantConfigType]
      ]
    val sequencersX: SequencerNodesX[config.SequencerNodeXConfigType] =
      mock[SequencerNodesX[config.SequencerNodeXConfigType]]
    val mediatorsX: MediatorNodesX[config.MediatorNodeXConfigType] =
      mock[MediatorNodesX[config.MediatorNodeXConfigType]]
    val participant: ParticipantNodeBootstrapX = mock[ParticipantNodeBootstrapX]
    val sequencer: SequencerNodeBootstrapX = mock[SequencerNodeBootstrapX]
    val mediator: MediatorNodeBootstrapX = mock[MediatorNodeBootstrapX]

    when(environment.config).thenReturn(config)
    when(environment.testingConfig).thenReturn(
      TestingConfigInternal(initializeGlobalOpenTelemetry = false)
    )
    when(environment.participants).thenReturn(participants)
    when(environment.sequencers).thenReturn(sequencersX)
    when(environment.mediators).thenReturn(mediatorsX)
    when(environment.simClock).thenReturn(None)
    when(environment.loggerFactory).thenReturn(loggerFactory)
    when(environment.configuredOpenTelemetry).thenReturn(
      ConfiguredOpenTelemetry(
        OpenTelemetrySdk.builder().build(),
        SdkTracerProvider.builder(),
        NoOpOnDemandMetricsReader$,
        metricsEnabled = false,
      )
    )
    type NodeGroup = Seq[(String, Nodes[CantonNode, CantonNodeBootstrap[CantonNode]])]
    when(environment.startNodes(any[NodeGroup])(anyTraceContext)).thenReturn(Right(()))

    when(participants.startAndWait(anyString())(anyTraceContext)).thenReturn(Right(()))
    when(participants.stopAndWait(anyString())(anyTraceContext)).thenReturn(Right(()))
    when(participants.isRunning(anyString())).thenReturn(true)

    val adminCommandRunner: ConsoleGrpcAdminCommandRunner = mock[ConsoleGrpcAdminCommandRunner]
    val testConsoleOutput: TestConsoleOutput = new TestConsoleOutput(loggerFactory)

    // Setup default admin command response
    when(
      adminCommandRunner
        .runCommand(
          anyString(),
          any[GrpcAdminCommand[_, _, Nothing]],
          any[ClientConfig],
          isEq(None),
        )
    )
      .thenReturn(GenericCommandError("Mocked error"))

    val consoleEnvironment =
      new CommunityConsoleEnvironment(
        environment,
        consoleOutput = testConsoleOutput,
        createAdminCommandRunner = _ => adminCommandRunner,
      )

    def runOrFail(commands: String*): Unit = {
      val (result, stderr) = run(commands*)

      // fail if unexpected content was printed to stderr (this likely indicates an error of some form which wasn't bubbled up through the interpreter)
      assertExpectedStdErrorOutput(stderr)

      // fail if the run was unsuccessful
      result shouldBe Right(())
    }

    def run(commands: String*): (Either[HeadlessConsoleError, Unit], String) = {
      // put a newline at the end to ensure it's run
      val input = commands.mkString(s";${System.lineSeparator}") + System.lineSeparator

      // capture output
      val errorStream = new ByteArrayOutputStream()

      // run headless but direct stderr to a captured stream
      val result = HeadlessConsole.run(
        consoleEnvironment,
        input,
        path = None,
        _.copy(
          errorStream = errorStream,
          colors =
            Colors.BlackWhite, // as pretty as colors are, it really messes up the regular expressions we run for verification
          storageBackend =
            InMemory(), // due to an odd jenkins/docker-in-jenkins thing the `user.home` env var isn't set that blocks up ammonite's default Main() ctor for storage
          wd = os.Path(Paths.get(".").toAbsolutePath),
        ),
        logger = logger,
      )

      (result, errorStream.toString)
    }

    def setupAdminCommandResponse[Svc <: AbstractStub[Svc], Result](
        id: String,
        result: Either[String, Result],
    ): Unit =
      when(
        adminCommandRunner.runCommand(
          isEq((id)),
          any[GrpcAdminCommand[_, _, Result]],
          any[ClientConfig],
          isEq(None),
        )
      )
        .thenReturn(result.toResult)

    private val expectedErrorLinePatterns = Seq("Compiling .*", "Bye!")
    private def isExpectedStdErrorOutput(stderr: String): Boolean =
      stderr
        .split(System.lineSeparator)
        .filterNot(_.isEmpty)
        .forall(line => expectedErrorLinePatterns.exists(line.matches))

    def assertExpectedStdErrorOutput(stderr: String): Assertion =
      assert(
        isExpectedStdErrorOutput(stderr),
        s"stderr from REPL included unexpected output:${System.lineSeparator}$stderr",
      )
  }

  "Console" can {
    "start a participant" in new TestEnvironment {
      runOrFail("p1 start")
      verify(participants).startAndWait("p1")
    }
    "start a participant with scala keyword as name" in new TestEnvironment {
      runOrFail("`new` start")
      verify(participants).startAndWait("new")
    }
    "start a participant with underscore in name" in new TestEnvironment {
      runOrFail("`p-4` start")
      verify(participants).startAndWait("p-4")
    }
    "stop a participant" in new TestEnvironment {
      runOrFail(
        "p1 start",
        "p1 stop",
      )

      verify(participants).startAndWait("p1")
      verify(participants).stopAndWait("p1")
    }

    def verifyStart(env: TestEnvironment, names: Seq[String]): Assertion = {
      import env.*
      val argCapture: ArgumentCaptor[NodeGroup] =
        ArgumentCaptor.forClass(classOf[NodeGroup])
      verify(environment).startNodes(argCapture.capture())(anyTraceContext)
      argCapture.getValue.map(_._1) shouldBe names
    }

    "start all participants" in new TestEnvironment {
      runOrFail("participantsX.local start")
      verifyStart(this, Seq("p1", "p2", "new", "p-4"))
    }
    "start all sequencers" in new TestEnvironment {
      runOrFail("sequencersX.local start")
      verifyStart(this, Seq("s1", "s2", "s-3"))
    }
    "start all mediators" in new TestEnvironment {
      runOrFail("mediatorsX.local start")
      verifyStart(this, Seq("m1", "m2", "m-3"))
    }
    "start all" in new TestEnvironment {
      runOrFail("nodes.local.start()")
      verifyStart(this, Seq("p1", "p2", "new", "p-4", "s1", "s2", "s-3", "m1", "m2", "m-3"))
    }

    "return a compile error if the code fails to compile" in new TestEnvironment {
      inside(run("This really shouldn't compile")) { case (Left(CompileError(message)), _) =>
        message shouldEqual
          """(synthetic)/ammonite/canton/interpreter/canton$minusscript.sc:1:1 expected end-of-input
              |This really shouldn't compile
              |^""".stripMargin
      }
    }
    "return a runtime error if the code does not run successfully" in new TestEnvironment {
      val (result, _) = run("""sys.error("whoopsie")""")

      inside(result) { case Left(RuntimeError(message, cause)) =>
        cause.getMessage shouldBe "whoopsie"
        cause.getClass shouldBe classOf[RuntimeException]
        message shouldEqual ""
      }
    }

    "participantsX.all.dars.upload should attempt to invoke UploadDar on all participants" in new TestEnvironment {
      setupAdminCommandResponse("p1", Right(Seq()))
      setupAdminCommandResponse("p2", Right(Seq()))
      setupAdminCommandResponse("new", Right(Seq()))
      setupAdminCommandResponse("p-4", Right(Seq()))

      runOrFail(s"""participantsX.all.dars.upload("$CantonExamplesPath", false)""")

      def verifyUploadDar(p: String): ConsoleCommandResult[String] =
        verify(adminCommandRunner).runCommand(
          isEq(p),
          any[ParticipantAdminCommands.Package.UploadDar],
          any[ClientConfig],
          isEq(None),
        )

      verifyUploadDar("p1")
      verifyUploadDar("p2")
      verifyUploadDar("new")
      verifyUploadDar("p-4")
    }

    "participants.local help shows help from both InstanceExtensions and ParticipantExtensions" in new TestEnvironment {
      testConsoleOutput.assertConsoleOutput(
        {
          runOrFail("participantsX.local help")
        },
        { helpText =>
          helpText should include("start") // from instance extensions
          helpText should include("stop")
          helpText should include("dars")
          helpText should include("domains")
        },
      )
    }
  }

  "Console" must {
    "fail on name clashes in config" in new TestEnvironment(NameClashConfig) {
      inside(run("1+1")) { case (Left(RuntimeError(message, ex)), _) =>
        message shouldEqual "Unable to create the console bindings"
        ex.getMessage should startWith(
          """Node names must be unique and must differ from reserved keywords. Please revisit node names in your config file.
            |Offending names: (`participantsX` (2 occurrences), `s1` (2 occurrences))""".stripMargin
        )
      }
    }
  }
}
