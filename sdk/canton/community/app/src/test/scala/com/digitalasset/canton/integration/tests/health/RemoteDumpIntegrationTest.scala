// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.health

import better.files.File
import com.digitalasset.canton.cli.Cli
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  CommandFailure,
  GrpcAdminCommandRunner,
  HealthDumpGenerator,
  InstanceReference,
}
import com.digitalasset.canton.environment.{Environment, EnvironmentFactory}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseExternalProcess,
  UseH2,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory}
import com.digitalasset.canton.participant.ParticipantNodeBootstrapFactoryImpl
import com.digitalasset.canton.synchronizer.mediator.MediatorNodeBootstrapFactoryImpl
import com.digitalasset.canton.synchronizer.sequencer.SequencerNodeBootstrapFactoryImpl
import com.digitalasset.canton.version.{ProtocolVersionCompatibility, ReleaseVersion}
import com.digitalasset.canton.{HasExecutionContext, config}
import io.circe.generic.auto.*
import io.circe.{Json, JsonObject}
import monocle.macros.syntax.lens.*

import java.nio.charset.Charset
import scala.concurrent.duration.DurationInt

final class RemoteDumpIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext
    with StatusIntegrationTestUtil {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S1M1_Config
      .addConfigTransform(
        _.focus(_.monitoring.dumpNumRollingLogFiles).replace(NonNegativeInt.tryCreate(100))
      )
      .withSetup { implicit env =>
        import env.*
        nodes.remote.foreach(_.health.wait_for_running())
        // This is normally done when running the Canton binary, and sets system properties that are then used in the
        // health dump code to figure out the location of the log file. Doing it here manually, otherwise the health dump
        // code will miss the log file, which is called "canton_test.log" in tests instead of the default "canton.log"
        Cli(logFileName = Some("log/canton_test.log")).installLogging()

        bootstrap.synchronizer(
          "remote-health-synchronizer",
          Seq(rs(sequencer1Name)),
          Seq(rm(mediator1Name)),
          Seq[InstanceReference](rs(sequencer1Name), rm(mediator1Name)),
          synchronizerThreshold = PositiveInt.two,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )

        nodes.remote.foreach(_.health.wait_for_initialized())

        // We add a parameter change to observe it in the health dump
        Seq[InstanceReference](rs(sequencer1Name), rm(mediator1Name)).foreach { owner =>
          owner.topology.synchronizer_parameters.propose_update(
            rs(sequencer1Name).physical_synchronizer_id,
            _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(2000000)),
          )
        }
        utils.synchronize_topology()

        rp(participant1Name).synchronizers.connect(
          rs(sequencer1Name),
          daName,
        )
        rp(participant1Name).health.ping(rp(participant1Name).id)
      }

  private val participant1Name = "participant1"
  private val sequencer1Name = "sequencer1"
  private val mediator1Name = "mediator1"
  private implicit val unzipCharset: Charset = Charset.forName("UTF-8")
  private lazy val external =
    new UseExternalProcess(
      loggerFactory,
      externalParticipants = Set(participant1Name),
      externalSequencers = Set(sequencer1Name),
      externalMediators = Set(mediator1Name),
      fileNameHint = this.getClass.getSimpleName,
    )

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(external)

  private def verifyHealthDumpContent(
      dumpFile: File,
      env: TestConsoleEnvironment,
      withRollingFile: Boolean = false,
  ): Unit =
    File.usingTemporaryDirectory() { dir =>
      dumpFile.unzipTo(dir)
      val localZip = dir.glob("local-*").nextOption().value
      val sequencerZip = dir.glob("remote-sequencer1-*").nextOption().value
      val mediatorZip = dir.glob("remote-mediator1-*").nextOption().value
      val participant1Zip = dir.glob("remote-participant1-*").nextOption().value
      val synchronizerId = env.rs(sequencer1Name).physical_synchronizer_id

      // Check that the local zip contains the correct files
      File.usingTemporaryDirectory() { localUnzip =>
        localZip.unzipTo(localUnzip)
        (localUnzip / "canton_test.log").exists shouldBe true
        val json = localUnzip.glob("canton-dump*.json").nextOption().value
        val parsed = io.circe.parser.decode[JsonDump](json.contentAsString).value

        if (env.participant2.is_running)
          parsed.status("participantStatus")(env.participant2.name) shouldBe defined
        else
          parsed.status("unreachableParticipants")(env.participant2.name) shouldBe defined

        if (env.participant3.is_running)
          parsed.status("participantStatus")(env.participant3.name) shouldBe defined
        else
          parsed.status("unreachableParticipants")(env.participant3.name) shouldBe defined
      }

      def assertNodeVersion(json: Option[Json]) =
        json.getOrElse(fail()).findAllByKey("version").map(_.asString) shouldBe List(
          Some(ReleaseVersion.current.fullVersion)
        )

      def assertParticipantSupportedProtocolVersions(json: Option[Json]) = {
        val jsons = json.getOrElse(fail()).findAllByKey("supportedProtocolVersions")
        val supportedPvs = ProtocolVersionCompatibility
          .supportedProtocols(
            testedProtocolVersion.isAlpha,
            testedProtocolVersion.isBeta,
            ReleaseVersion.current,
          )
        val jsonString = jsons.map(_.asArray).mkString
        supportedPvs.map(_.toString).exists(jsonString.contains(_)) shouldBe true
      }

      def assertSynchronizerProtocolVersion(json: Option[Json]) =
        json.getOrElse(fail()).findAllByKey("protocolVersion").map(_.asString) shouldBe List(
          Some(testedProtocolVersion.toString)
        )

      def assertSynchronizerParameters(json: Option[Map[String, Json]], nodeName: String) = {
        val parameters = json.valueOrFail("synchronizerParameters were empty")
        parameters should have size 1
        val (nodeNameFromTheDump, nodeParameters) = parameters.headOption.value
        nodeNameFromTheDump shouldBe nodeName
        val synchronizerKeys = nodeParameters.asObject.value.keys.toSeq
        synchronizerKeys should have size 1
        synchronizerKeys.headOption.value shouldBe synchronizerId.toString

        val parameterChanges =
          nodeParameters.asObject.value.apply(synchronizerId.toString).value.asArray.value
        parameterChanges should have size 1

        val confirmationRequestsMaxRateValue = parameterChanges.headOption.value.asObject.value
          .apply("parameters")
          .value
          .asObject
          .value
          .apply("participantSynchronizerLimits")
          .value
          .asObject
          .value
          .apply("confirmationRequestsMaxRate")
          .value
          .asNumber
          .value
          .toInt
          .value
        confirmationRequestsMaxRateValue shouldBe 2000000
      }

      // Check that the sequencer zip contains the correct files
      File.usingTemporaryDirectory() { daUnzip =>
        sequencerZip.unzipTo(daUnzip)
        (daUnzip / external
          .logFile(sequencer1Name)
          .getFileName
          .toString).exists shouldBe true
        val json = daUnzip.glob("canton-dump*.json").nextOption().value
        val parsed = io.circe.parser.decode[JsonDump](json.contentAsString).value
        val sequencerJson = parsed.status("sequencerStatus")(sequencer1Name)
        sequencerJson shouldBe defined
        assertNodeVersion(sequencerJson)
        assertSynchronizerProtocolVersion(sequencerJson)
        assertSynchronizerParameters(parsed.synchronizerParameters, sequencer1Name)
      }

      // Check that the mediator zip contains the correct files
      File.usingTemporaryDirectory() { daUnzip =>
        mediatorZip.unzipTo(daUnzip)
        (daUnzip / external
          .logFile(mediator1Name)
          .getFileName
          .toString).exists shouldBe true
        val json = daUnzip.glob("canton-dump*.json").nextOption().value
        val parsed = io.circe.parser.decode[JsonDump](json.contentAsString).value
        val mediatorJson = parsed.status("mediatorStatus")(mediator1Name)
        mediatorJson shouldBe defined
        assertNodeVersion(mediatorJson)
        assertSynchronizerProtocolVersion(mediatorJson)
        assertSynchronizerParameters(parsed.synchronizerParameters, mediator1Name)
      }

      // Check that the participant1 zip contains the correct files
      File.usingTemporaryDirectory() { participantUnzip =>
        participant1Zip.unzipTo(participantUnzip)
        (participantUnzip / external
          .logFile(participant1Name)
          .getFileName
          .toString).exists shouldBe true
        if (withRollingFile) {
          (participantUnzip / (external
            .logFile(participant1Name)
            .getFileName
            .toString + ".1.gz")).exists shouldBe true
        }
        val json = participantUnzip.glob("canton-dump*.json").nextOption().value
        val parsed = io.circe.parser.decode[JsonDump](json.contentAsString).value
        val participant1Json = parsed.status("participantStatus")(participant1Name)
        participant1Json shouldBe defined
        assertNodeVersion(participant1Json)
        assertParticipantSupportedProtocolVersions(participant1Json)
        assertSynchronizerParameters(parsed.synchronizerParameters, participant1Name)
      }
    }

  "get a remote health dump" in { implicit env =>
    File.usingTemporaryFile() { f =>
      val dumpFile = File(env.health.dump(outputFile = f.canonicalPath))
      dumpFile.pathAsString shouldBe f.pathAsString
      verifyHealthDumpContent(dumpFile, env)
    }
  }

  "stream health dump in multiple chunks" in { implicit env =>
    File.usingTemporaryFile() { f =>
      val dumpFile = File(env.health.dump(outputFile = f.canonicalPath, chunkSize = Option(10000)))
      dumpFile.size > 10000 shouldBe true // Make sure the file was actually larger than 10000 bytes
      verifyHealthDumpContent(dumpFile, env)
    }
  }

  "gather rolling log files" in { implicit env =>
    File.usingTemporaryFile() { f =>
      val p1LogFile = File(external.logFile(participant1Name))
      p1LogFile.parent.createChild(p1LogFile.name + ".1.gz")

      val dumpFile = File(env.health.dump(outputFile = f.canonicalPath, chunkSize = Option(10000)))
      dumpFile.size > 10000 shouldBe true // Make sure the file was actually larger than 10000 bytes
      verifyHealthDumpContent(dumpFile, env, withRollingFile = true)
    }
  }

  "try to get health dump from other local nodes if one fails" in { implicit env =>
    env.participant2.stop()

    val expectedWarnings = LogEntry.assertLogSeq(
      mustContainWithClue = Seq(
        (
          _.message should include(
            "NODE_NOT_STARTED"
          ),
          "node not started message not logged",
        )
      ),
      Seq.empty,
    ) _

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      File.usingTemporaryFile() { f =>
        val dumpFile = File(env.health.dump(outputFile = f.canonicalPath))
        dumpFile.pathAsString shouldBe f.pathAsString
        verifyHealthDumpContent(dumpFile, env)
      },
      expectedWarnings,
    )
  }

  // Class to parse only the status part of the dump to be able to do some validation on the content
  case class JsonDump(
      status: Map[String, JsonObject],
      synchronizerParameters: Option[Map[String, Json]],
  )

}

class NegativeRemoteDumpIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  private val dumpDelay = 1.second

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  // Customize the environment factory to tweak the health dump generation
  override protected val environmentFactory: EnvironmentFactory =
    (
        config: CantonConfig,
        loggerFactory: NamedLoggerFactory,
        testingConfigInternal: TestingConfigInternal,
    ) =>
      new Environment(
        config,
        testingConfigInternal,
        ParticipantNodeBootstrapFactoryImpl,
        SequencerNodeBootstrapFactoryImpl,
        MediatorNodeBootstrapFactoryImpl,
        loggerFactory,
      ) {
        override def createHealthDumpGenerator(
            commandRunner: GrpcAdminCommandRunner
        ): HealthDumpGenerator =
          new HealthDumpGenerator(this, commandRunner, this.loggerFactory) {
            override def generateHealthDump(
                outputFile: File,
                extraFilesToZip: Seq[File],
            ): Unit =
              Threading.sleep(
                dumpDelay.toMillis
              ) // Add a delay to allow triggering of timeout
          }
      }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  "dump command should timeout" in { implicit env =>
    val expectedWarnings = LogEntry.assertLogSeq(
      mustContainWithClue = Seq(
        (
          _.message should include("CONSOLE_COMMAND_TIMED_OUT"),
          "health dump timeout message not logged",
        )
      ),
      Seq.empty,
    ) _

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      File.usingTemporaryFile() { f =>
        a[CommandFailure] shouldBe thrownBy {
          // Returns as soon as the first call (future) fails (times out)
          env.health.dump(
            outputFile = f.canonicalPath,
            timeout = config.NonNegativeDuration(
              dumpDelay / 100 // Timeout much lower than the artificial delay to make sure it triggers
            ),
          )
        }
        // As we make calls to multiple nodes (participant, sequencer, mediator), to avoid errors in the logs,
        //  we, unfortunately, need this `sleep` to give enough time not only for all the calls (futures) to time out
        //  locally but also the gRPC requests on the remote nodes to cancel properly. Otherwise, the test might
        //  already start the shutdown procedure and interrupt the requests resulting in unsuppressed errors.
        Threading.sleep((dumpDelay * 3).toMillis)
      },
      expectedWarnings,
    )
  }
}
