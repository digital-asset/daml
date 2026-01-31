// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.integration.{CommunityIntegrationTest, ConfigTransforms, EnvironmentDefinition, SharedEnvironment, TestConsoleEnvironment}
import org.scalatest.BeforeAndAfterAll
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.daml.lf.archive.DarDecoder
import monocle.macros.syntax.lens.*
import com.digitalasset.daml.lf.engine.script.{RunnerMain, RunnerMainConfig}
import com.digitalasset.daml.lf.engine.script.RunnerMainConfig.RunMode.RunOne
import com.daml.tls.TlsConfiguration
import com.digitalasset.daml.lf.engine.script.ParticipantMode.RemoteParticipantHost
import org.scalatest.Assertion
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode.Static

import java.nio.file.{FileSystems, Files, Path}

/** Generate and save snapshot data by running Daml script code.
  *
  * The following environment variables provide test arguments:
  * - DAR_FILE:     defines the actual Dar file containing the script code
  * - SCRIPT_NAME:  defines the script function that should be ran to generate transaction entries for the snapshot file - e.g. Module:Script
  * - SNAPSHOT_DIR: defines the (base) directory used for storing snapshot data. Snapshot files are saved in the file with
  *   path $SNAPSHOT_DIR/$(basename DAR_FILE)/Script/snapshot-participant0*.bin (where Script is the script function name defined by $SCRIPT_NAME)
  */
// Integration tests need to live in the package com.digitalasset.canton.integration.tests, so we
// make the test base an abstract class
abstract class GenerateSnapshotsBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with BeforeAndAfterAll {

  private var snapshotBaseDir: Path = _
  private var scriptDarPath: Path = _
  private var scriptEntryPoint: Ref.QualifiedName = _

  override protected def beforeAll(): Unit = {
    assume(
      Seq("DAR_FILE", "SCRIPT_NAME", "SNAPSHOT_DIR")
        .forall(envVar => sys.env.contains(envVar)),
      "The environment variables DAR_FILE, SCRIPT_NAME and SNAPSHOT_DIR all need to be set"
    )

    snapshotBaseDir = Path.of(sys.env("SNAPSHOT_DIR"))
    scriptDarPath = Path.of(sys.env("DAR_FILE"))
    scriptEntryPoint = Ref.QualifiedName.assertFromString(sys.env("SCRIPT_NAME"))

    super.beforeAll()
  }

  lazy val snapshotDir = snapshotBaseDir.resolve(s"${scriptDarPath.getFileName}/${scriptEntryPoint.name}")
  lazy val participantId = Ref.ParticipantId.assertFromString("participant1")
  lazy val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")
  val workflowDarFile = "ReplayBenchmark.dar"
  val workflowDarPath: Path =
    Option(getClass.getClassLoader.getResource(workflowDarFile))
      .map(path => Path.of(path.getPath))
      .getOrElse(throw new IllegalArgumentException(s"Cannot find resource $workflowDarFile"))
  val workflowPkgId: LfPackageId = getPkgId(workflowDarPath)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableNonStandardConfig,
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.features.snapshotDir).replace(Some(snapshotDir))
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
        }

        // Upload the workflow DAR and vet its packages
        SetupPackageVetting(
          Set(workflowDarPath.toFile.getAbsolutePath),
          targetTopology = Map(
            daId -> participants.all
              .map(
                _ -> VettedPackage
                  .unbounded(
                    Seq(workflowPkgId)
                  )
                  .toSet
              )
              .toMap
          ),
        )
      }

  private def runWhenEnvVarSet(name: String)(testFun: TestConsoleEnvironment => Assertion): Unit = {
    if (sys.env.contains("STANDALONE")) {
      name.in(testFun)
    } else {
      name.ignore(testFun)
    }
  }

  runWhenEnvVarSet("Generate snapshot data") { implicit env =>
    import env.*

    println(s"Using script ${scriptDarPath.getFileName}/${scriptEntryPoint.name}")

    val scriptConfig = getConfig(
      participant1.config.ledgerApi.address,
      participant1.config.ledgerApi.port,
    )

    RunnerMain.main(scriptConfig)

    val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
    snapshotFiles.size() should be(1)
  }

  private def getPkgId(darPath: Path): LfPackageId =
    DarDecoder.assertReadArchiveFromFile(darPath.toFile).main._1

  private def getConfig(host: String, port: Port): RunnerMainConfig =
    RunnerMainConfig(
      darPath = scriptDarPath.toFile,
      runMode = RunOne(scriptEntryPoint.toString, None, None),
      participantMode = RemoteParticipantHost(host, port.unwrap),
      timeMode = Static,
      accessTokenFile = None,
      tlsConfig = TlsConfiguration(enabled = false),
      maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
      userId = None,
      uploadDar = true,
    )
}
