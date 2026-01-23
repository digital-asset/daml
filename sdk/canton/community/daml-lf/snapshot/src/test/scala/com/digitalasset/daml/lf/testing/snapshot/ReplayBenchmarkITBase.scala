// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.daml.ledger.api.v2.commands.{Command, CreateAndExerciseCommand}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.value.{Identifier, Record, RecordField, Value}
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{CommunityIntegrationTest, ConfigTransforms, EnvironmentDefinition, SharedEnvironment, TestConsoleEnvironment}
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, VettedPackage}
import com.digitalasset.canton.topology.{Party, PartyId}
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.ContractIdVersion
import com.digitalasset.daml.lf.value.Value.ContractId
import monocle.macros.syntax.lens.*
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{FileSystems, Files, Path}

// Integration tests need to live in the package com.digitalasset.canton.integration.tests, so we
// make the test base an abstract class
abstract class ReplayBenchmarkITBase(
    contractIdVersion: ContractIdVersion,
) extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with BeforeAndAfterEach {

  val participantId = Ref.ParticipantId.assertFromString("participant1")
  val snapshotDir = Files.createTempDirectory("ReplayBenchmarkTest")
  val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")
  val darFile = "ReplayBenchmark.dar"
  val darPath: Path =
    Option(getClass.getClassLoader.getResource(darFile))
      .map(path => Path.of(path.getPath))
      .getOrElse(throw new IllegalArgumentException(s"Cannot find resource $darFile"))
  val ReplayBenchmarkPkgId: LfPackageId = getPkgId(darPath)

  private var alice: PartyId = _

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

      // Allocate parties
      PartiesAllocator(Set(participant1))(
        newParties = Seq("alice" -> participant1),
        targetTopology = Map(
          "alice" -> Map(
            daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission))
          ),
        ),
      )
      alice = "alice".toPartyId(participant1)

      // Upload the test DAR and vet its packages
      SetupPackageVetting(
        Set(darPath.toFile.getAbsolutePath),
        targetTopology = Map(
          daId -> participants.all
            .map(
              _ -> VettedPackage
                .unbounded(
                  Seq(ReplayBenchmarkPkgId)
                )
                .toSet
            )
            .toMap
        ),
      )
    }

  override def afterEach(): Unit = {
    Files.newDirectoryStream(snapshotDir).forEach(Files.delete)
  }

  "Ledger submission" should {
    "with a create" should {
      "generate a snapshot file" in { implicit env =>
        import env.*

        participant1.ledger_api.commands
          .submit(Seq(alice), createT(alice, 13))

        val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
        snapshotFiles.size() should be(1)

        val snapshotFile = snapshotFiles.get(0)
        Files.exists(snapshotFile) should be(true)
        Files.size(snapshotFile) should be > 0L

        // Replay and attempt to validate the snapshot file
        val benchmark = new ReplayBenchmark
        benchmark.darFile = darPath.toFile.getAbsolutePath
        benchmark.choiceName = "ReplayBenchmark:T:Add"
        benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath
        benchmark.contractIdVersion = contractIdVersion.toString

        val exn = intercept[RuntimeException] {
          benchmark.init()
        }
        exn.getMessage should be("choice ReplayBenchmark:T:Add not found")
      }
    }

    "with a create and exercise" should {
      "generate a replayable snapshot file when contract is global" in { implicit env =>
        import env.*

        val cid = getContractId(participant1.ledger_api.commands
          .submit(Seq(alice), createT(alice, 13)))
        participant1.ledger_api.commands
          .submit(Seq(alice), exerciseAdd(cid, 7))

        val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
        snapshotFiles.size() should be(1)

        val snapshotFile = snapshotFiles.get(0)
        Files.exists(snapshotFile) should be(true)
        Files.size(snapshotFile) should be > 0L

        // Replay and validate the snapshot file
        val benchmark = new ReplayBenchmark
        benchmark.darFile = darPath.toFile.getAbsolutePath
        benchmark.choiceName = "ReplayBenchmark:T:Add"
        benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath
        benchmark.contractIdVersion = contractIdVersion.toString

        noException should be thrownBy benchmark.init()
      }

      "generate a replayable snapshot file when contract is local" in { implicit env =>
        import env.*

        participant1.ledger_api.commands
          .submit(Seq(alice), createAndExerciseAdd(alice, 42, 7))

        val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
        snapshotFiles.size() should be(1)

        val snapshotFile = snapshotFiles.get(0)
        Files.exists(snapshotFile) should be(true)
        Files.size(snapshotFile) should be > 0L

        // Replay and validate the snapshot file
        val benchmark = new ReplayBenchmark
        benchmark.darFile = darPath.toFile.getAbsolutePath
        benchmark.choiceName = "ReplayBenchmark:T:Add"
        benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath
        benchmark.contractIdVersion = contractIdVersion.toString

        noException should be thrownBy benchmark.init()
      }
    }

    "with Daml exceptions" should {
      "not generate a snapshot file" in { implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.commands
            .submit(Seq(alice), createAndExerciseExplode(alice, 7)),
          _.commandFailureMessage should include("UNHANDLED_EXCEPTION/DA.Exception.AssertionFailed:AssertionFailed")
        )

        val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
        snapshotFiles.size() should be(0)
      }

      "not update or corrupt a snapshot file" in { implicit env =>
        import env.*

        participant1.ledger_api.commands
          .submit(Seq(alice), createAndExerciseAdd(alice, 42, 7))
        assertThrowsAndLogsCommandFailures(
          participant1.ledger_api.commands
            .submit(Seq(alice), createAndExerciseExplode(alice, 17)),
          _.commandFailureMessage should include("UNHANDLED_EXCEPTION/DA.Exception.AssertionFailed:AssertionFailed")
        )

        val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
        snapshotFiles.size() should be(1)

        val snapshotFile = snapshotFiles.get(0)
        Files.exists(snapshotFile) should be(true)
        Files.size(snapshotFile) should be > 0L

        // Replay and validate the snapshot file
        val benchmark = new ReplayBenchmark
        benchmark.darFile = darPath.toFile.getAbsolutePath
        benchmark.choiceName = "ReplayBenchmark:T:Add"
        benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath
        benchmark.contractIdVersion = contractIdVersion.toString

        noException should be thrownBy benchmark.init()
      }
    }
  }

  private def getPkgId(darPath: Path): LfPackageId =
    DarDecoder.assertReadArchiveFromFile(darPath.toFile).main._1

  private def getContractId(tx: Transaction): ContractId =
    ContractId.assertFromString(tx.events.map(_.getCreated.contractId).loneElement)

  private def createT(party: Party, value: Int)(implicit env: TestConsoleEnvironment): Seq[Command] = {
    import env.*

    Seq(
      ledger_api_utils.create(
        ReplayBenchmarkPkgId,
        "ReplayBenchmark",
        "T",
        Map("p" -> party.partyId, "v" -> value),
      )
    )
  }

  private def exerciseAdd(
    cid: ContractId,
    value: Int,
  )(implicit
    env: TestConsoleEnvironment
  ): Seq[Command] = {
    import env.*

    Seq(
      ledger_api_utils.exercise(
        ReplayBenchmarkPkgId,
        "ReplayBenchmark",
        "T",
        "Add",
        Map("n" -> value),
        cid.coid,
      )
    )
  }

  private def createAndExerciseAdd(party: Party, value1: Int, value2: Int): Seq[Command] = {
    val createArgs = Record(None, Seq(RecordField("p", Some(Value(Value.Sum.Party(party.toLf)))), RecordField("v", Some(Value(Value.Sum.Int64(value1.toLong))))))
    val choiceArg = Value(Value.Sum.Record(Record(None, Seq(RecordField("n", Some(Value(Value.Sum.Int64(value2.toLong))))))))

    Seq(
      Command(Command.Command.CreateAndExercise(CreateAndExerciseCommand(
        templateId = Some(Identifier(ReplayBenchmarkPkgId, "ReplayBenchmark", "T")),
        createArguments = Some(createArgs),
        choice = "Add",
        choiceArgument = Some(choiceArg),
      )))
    )
  }
  private def createAndExerciseExplode(party: Party, value: Int): Seq[Command] = {
    val createArgs = Record(None, Seq(RecordField("p", Some(Value(Value.Sum.Party(party.toLf)))), RecordField("v", Some(Value(Value.Sum.Int64(value.toLong))))))
    val choiceArg = Value(Value.Sum.Record(Record(None, Seq.empty)))

    Seq(
      Command(Command.Command.CreateAndExercise(CreateAndExerciseCommand(
        templateId = Some(Identifier(ReplayBenchmarkPkgId, "ReplayBenchmark", "T")),
        createArguments = Some(createArgs),
        choice = "Explode",
        choiceArgument = Some(choiceArg),
      )))
    )
  }
}
