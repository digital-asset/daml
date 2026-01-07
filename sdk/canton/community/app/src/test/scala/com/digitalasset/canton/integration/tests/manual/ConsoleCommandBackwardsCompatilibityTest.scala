// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.console.FeatureFlag.{Repair, Stable}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleMacros,
  Help,
  LocalInstancesExtensions,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
  ParticipantReferencesExtensions,
  RemoteMediatorReference,
  RemoteParticipantReference,
  RemoteSequencerReference,
}
import com.digitalasset.canton.integration.tests.manual.ConsoleCommandBackwardsCompatibilityReaderTest.checkForBreakingChanges
import com.digitalasset.canton.integration.tests.manual.ConsoleCommandBackwardsCompatibilityTestBase.*
import com.digitalasset.canton.version.ReleaseVersion
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.{FileInput, Formats, NoTypeHints}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Inside, LoneElement}

import java.nio.file.Files

/** This test checks console commands for breaking changes compared to the latest patch releases of
  * the current and previous minor release.
  */
sealed trait ConsoleCommandBackwardsCompatibilityTestBase
    extends AnyWordSpec
    with Matchers
    with Inside
    with LoneElement
    with S3Synchronization {

  protected def generateCommandMetadata(): Seq[NamedConsoleCommandSignature] = {
    val localParticipantCommands = Help
      .getItemsForClass[LocalParticipantReference](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("local-participant")))
    val remoteParticipantCommands = Help
      .getItemsForClass[RemoteParticipantReference](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("remote-participant")))
    val participantExtensionCommands = Help
      .getItemsForClass[ParticipantReferencesExtensions](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("participant-extensions")))

    val localMediatorCommands = Help
      .getItemsForClass[LocalMediatorReference](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("local-mediator")))
    val remoteMediatorCommands = Help
      .getItemsForClass[RemoteMediatorReference](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("remote-mediator")))

    val localSequencerCommands = Help
      .getItemsForClass[LocalSequencerReference](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("local-sequencer")))
    val remoteSequencerCommands = Help
      .getItemsForClass[RemoteSequencerReference](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("remote-sequencer")))

    val consoleMacrosCommands = Help
      .getItemsForClass[ConsoleMacros.type](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("console-macros")))

    val consoleEnvironmentCommands = Help
      .getItemsForClass[ConsoleEnvironment](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("console-environment")))

    val localInstancesExtensionsCommands = Help
      .getItemsForClass[LocalInstancesExtensions.Impl[?]](scope = Set(Repair, Stable))
      .flatMap(Help.flattenItem(Seq("local-instances-extensions")))

    val allCommands =
      localParticipantCommands ++ remoteParticipantCommands ++ participantExtensionCommands ++
        localMediatorCommands ++ remoteMediatorCommands ++
        localSequencerCommands ++ remoteSequencerCommands ++
        consoleMacrosCommands ++ consoleEnvironmentCommands ++
        localInstancesExtensionsCommands

    allCommands
      .flatMap(i =>
        i.signature.map(sig => NamedConsoleCommandSignature(i.name, sig.argsWithTypes, sig.retType))
      )
      .sortBy(_.name)
  }

  protected val metadataFileName = s"console-commands.json"
  protected implicit val formats: Formats = Serialization.formats(NoTypeHints)

}

object ConsoleCommandBackwardsCompatibilityTestBase {
  final case class NamedConsoleCommandSignature(
      name: String,
      argsWithTypes: Seq[Help.Parameter],
      retType: String,
  )

  sealed trait BreakingChange extends Product with Serializable {
    def commandName: String
  }

  final case class ParameterRemovedOrNameChanged(
      commandName: String,
      originalParameterName: String,
  ) extends BreakingChange

  final case class ParameterTypeChanged(
      commandName: String,
      parameterName: String,
      originalType: String,
      currentType: String,
  ) extends BreakingChange

  // Parameter added without default value
  final case class ParameterAdded(
      commandName: String,
      parameterName: String,
  ) extends BreakingChange

  final case class ReturnTypeChanged(
      commandName: String,
      originalReturnType: String,
      currentReturnType: String,
  ) extends BreakingChange

  final case class CommandRemoved(commandName: String) extends BreakingChange
}

final class ConsoleCommandBackwardsCompatibilityWriterTest
    extends ConsoleCommandBackwardsCompatibilityTestBase {
  "dump current metadata" in {
    // the metadata will then be uploaded automatically during a release, via publish-data-continuity-dumps-to-s3.sh
    val dumpDirectoryForCurrentVersion =
      DataContinuityTest.baseDbDumpPath / ReleaseVersion.current.fullVersion
    Files.createDirectories(dumpDirectoryForCurrentVersion.path)
    val dumpFileForCurrentVersion = dumpDirectoryForCurrentVersion / metadataFileName

    val currentCommands = generateCommandMetadata()
    val json = write(currentCommands)
    val file = better.files.File(dumpFileForCurrentVersion.path)
    file.write(json)
  }
}

final class ConsoleCommandBackwardsCompatibilityReaderTest
    extends ConsoleCommandBackwardsCompatibilityTestBase {

  "console commands" should {

    // Test against previous patch versions of the current and the previous minor version
    val versionsToTest =
      Option(ReleaseVersion.current)
        .filter(_.patch > 0)
        .map(_.majorMinor)
        .toList :+
        (ReleaseVersion.current.major, ReleaseVersion.current.minor - 1)

    versionsToTest.foreach { case majorMinor @ (major, minor) =>
      // check the backwards compatibility against the version of
      s"be backwards compatible with commands in latest patch release of $major.$minor" in {

        // load the console command metadata for the latest release version of the given majorMinor
        def metadataForVersion(): Option[(ReleaseVersion, Seq[NamedConsoleCommandSignature])] =
          S3Dump
            .getDumpBaseDirectoriesForVersion(majorUpgradeTestFrom = Some(majorMinor))
            .maxByOption { case (_, version) => version }
            .map { case (dumpRef, rv) =>
              val file = better.files.File(dumpRef.localDownloadPath, metadataFileName)
              val metadata = read[Seq[NamedConsoleCommandSignature]](FileInput(file.toJava))
              rv -> metadata
            }

        val (versionOfDump, origCmds) = metadataForVersion().valueOrFail(
          s"Unable to load console command metadata for $majorMinor"
        )

        val currentCommands = generateCommandMetadata()
        val allBreakingChanges = for {
          origCmd <- origCmds
          error <- checkForBreakingChanges(versionOfDump, currentCommands, origCmd)
        } yield error

        if (allBreakingChanges.nonEmpty) {
          val errorReportByCmd =
            allBreakingChanges.groupBy(_.commandName).toSeq.sortBy { case (name, _) => name }.map {
              case (cmdName, breakingChanges) =>
                s"""$cmdName:
                   |${breakingChanges.mkString("\t", "\n\t", "\n")}""".stripMargin
            }
          logger.error(s"""Detected breaking changes since $versionOfDump:
                          |${errorReportByCmd.mkString("\n")} """.stripMargin)
        }
        allBreakingChanges shouldBe empty
      }
    }
  }
}

object ConsoleCommandBackwardsCompatibilityReaderTest {
  // All admissible changes are original -> new

  // Changes that are allowed for all versions
  private val permissibleParameterTypeChanges: Set[(String, String)] = {
    val typeChanges = Set(
      ("com.digitalasset.canton.topology.PartyId", "com.digitalasset.canton.topology.Party"),

      // automatic conversions added
      ("com.digitalasset.canton.config.RequireTypes.NonNegativeLong", "Long"),

      // Removing usage of internal classes around sequencer connections
      (
        "com.digitalasset.canton.sequencing.SubmissionRequestAmplification",
        "com.digitalasset.canton.admin.api.client.data.SubmissionRequestAmplification",
      ),
    )

    // For each type, also allow for collections and wrappers
    typeChanges.flatMap { case (originalType, newType) =>
      Seq(
        (originalType, newType),
        (s"Seq[$originalType]", s"Seq[$newType]"),
        (s"List[$originalType]", s"List[$newType]"),
        (s"Set[$originalType]", s"Set[$newType]"),
        (s"Option[$originalType]", s"Option[$newType]"),
      )
    }
  }

  // Changes in return type that are allowed per version (discussed and agreed upon)
  private val permissibleReturnChanges: Map[(Int, Int, Int), Set[ReturnTypeChanged]] = Map(
    (3, 4, 10) ->
      (Set(
        // WIP feature work
        (
          "local-participant.parties.clear_party_onboarding_flag",
          "(Boolean, Option[com.digitalasset.canton.data.CantonTimestamp])",
          "com.digitalasset.canton.admin.api.client.data.PartyOnboardingFlagStatus",
        ),
        (
          "remote-participant.parties.clear_party_onboarding_flag",
          "(Boolean, Option[com.digitalasset.canton.data.CantonTimestamp])",
          "com.digitalasset.canton.admin.api.client.data.PartyOnboardingFlagStatus",
        ),
      ) ++
        // Removal of contract id recomputation (broken, incompatible with streaming endpoint). Agreed with CN
        Set(
          "remote-participant.repair.add",
          "local-participant.repair.add",
          "remote-participant.repair.import_acs_old",
          "local-participant.repair.import_acs_old",
          "remote-participant.repair.import_acs",
          "local-participant.repair.import_acs",
        ).map(
          (
            _,
            "Map[com.digitalasset.canton.protocol.LfContractId,com.digitalasset.canton.protocol.LfContractId]",
            "Unit",
          )
        ))
  ).fmap(_.map((ReturnTypeChanged.apply _).tupled))

  // Changes in parameters that are allowed per version (discussed and agreed upon)
  private val permissibleParameterRemovedOrNameChanged
      : Map[(Int, Int, Int), Set[ParameterRemovedOrNameChanged]] = Map(
    (3, 4, 10) ->
      (
        // Removal of contract id recomputation (broken, incompatible with streaming endpoint). Agreed with CN
        Set(
          "remote-participant.repair.add",
          "local-participant.repair.add",
          "remote-participant.repair.import_acs_old",
          "local-participant.repair.import_acs_old",
          "remote-participant.repair.import_acs",
          "local-participant.repair.import_acs",
        ).map((_, "allowContractIdSuffixRecomputation"))
      )
  ).fmap(_.map((ParameterRemovedOrNameChanged.apply _).tupled))

  private val permissibleReturnTypeChanges: Set[(String, String)] =
    Set(
      // automatic conversions added
      ("com.digitalasset.canton.config.RequireTypes.NonNegativeLong", "Long")
    )

  private val allowRemovalOfCommands: Set[ /* commandName: */ String] = Set(
    // the @Help.Summary annotation used to be on the private val instead of the public def
    "console-environment.global_secret_",
    "console-environment.global_secret_.get_signing_key",
    "console-environment.global_secret_.help",
    "console-environment.global_secret_.sign",
    "console-environment.global_secret_.sign",
    "console-environment.global_secret_.sign",
    "console-environment.health_",
    "console-environment.health_.dump",
    "console-environment.health_.help",
    "console-environment.health_.status",
  )

  /** Checks that `origCmd` has a corresponding command in `currentCommands` without breaking
    * changes. A breaking change is one of the following:
    *   - the command was removed
    *   - the return type of the command has changed
    *   - the type of a parameter has changed
    *   - a parameter was renamed or removed (we cannot really detect the difference)
    *
    * Breaking changes can be allowed by adding exceptions to `allowRemovalOfCommands` and
    * `permissibleTypeChanges`. The latter applies to both the return type and the type of
    * parameters.
    *
    * Since some commands have overloaded methods, we compare the original command from a previous
    * version with all commands in the current version. If one of the current overloads is
    * compatible without breaking changes (after applying the exceptions), the breaking changes
    * reported from comparing the original command to other overloads are discarded.
    *
    * @return
    *   a list of breaking changes or an empty list if not breaking changes were detected.
    */
  def checkForBreakingChanges(
      sourceReleaseVersion: ReleaseVersion,
      currentCommands: Seq[NamedConsoleCommandSignature],
      origCmd: NamedConsoleCommandSignature,
  ): Seq[BreakingChange] = {
    val admissibleReturnChanges = permissibleReturnChanges.getOrElse(
      (sourceReleaseVersion.major, sourceReleaseVersion.minor, sourceReleaseVersion.patch),
      Set.empty,
    )

    val admissibleParameterRemovedOrNameChanged =
      permissibleParameterRemovedOrNameChanged.getOrElse(
        (sourceReleaseVersion.major, sourceReleaseVersion.minor, sourceReleaseVersion.patch),
        Set.empty,
      )

    val allAdmissibleChanges: Set[BreakingChange] =
      admissibleReturnChanges ++ admissibleParameterRemovedOrNameChanged

    val errorsPerCurrentCommand: Seq[List[BreakingChange]] = currentCommands
      // some commands/methods are overloaded, therefore we compare the original command with all
      // current commands with the same name.
      .filter(_.name == origCmd.name)
      .map { currCmd =>
        val currArgs: Map[String, Help.Parameter] =
          currCmd.argsWithTypes.map(param => param.name -> param).toMap
        val origArgs: Map[String, Help.Parameter] =
          origCmd.argsWithTypes.map(param => param.name -> param).toMap

        // return type
        val returnTypeError = Option
          .when(
            currCmd.retType != origCmd.retType && !permissibleReturnTypeChanges
              .contains(origCmd.retType -> currCmd.retType)
          )(ReturnTypeChanged(origCmd.name, origCmd.retType, currCmd.retType))
          .toList

        // parameters: removed, name changed, type changed
        val parameterErrors = origCmd.argsWithTypes.flatMap(orig =>
          currArgs.get(orig.name) match {
            case None => Seq(ParameterRemovedOrNameChanged(origCmd.name, orig.name))
            case Some(curr) =>
              Option
                .when(
                  orig.tpe != curr.tpe && !permissibleParameterTypeChanges
                    .contains(orig.tpe -> curr.tpe)
                )(ParameterTypeChanged(origCmd.name, orig.name, orig.tpe, curr.tpe))
                .toList
          }
        )

        // parameters: added without default value
        val parameterAddedErrors = currCmd.argsWithTypes.mapFilter { currArg =>
          Option.when(!currArg.hasDefaultParameter && !origArgs.isDefinedAt(currArg.name))(
            ParameterAdded(origCmd.name, currArg.name)
          )
        }

        (returnTypeError ++ parameterErrors ++ parameterAddedErrors).filterNot(
          allAdmissibleChanges.contains
        )
      }

    // find the comparison between the old command and a current command with the fewest errors.
    // backwards compatibility is not broken, if there was a comparison with no errors (i.e. an empty sequence)
    errorsPerCurrentCommand
      .minByOption(_.size)
      // if there's no result at all, it means that the command was removed in the current version.
      // first check whether we explicitly allow the removal
      .orElse(Option.when(allowRemovalOfCommands(origCmd.name))(Seq.empty))
      // if the removal is not allowed, report a breaking change
      .getOrElse(Seq(CommandRemoved(origCmd.name)))
  }
}
