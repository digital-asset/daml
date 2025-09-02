// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json

import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.value.Value
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.http.json.v2.{JsCommand, TranscodePackageIdResolver}
import com.digitalasset.canton.ledger.api.PackageReference
import com.digitalasset.canton.ledger.api.validation.GetPreferredPackagesRequestValidator.PackageVettingRequirements
import com.digitalasset.canton.ledger.error.JsonApiErrors.JsonApiPackageSelectionFailed
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.{
  InvalidArgument,
  InvalidField,
  MissingField,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.platform.PackagePreferenceBackend.PackageFilterRestriction
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  LocalPackagePreference,
  PackageResolution,
}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorService,
  LfPackageId,
  LfPackageName,
  LfPackageVersion,
  LfPartyId,
}
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.{Failure, Success, Try}

import TranscodePackageIdResolverTest.*

class TranscodePackageIdResolverTest extends AsyncWordSpec with BaseTest with HasExecutorService {
  "Topology-state backed resolver" when {
    testCases(TopologyStateBacked)
  }

  "Package metadata backed resolver" when {
    testCases(PackageMetadataBacked)
  }

  private def testCases(backend: TargetBackendType): Unit =
    test(backend)(
      TestDef(
        targetBackend = TopologyStateBacked,
        description =
          "output transcoding package-ids for input commands of all types (topology-state backed)",
        arrange = packagePreferenceBackend => {
          when(
            packagePreferenceBackend.getPreferredPackages(
              packageVettingRequirements = PackageVettingRequirements(
                Map(
                  createPkgRef.packageName -> Set(submittingParty),
                  exercisePkgRef.packageName -> Set(submittingParty),
                  createAndExercisePkgRef.packageName -> Set(submittingParty),
                  exerciseByKeyPkgRef.packageName -> Set(submittingParty),
                )
              ),
              packageFilter = PackageFilterRestriction(
                Map.empty,
                "Commands.packageIdSelectionPreferences",
              ),
              synchronizerId = None,
              vettingValidAt = None,
            )
          ).thenReturn(
            FutureUnlessShutdown.pure(
              Right(
                Seq(
                  createPkgRef,
                  exercisePkgRef,
                  createAndExercisePkgRef,
                  exerciseByKeyPkgRef,
                ) -> psId_1
              )
            )
          )
        },
        commands = Seq(
          SimpleCreateCommand(createTplId),
          SimpleExerciseCommand(exerciseTplId),
          SimpleCreateAndExerciseCommand(createAndExerciseTplId),
          SimpleExerciseByKeyCommand(exerciseByKeyTplId),
        ),
        assert = Right(
          Seq(
            createPkgRef.pkgId,
            exercisePkgRef.pkgId,
            createAndExercisePkgRef.pkgId,
            exerciseByKeyPkgRef.pkgId,
          )
        ),
      ),
      TestDef(
        targetBackend = PackageMetadataBacked,
        description =
          "output transcoding package-ids for input commands of all types (package-metadata backed)",
        commands = Seq(
          SimpleCreateCommand(createTplId),
          SimpleExerciseCommand(exerciseTplId),
          SimpleCreateAndExerciseCommand(createAndExerciseTplId),
          SimpleExerciseByKeyCommand(exerciseByKeyTplId),
        ),
        assert = Right(
          Seq(
            upgradeCreatePkgRef.pkgId,
            exercisePkgRef.pkgId,
            createAndExercisePkgRef.pkgId,
            exerciseByKeyPkgRef.pkgId,
          )
        ),
      ),
      TestDef(
        description = "only resolve package-ids for commands with package-name references",
        arrange = packagePreferenceBackend => {
          when(
            packagePreferenceBackend.getPreferredPackages(
              packageVettingRequirements = PackageVettingRequirements(
                Map(createPkgRef.packageName -> Set(submittingParty))
              ),
              packageFilter = PackageFilterRestriction(
                Map.empty,
                "Commands.packageIdSelectionPreferences",
              ),
              synchronizerId = None,
              vettingValidAt = None,
            )
          ).thenReturn(
            FutureUnlessShutdown.pure(Right(Seq(upgradeCreatePkgRef) -> psId_1))
          )
        },
        commands = Seq(
          // Only the create command uses the package-name reference format
          SimpleCreateCommand(createTplId),
          SimpleExerciseCommand(exerciseTplId.copy(packageId = exercisePkgRef.pkgId)),
          SimpleCreateAndExerciseCommand(
            createAndExerciseTplId.copy(packageId = createAndExercisePkgRef.pkgId)
          ),
          SimpleExerciseByKeyCommand(exerciseByKeyTplId.copy(packageId = exerciseByKeyPkgRef.pkgId)),
        ),
        assert = Right(
          Seq(
            upgradeCreatePkgRef.pkgId,
            exercisePkgRef.pkgId,
            createAndExercisePkgRef.pkgId,
            exerciseByKeyPkgRef.pkgId,
          )
        ),
      ),
      TestDef(
        description = "consider Commands.packageIdSelectionPreferences",
        arrange = packagePreferenceBackend => {
          when(
            packagePreferenceBackend.getPreferredPackages(
              packageVettingRequirements = PackageVettingRequirements(
                Map(createPkgRef.packageName -> Set(submittingParty))
              ),
              packageFilter = PackageFilterRestriction(
                Map(createPkgRef.packageName -> Set(upgradeCreatePkgRef.pkgId)),
                "Commands.packageIdSelectionPreferences",
              ),
              synchronizerId = None,
              vettingValidAt = None,
            )
          ).thenReturn(FutureUnlessShutdown.pure(Right(Seq(upgradeCreatePkgRef) -> psId_1)))
        },
        commands =
          Seq(SimpleCreateCommand(createTplId.copy(packageId = upgradeCreatePkgRef.pkgId))),
        actAs = List(submittingParty),
        packageIdSelectionPreferences = Seq(upgradeCreatePkgRef.pkgId),
        synchronizerIdO = None,
        assert = Right(Seq(upgradeCreatePkgRef.pkgId)),
      ),
      TestDef(
        // Package metadata backed resolver does not care about synchronizerId
        targetBackend = TopologyStateBacked,
        description = "consider synchronizerId",
        arrange = packagePreferenceBackend => {
          when(
            packagePreferenceBackend.getPreferredPackages(
              packageVettingRequirements = PackageVettingRequirements(
                Map(createPkgRef.packageName -> Set(submittingParty))
              ),
              packageFilter = PackageFilterRestriction(
                Map(createPkgRef.packageName -> Set(upgradeCreatePkgRef.pkgId)),
                "Commands.packageIdSelectionPreferences",
              ),
              synchronizerId = Some(psId_2.logical),
              vettingValidAt = None,
            )
          ).thenReturn(FutureUnlessShutdown.pure(Right(Seq(createPkgRef) -> psId_2)))
        },
        commands = Seq(SimpleCreateCommand(createTplId)),
        packageIdSelectionPreferences = Seq(upgradeCreatePkgRef.pkgId),
        synchronizerIdO = Some(psId_2.logical.toProtoPrimitive),
        assert = Right(Seq(createPkgRef.pkgId)),
      ),
      // Negative test cases
      TestDef(
        description = "fail on unknown package-id in Commands.packageIdSelectionPreferences",
        packageIdSelectionPreferences = Seq("unknown"),
        assert = Left(JsonApiPackageSelectionFailed.id -> "Package-id unknown not known"),
      ),
      TestDef(
        description = "fail on invalid package-id in Commands.packageIdSelectionPreferences",
        packageIdSelectionPreferences = Seq("invalid#pkg"),
        assert = Left(InvalidArgument.id -> "non expected character"),
      ),
      TestDef(
        description = "fail on empty actAs",
        actAs = Seq.empty,
        assert = Left(
          MissingField.id -> "The submitted command is missing a mandatory field: act_as/actAs"
        ),
      ),
      TestDef(
        description = "fail on invalid submitter party",
        actAs = Seq("invalid#party"),
        assert = Left(
          InvalidArgument.id -> "Invalid party in actAs/act_as: non expected character"
        ),
      ),
      TestDef(
        description = "fail on invalid package references in a command",
        commands = Seq(SimpleCreateCommand(createTplId.copy(packageId = "!#weird"))),
        assert = Left(
          InvalidArgument.id -> "Value does not match the package-id or package-name formats"
        ),
      ),
      TestDef(
        // Synchronizer-id is not relevant for package-metadata backed resolver
        targetBackend = TopologyStateBacked,
        description = "fail on invalid synchronizerId",
        synchronizerIdO = Some("invalidsync"),
        assert = Left(
          InvalidField.id -> "The submitted command has a field with invalid value: Invalid field synchronizer_id: Invalid unique identifier `invalidsync` with missing namespace"
        ),
      ),
    )

  case class TestDef(
      description: String,
      arrange: PackagePreferenceBackend => Unit = _ => (),
      commands: Seq[SimpleAbstractCommand] = Seq(SimpleCreateCommand(createTplId)),
      actAs: Seq[String] = Seq(submittingParty),
      packageIdSelectionPreferences: Seq[String] = Nil,
      synchronizerIdO: Option[String] = None,
      assert: Either[(String /* error code */, String /* error message */ ), Seq[
        LfPackageId
      ]],
      packageMetadata: PackageMetadata = packageMetadata,
      targetBackend: TargetBackendType = BothBackends,
  )

  def test(backend: TargetBackendType)(
      testCases: TestDef*
  ): Unit =
    (GrpcFrontend :: JsonFrontend :: Nil).foreach { targetFrontend =>
      s"resolving $targetFrontend commands" should {
        testCases.foreach {
          case TestDef(
                description,
                arrange,
                commands,
                actAs,
                packageIdSelectionPreferences,
                synchronizerIdO,
                assert,
                packageMetadata,
                targetBackend,
              ) =>
            if (targetBackend == BothBackends || targetBackend == backend) {
              s"$description" in {
                val packagePreferenceBackend = mock[PackagePreferenceBackend]
                arrange(packagePreferenceBackend)
                val transcodePackageIdResolver =
                  if (backend == PackageMetadataBacked)
                    TranscodePackageIdResolver.packageMetadataBacked(
                      fetchPackageMetadataSnapshot = () => packageMetadata,
                      namedLoggerFactory = loggerFactory,
                    )
                  else
                    TranscodePackageIdResolver.topologyStateBacked(
                      packagePreferenceBackend = packagePreferenceBackend,
                      fetchPackageMetadataSnapshot = _ => packageMetadata,
                      loggerFactory = loggerFactory,
                    )

                targetFrontend match {
                  case GrpcFrontend =>
                    val grpcCommands = commands.map(_.toGrpc)
                    transcodePackageIdResolver
                      .resolveDecodingPackageIdsForGrpcCommands(
                        grpcCommands = grpcCommands,
                        actAs = actAs,
                        packageIdSelectionPreference = packageIdSelectionPreferences,
                        synchronizerIdO = synchronizerIdO,
                      )
                      .transform {
                        case Failure(DecodedCantonError(decodedCantonError)) =>
                          val (expectedErrorCode, expectedErrorContent) = assert.leftOrFail(
                            s"expecting $assert for $description but got an error $decodedCantonError"
                          )

                          Try {
                            decodedCantonError.code.id shouldBe expectedErrorCode
                            decodedCantonError.cause should include(expectedErrorContent)
                          }
                        case Success(commandsAndTheirDecodingPackages) =>
                          val expectedDecodingPackageIds = assert.valueOrFail(
                            s"expecting $assert for $description but got a success $commandsAndTheirDecodingPackages"
                          )
                          Try(
                            commandsAndTheirDecodingPackages should contain theSameElementsInOrderAs grpcCommands
                              .zip(expectedDecodingPackageIds)
                          )
                        case Failure(err) => Failure(err)
                      }
                  case JsonFrontend =>
                    val jsonCommands = commands.map(_.toJson)
                    transcodePackageIdResolver
                      .resolveDecodingPackageIdsForJsonCommands(
                        jsCommands = jsonCommands,
                        actAs = actAs,
                        packageIdSelectionPreference = packageIdSelectionPreferences,
                        synchronizerIdO = synchronizerIdO,
                      )
                      .transform {
                        case Failure(DecodedCantonError(decodedCantonError)) =>
                          val (expectedErrorCode, expectedErrorContent) = assert.leftOrFail(
                            s"expecting $assert for $description but got an error $decodedCantonError"
                          )

                          Try {
                            decodedCantonError.code.id shouldBe expectedErrorCode
                            decodedCantonError.cause should include(expectedErrorContent)
                          }
                        case Success(commandsAndTheirDecodingPackages) =>
                          val expectedDecodingPackageIds = assert.valueOrFail(
                            s"expecting $assert for $description but got a success $commandsAndTheirDecodingPackages"
                          )
                          Try(
                            commandsAndTheirDecodingPackages should contain theSameElementsInOrderAs jsonCommands
                              .zip(expectedDecodingPackageIds)
                          )
                        case Failure(err) => Failure(err)
                      }
                }
              }
            } else ()
        }
      }
    }
}

object TranscodePackageIdResolverTest {
  private val submittingParty = LfPartyId.assertFromString("submittingParty")
  private val createPkgRef = PackageReference(
    pkgId = LfPackageId.assertFromString("createPkgId"),
    version = LfPackageVersion.assertFromString("0.0.1"),
    packageName = LfPackageName.assertFromString("pkgNameCreate"),
  )
  private val upgradeCreatePkgRef = PackageReference(
    pkgId = LfPackageId.assertFromString("upgradedCreatePkgId"),
    version = LfPackageVersion.assertFromString("0.0.2"),
    packageName = LfPackageName.assertFromString("pkgNameCreate"),
  )
  private val exercisePkgRef = PackageReference(
    pkgId = LfPackageId.assertFromString("exePkgId"),
    version = LfPackageVersion.assertFromString("0.0.1"),
    packageName = LfPackageName.assertFromString("pkgNameExe"),
  )
  private val createAndExercisePkgRef = PackageReference(
    pkgId = LfPackageId.assertFromString("createAndExePkgId"),
    version = LfPackageVersion.assertFromString("0.0.1"),
    packageName = LfPackageName.assertFromString("pkgNameCreateAndExe"),
  )
  private val exerciseByKeyPkgRef = PackageReference(
    pkgId = LfPackageId.assertFromString("exeByKeyPkgId"),
    version = LfPackageVersion.assertFromString("0.0.1"),
    packageName = LfPackageName.assertFromString("pkgNameExeByKey"),
  )
  private val createTplId = lapi.value.Identifier(
    packageId = s"#${createPkgRef.packageName}",
    moduleName = "SomeModule",
    entityName = "SomeEntity",
  )
  private val exerciseTplId = lapi.value.Identifier(
    packageId = s"#${exercisePkgRef.packageName}",
    moduleName = "SomeModule",
    entityName = "SomeEntity",
  )
  private val createAndExerciseTplId = lapi.value.Identifier(
    packageId = s"#${createAndExercisePkgRef.packageName}",
    moduleName = "SomeModule",
    entityName = "SomeEntity",
  )
  private val exerciseByKeyTplId = lapi.value.Identifier(
    packageId = s"#${exerciseByKeyPkgRef.packageName}",
    moduleName = "SomeModule",
    entityName = "SomeEntity",
  )
  private val packageMetadata = PackageMetadata(
    packageIdVersionMap = Map(
      createPkgRef.pkgId -> (createPkgRef.packageName, createPkgRef.version),
      upgradeCreatePkgRef.pkgId -> (upgradeCreatePkgRef.packageName, upgradeCreatePkgRef.version),
      exercisePkgRef.pkgId -> (exercisePkgRef.packageName, exercisePkgRef.version),
      createAndExercisePkgRef.pkgId -> (createAndExercisePkgRef.packageName, createAndExercisePkgRef.version),
      exerciseByKeyPkgRef.pkgId -> (exerciseByKeyPkgRef.packageName, exerciseByKeyPkgRef.version),
    ),
    packageNameMap = Map(
      createPkgRef.packageName ->
        PackageResolution(
          LocalPackagePreference(upgradeCreatePkgRef.version, upgradeCreatePkgRef.pkgId),
          NonEmpty(Set, createPkgRef.pkgId, upgradeCreatePkgRef.pkgId),
        ),
      exercisePkgRef.packageName ->
        PackageResolution(
          LocalPackagePreference(exercisePkgRef.version, exercisePkgRef.pkgId),
          NonEmpty(Set, exercisePkgRef.pkgId),
        ),
      createAndExercisePkgRef.packageName ->
        PackageResolution(
          LocalPackagePreference(createAndExercisePkgRef.version, createAndExercisePkgRef.pkgId),
          NonEmpty(Set, createAndExercisePkgRef.pkgId),
        ),
      exerciseByKeyPkgRef.packageName ->
        PackageResolution(
          LocalPackagePreference(exerciseByKeyPkgRef.version, exerciseByKeyPkgRef.pkgId),
          NonEmpty(Set, exerciseByKeyPkgRef.pkgId),
        ),
    ),
  )
  private val psId_1: PhysicalSynchronizerId = PhysicalSynchronizerId.tryFromString(
    s"da::first::${ProtocolVersion.latest.toString}-0"
  )
  private val psId_2: PhysicalSynchronizerId = PhysicalSynchronizerId.tryFromString(
    s"da::second::${ProtocolVersion.latest.toString}-0"
  )

  sealed trait FrontendType extends Product with Serializable
  case object GrpcFrontend extends FrontendType {
    override def toString: String = "gRPC"
  }
  case object JsonFrontend extends FrontendType {
    override def toString: String = "JSON"
  }

  sealed trait TargetBackendType extends Product with Serializable
  case object PackageMetadataBacked extends TargetBackendType
  case object TopologyStateBacked extends TargetBackendType
  case object BothBackends extends TargetBackendType

  sealed trait SimpleAbstractCommand extends Product with Serializable {
    def templateId: lapi.value.Identifier
    def toGrpc: lapi.commands.Command.Command
    def toJson: JsCommand.Command
  }
  final case class SimpleCreateCommand(templateId: lapi.value.Identifier)
      extends SimpleAbstractCommand {
    override def toGrpc: lapi.commands.Command.Command =
      lapi.commands.Command.Command.Create(
        lapi.commands
          .CreateCommand(
            Some(templateId),
            createArguments = Some(lapi.value.Record(recordId = Some(templateId))),
          )
      )

    override def toJson: JsCommand.Command =
      JsCommand.CreateCommand(
        templateId = templateId,
        createArguments = io.circe.Json.fromString("create"),
      )
  }
  final case class SimpleExerciseCommand(templateId: lapi.value.Identifier)
      extends SimpleAbstractCommand {
    override def toGrpc: lapi.commands.Command.Command =
      lapi.commands.Command.Command.Exercise(
        lapi.commands.ExerciseCommand(
          templateId = Some(templateId),
          contractId = "some-cid",
          choice = "someChoice",
          choiceArgument = Some(Value(com.daml.ledger.api.v2.value.Value.Sum.Text("value"))),
        )
      )

    override def toJson: JsCommand.Command =
      JsCommand.ExerciseCommand(
        templateId = templateId,
        contractId = "some-cid",
        choice = "someChoice",
        choiceArgument = io.circe.Json.fromString("value"),
      )
  }

  final case class SimpleCreateAndExerciseCommand(templateId: lapi.value.Identifier)
      extends SimpleAbstractCommand {
    override def toGrpc: lapi.commands.Command.Command =
      lapi.commands.Command.Command.CreateAndExercise(
        lapi.commands.CreateAndExerciseCommand(
          templateId = Some(templateId),
          createArguments = Some(lapi.value.Record(recordId = Some(templateId))),
          choice = "someChoice",
          choiceArgument = Some(Value(com.daml.ledger.api.v2.value.Value.Sum.Text("value"))),
        )
      )

    override def toJson: JsCommand.Command =
      JsCommand.CreateAndExerciseCommand(
        templateId = templateId,
        createArguments = io.circe.Json.fromString("create"),
        choice = "someChoice",
        choiceArgument = io.circe.Json.fromString("value"),
      )
  }
  final case class SimpleExerciseByKeyCommand(templateId: lapi.value.Identifier)
      extends SimpleAbstractCommand {
    override def toGrpc: lapi.commands.Command.Command =
      lapi.commands.Command.Command.ExerciseByKey(
        lapi.commands.ExerciseByKeyCommand(
          templateId = Some(templateId),
          contractKey = Some(Value(com.daml.ledger.api.v2.value.Value.Sum.Text("key"))),
          choice = "someChoice",
          choiceArgument = Some(Value(com.daml.ledger.api.v2.value.Value.Sum.Text("value"))),
        )
      )

    override def toJson: JsCommand.Command =
      JsCommand.ExerciseByKeyCommand(
        templateId = templateId,
        contractKey = io.circe.Json.fromString("key"),
        choice = "someChoice",
        choiceArgument = io.circe.Json.fromString("value"),
      )
  }
}
