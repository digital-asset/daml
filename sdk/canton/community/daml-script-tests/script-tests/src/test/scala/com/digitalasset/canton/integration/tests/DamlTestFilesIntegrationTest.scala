// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.integration.tests

import cats.syntax.either.*
import com.daml.tls.TlsConfiguration
import com.digitalasset.canton.BaseTest.DamlScript3TestFilesPath
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.DamlTestFilesIntegrationTest.failingScriptsDamlTestFiles
import com.digitalasset.canton.integration.{
  ConfigTransform,
  ConfigTransforms,
  EnterpriseIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetup,
  SharedEnvironment,
}
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.CantonOnly.tryBuildCompiledPackages
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref.{Identifier, QualifiedName}
import com.digitalasset.daml.lf.engine.free
import com.digitalasset.daml.lf.engine.script.ParticipantMode.RemoteParticipantHost
import com.digitalasset.daml.lf.engine.script.RunnerMainConfig.RunMode.RunOne
import com.digitalasset.daml.lf.engine.script.Script.FailedCmd
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode.Static
import com.digitalasset.daml.lf.engine.script.{RunnerMain, RunnerMainConfig, Script}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException

import java.io.File
import java.util.zip.ZipInputStream

final case class TestedScript(scriptIdentifier: String, expectedFailure: Option[Failure] = None)

final case class Failure(exceptionClass: Class[? <: Throwable], exceptionMessage: String)

trait DamlTestFilesIntegrationTest extends EnterpriseIntegrationTest {
  self: EnvironmentSetup =>

  protected def uckMode: Boolean

  protected def enableLfDev: Boolean

  // Skip test if protocol version is smaller
  protected def minimumProtocolVersion: ProtocolVersion = ProtocolVersion.minimum

  protected def darPath: String

  private lazy val lowerCommandTrackerDuration: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_ {
      _.focus(_.ledgerApi.commandService.defaultTrackingTimeout)
        .replace(config.NonNegativeFiniteDuration.ofSeconds(15))
    }

  private lazy val useTestingTimeService: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService)

  private lazy val maybeEnableLfDev: Seq[ConfigTransform] =
    if (enableLfDev)
      ConfigTransforms.enableAlphaVersionSupport
    else Nil

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(lowerCommandTrackerDuration)
      .addConfigTransforms(useTestingTimeService)
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransforms(maybeEnableLfDev*)
      .withSetup { env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(darPath)
      }

  protected def allScripts: Seq[String] = {
    val payload = BinaryFileUtil.readByteStringFromFile(darPath)

    val listScriptFiles = for {
      nonEmptyPayload <- payload

      // decode dar and compile packages
      stream = new ZipInputStream(nonEmptyPayload.newInput())
      dar <- DarDecoder.readArchive(darPath, stream).leftMap(_.toString)
      darMap = dar.all.toMap
      compiledPackages = tryBuildCompiledPackages(darMap, enableLfDev)
      (packageId, packageObj) = dar.main

      modulesList = packageObj.modules.toList
      listScriptFiles = modulesList.flatMap { case (moduleName, module) =>
        // extract all moduleName:scriptName from definitions
        module.definitions.toList.map { case (name, _) =>
          val id = Identifier(packageId, QualifiedName(moduleName, name))
          Script
            .fromIdentifier(compiledPackages, id) match {
            // We exclude generated identifiers starting with `$`. and only accept scripts without arguments
            case Right(_: Script.Action) if !name.dottedName.startsWith("$") =>
              Right(id.qualifiedName.toString)
            case _ => Left("not a script test")
          }
        }
      }
    } yield listScriptFiles

    listScriptFiles
      .fold(err => sys.error(s"Unable to read dar `$darPath`: $err"), identity)
      .collect { case Right(testedScript) => testedScript }
  }

  protected def failingScripts: Map[String, Failure] = Map()

  protected def filterOutScripts: Seq[String] = Seq() // Tests that will not be executed

  private lazy val scripts = {
    val failing = failingScripts.map { case (script, expectedFailure) =>
      TestedScript(script, Some(expectedFailure))
    }

    val success = (allScripts diff filterOutScripts diff failingScripts.keys.toSeq)
      .map(TestedScript.apply(_))

    failing ++ success
  }

  protected def getConfig(scriptId: String, host: String, port: Port): RunnerMainConfig =
    RunnerMainConfig(
      darPath = new File(darPath),
      runMode = RunOne(scriptId, None, None),
      participantMode = RemoteParticipantHost(host, port.unwrap),
      timeMode = Static,
      accessTokenFile = None,
      tlsConfig = TlsConfiguration(enabled = false),
      maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
      userId = None,
      uploadDar = true,
    )

  protected def checkFailure(
      e: Throwable,
      expected: Failure,
      testedScript: TestedScript,
  ): Assertion =
    if (expected.exceptionClass == e.getClass) {
      e.getMessage should include regex expected.exceptionMessage
      logger.debug(
        s"Script ${testedScript.scriptIdentifier} failed as expected with error",
        e,
      )
      succeed
    } else
      fail(
        s"""unexpected exception was thrown $e while an exception of ${expected.exceptionClass} with message "${expected.exceptionMessage}" was expected""",
        e,
      )

  forAll(scripts) { testedScript =>
    s"run ${testedScript.scriptIdentifier}" onlyRunWithOrGreaterThan minimumProtocolVersion in {
      env =>
        import env.*

        val config = getConfig(
          testedScript.scriptIdentifier,
          participant1.config.ledgerApi.address,
          participant1.config.ledgerApi.port,
        )

        testedScript.expectedFailure match {
          case None => RunnerMain.main(config)
          case Some(expectedFailure) =>
            try {
              loggerFactory.suppressWarningsAndErrors {
                RunnerMain.main(config)
              }
              fail(
                s"the expected exception of class ${expectedFailure.exceptionClass} with message ${expectedFailure.exceptionMessage} was not thrown"
              )
            } catch {
              case e: TestFailedException => throw e
              case e: Throwable =>
                checkFailure(e, expectedFailure, testedScript)
            }
        }

        succeed
    }
  }
}

abstract class DamlTestFilesNonUckIntegrationTest
    extends DamlTestFilesIntegrationTest
    with SharedEnvironment {

  override lazy val darPath: String = DamlTestFilesPath
  override lazy val enableLfDev = true
  override lazy val uckMode = false

  // TODO(#16458) This should be a stable protocol version
  /*
    We cannot use a stable protocol version because DamlTestFilesPath needs to be compiled with LF 2.dev
    Our Daml test files are compiled with LF 2.dev because they use keys.
   */
  override lazy val minimumProtocolVersion = ProtocolVersion.dev

  override lazy val filterOutScripts: Seq[String] = Seq(
    "ContractKeyNotVisible:blindLookup",
    "ContractKeyNotVisible:divulgeeLookup",
    "ExceptionSemantics:duplicateKey",
  )
}

class DamlTestFilesNonUckReferenceIntegrationTest extends DamlTestFilesNonUckIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

abstract class DamlTestLfDevFilesIntegrationTest
    extends DamlTestFilesIntegrationTest
    with SharedEnvironment {

  override lazy val uckMode = true
  override lazy val enableLfDev = true
  override lazy val darPath = DamlTestLfDevFilesPath
  override lazy val minimumProtocolVersion = ProtocolVersion.dev

  override lazy val failingScripts = failingScriptsDamlTestFiles.filterNot { case (scriptId, _) =>
    filterOutScripts.toSet(scriptId)
  }
  override lazy val filterOutScripts: Seq[String] = Seq(
    "ContractKeyNotVisible:blindLookup",
    "ContractKeyNotVisible:divulgeeLookup",
    "ExceptionSemantics:duplicateKey",
    "LfStableContractKeys:run",
    "LfStableContractKeyThroughExercises:run",
    "LFContractKeys:lookupTest",
  )
}

class DamlTestLfDevFilesReferenceIntegrationTest extends DamlTestLfDevFilesIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

abstract class DamlScript3TestFilesIntegrationTest
    extends DamlTestFilesIntegrationTest
    with SharedEnvironment {

  // Canton 3.0 doesn't support contract keys and later will likely only support non-unique contract key mode
  override lazy val uckMode = false
  override lazy val darPath = DamlScript3TestFilesPath
  override lazy val enableLfDev = true
  override lazy val minimumProtocolVersion = ProtocolVersion.dev

  override lazy val failingScripts = Map(
    "Daml3ScriptTrySubmit:contractNotActive" -> Failure(
      classOf[free.InterpretationError],
      "contractNotActive no additional info",
    ),
    "Daml3ScriptTrySubmit:truncatedError" -> Failure(
      classOf[free.InterpretationError],
      "EXPECTED_TRUNCATED_ERROR",
    ),
  )

  override lazy val filterOutScripts: Seq[String] = Seq("Daml3ScriptTrySubmit:duplicateContractKey")
}

class DamlScript3TestFilesReferenceIntegrationTest extends DamlScript3TestFilesIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
}

object DamlTestFilesIntegrationTest {

  lazy val failingScriptsDamlTestFiles: Map[String, Failure] = Map(
    "AuthEvalOrder:t1_create_success" -> Failure(
      classOf[free.InterpretationError],
      "t1 finished with no authorization failure",
    ),
    "AuthEvalOrder:t2_create_badlyAuthorized" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "AuthEvalOrder:t3_createViaExerice_success" -> Failure(
      classOf[free.InterpretationError],
      "t3 finished with no authorization failure",
    ),
    "AuthEvalOrder:t4_createViaExerice_badlyAuthorized" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "AuthFailure:t1_CreateMissingAuthorization" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "AuthFailure:t2_MaintainersNotSubsetOfSignatories" -> Failure(
      classOf[FailedCmd],
      "has maintainers .* which are not a subset of the signatories",
    ),
    "AuthFailure:t3_FetchMissingAuthorization" -> Failure(
      classOf[FailedCmd],
      "requires one of the stakeholders .* of the fetched contract to be an authorizer",
    ),
    "AuthFailure:t4_LookupByKeyMissingAuthorization" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* for lookup by key",
    ),
    "AuthFailure:t5_ExerciseMissingAuthorization" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "ConsumedContractKey:testFetchFromConsumingChoice" -> Failure(
      classOf[FailedCmd],
      "Update failed due to fetch of an inactive contract",
    ),
    "ConsumedContractKey:testFetchKeyFromConsumingChoice" -> Failure(
      classOf[FailedCmd],
      "dependency error: couldn't find key",
    ),
    "ContractIdInContractKeySkipCheck:createCmdCrashes" -> Failure(
      classOf[FailedCmd],
      "Contract IDs are not supported",
    ),
    // should have failed but it succeeds (tracked by https://github.com/digital-asset/daml/issues/17554)
    //    "ContractIdInContractKeySkipCheck:queryCrashes" -> Failure(
    //      classOf[FailedCmd],
    //      "Contract IDs are not supported",
    //    ),
    "ContractIdInContractKeySkipCheck:exerciseCmdCrashes" -> Failure(
      classOf[FailedCmd],
      "Contract IDs are not supported",
    ),
    "ContractIdInContractKeySkipCheck:createCrashes" -> Failure(
      classOf[FailedCmd],
      "Contract IDs are not supported",
    ),
    "ContractIdInContractKeySkipCheck:fetchCrashes" -> Failure(
      classOf[FailedCmd],
      "Contract IDs are not supported",
    ),
    "ContractIdInContractKeySkipCheck:lookupCrashes" -> Failure(
      classOf[FailedCmd],
      "Contract IDs are not supported",
    ),
    "ContractIdInContractKeySkipCheck:exerciseCrashes" -> Failure(
      classOf[FailedCmd],
      "Contract IDs are not supported",
    ),
    "ContractKeyNotEffective:fetchByKeyMustFail" -> Failure(
      classOf[FailedCmd],
      "Setting time backwards is not allowed",
    ),
    "ContractKeyNotVisible:aScript" -> Failure(
      classOf[free.InterpretationError],
      "Couldn't see contract with key .*",
    ),
    "ContractKeyNotVisible:blindLookup" -> Failure(
      classOf[FailedCmd],
      "expected unassigned key, which already exists",
    ),
    "ContractKeyNotVisible:divulgeeLookup" -> Failure(
      classOf[FailedCmd],
      "expected unassigned key, which already exists",
    ),
    "ExceptionSemantics:unhandledArithmeticError" -> Failure(
      classOf[FailedCmd],
      "UNHANDLED_EXCEPTION/DA.Exception.ArithmeticError:ArithmeticError",
    ),
    "ExceptionSemantics:unhandledUserException" -> Failure(
      classOf[FailedCmd],
      "UNHANDLED_EXCEPTION/ExceptionSemantics:E",
    ),
    "ExceptionSemantics:tryContext" -> Failure(
      classOf[FailedCmd],
      "Contract could not be found",
    ),
    "EmptyContractKeyMaintainers:createCmdNoMaintainer" -> Failure(
      classOf[FailedCmd],
      "Update failed due to a contract key with an empty set of maintainers",
    ),
    "EmptyContractKeyMaintainers:queryNoMaintainer" -> Failure(
      classOf[free.InterpretationError],
      "Couldn't see contract with key",
    ),
    "EmptyContractKeyMaintainers:createNoMaintainer" -> Failure(
      classOf[FailedCmd],
      "Update failed due to a contract key with an empty set of maintainers",
    ),
    "EmptyContractKeyMaintainers:fetchNoMaintainer" -> Failure(
      classOf[FailedCmd],
      "Update failed due to a contract key with an empty set of maintainers",
    ),
    "EmptyContractKeyMaintainers:lookupNoMaintainer" -> Failure(
      classOf[FailedCmd],
      "Update failed due to a contract key with an empty set of maintainers",
    ),
    "FailedFetch:fetchNonStakeholder" -> Failure(
      classOf[FailedCmd],
      "CONTRACT_NOT_FOUND",
    ),
    "FetchByKey:failSpeedy" -> Failure(
      classOf[FailedCmd],
      "couldn't find key",
    ),
    "FetchByKey:failLedger" -> Failure(
      classOf[FailedCmd],
      "couldn't find key",
    ),
    "KeyNotVisibleStakeholders:divulgeeFetch" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "KeyNotVisibleStakeholders:divulgeeLookup" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "KeyNotVisibleStakeholders:blindFetch" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "KeyNotVisibleStakeholders:blindLookup" -> Failure(
      classOf[FailedCmd],
      "requires authorizers .* but only .* were given",
    ),
    "LedgerTestException:test" -> Failure(
      classOf[free.InterpretationError],
      "ohno",
    ),
    "TransientFailure:testBio" -> Failure(
      classOf[FailedCmd],
      "FAILED_PRECONDITION",
    ),
  )

}
