// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.integrationtest.CantonFixture
import com.daml.SdkVersion
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.daml.lf.archive.ArchiveParser
import com.digitalasset.daml.lf.archive.{Dar, DarWriter}
import com.digitalasset.daml.lf.archive.DamlLf._
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.{UpgradeTest, UpgradeTestCases, UpgradeTestCasesV2Dev}
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.GrpcLedgerClient
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.{ScriptLedgerClient, SubmitError}
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.value.Value._
import com.google.protobuf.ByteString
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scalaz.OneAnd
import org.scalatest.Inside.inside
import org.scalatest.{Assertion, Succeeded}

// Split the tests across four suites with four Canton runners, which brings
// down the runtime from ~4000s on a single suite to ~1400s
class UpgradeTestIntegration0 extends UpgradeTestIntegration(4, 0)
class UpgradeTestIntegration1 extends UpgradeTestIntegration(4, 1)
class UpgradeTestIntegration2 extends UpgradeTestIntegration(4, 2)
class UpgradeTestIntegration3 extends UpgradeTestIntegration(4, 3)

/** A test suite to run the UpgradeTest matrix on Canton.
  *
  * This takes a while (~5000s when running with a single suite), so we have a
  * different test [[UpgradeTestUnit]] to catch simple engine issues early which
  * takes only ~40s.
  */
abstract class UpgradeTestIntegration(n: Int, k: Int)
    extends UpgradeTest[
      ScriptLedgerClient.SubmitFailure,
      (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
    ](UpgradeTestCasesV2Dev, Some((n, k)))
    with CantonFixture {
  def encodeDar(
      mainDalfName: String,
      mainDalf: Archive,
      deps: List[(String, Archive)],
  ): ByteString = {
    val os = ByteString.newOutput()
    DarWriter.encode(
      SdkVersion.sdkVersion,
      Dar(
        (mainDalfName, Bytes.fromByteString(mainDalf.toByteString)),
        deps.map { case (name, dalf) => (name, Bytes.fromByteString(dalf.toByteString)) },
      ),
      os,
    )
    os.toByteString
  }

  // override lazy val langVersion: LanguageVersion = LanguageVersion.v2_dev

  override val cantonFixtureDebugMode = CantonFixtureDebugKeepTmpFiles
  override protected val disableUpgradeValidation: Boolean = true
  override protected lazy val devMode: Boolean = true

  // Compiled dars
  val primDATypes = cases.stablePackages.allPackages.find(_.moduleName.dottedName == "DA.Types").get
  val primDATypesDalfName = s"${primDATypes.name}-${primDATypes.packageId}.dalf"
  val primDATypesDalf = ArchiveParser.assertFromBytes(primDATypes.bytes)

  val commonDefsDar = encodeDar(cases.commonDefsDalfName, cases.commonDefsDalf, List())
  val templateDefsV1Dar = encodeDar(
    cases.templateDefsV1DalfName,
    cases.templateDefsV1Dalf,
    List((cases.commonDefsDalfName, cases.commonDefsDalf)),
  )
  val templateDefsV2Dar = encodeDar(
    cases.templateDefsV2DalfName,
    cases.templateDefsV2Dalf,
    List((cases.commonDefsDalfName, cases.commonDefsDalf)),
  )
  val clientDar = encodeDar(
    cases.clientDalfName,
    cases.clientDalf,
    List(
      (cases.templateDefsV1DalfName, cases.templateDefsV1Dalf),
      (cases.templateDefsV2DalfName, cases.templateDefsV2Dalf),
      (cases.commonDefsDalfName, cases.commonDefsDalf),
      (primDATypesDalfName, primDATypesDalf),
    ),
  )

  private var client: LedgerClient = null
  private var scriptClient: GrpcLedgerClient = null

  override protected def beforeAll(): scala.Unit = {
    implicit def executionContext: ExecutionContext = ExecutionContext.global
    super.beforeAll()
    client = Await.result(
      for {
        client <- defaultLedgerClient()
        _ <- Future.traverse(List(commonDefsDar, templateDefsV1Dar, templateDefsV2Dar, clientDar))(
          dar => client.packageManagementClient.uploadDarFile(dar)
        )
      } yield client,
      10.seconds,
    )
    scriptClient = new GrpcLedgerClient(
      client,
      Some(Ref.UserId.assertFromString("upgrade-test-matrix")),
      None,
      cases.compiledPackages,
    )
  }

  private def createContract(
      party: Party,
      tplId: Identifier,
      arg: ValueRecord,
  ): Future[ContractId] =
    scriptClient
      .submit(
        actAs = OneAnd(party, Set()),
        readAs = Set(),
        disclosures = List(),
        optPackagePreference = None,
        commands =
          List(ScriptLedgerClient.CommandWithMeta(ApiCommand.Create(tplId.toRef, arg), true)),
        prefetchContractKeys = List(),
        optLocation = None,
        languageVersionLookup =
          _ => Right(LanguageVersion.defaultOrLatestStable(LanguageMajorVersion.V2)),
        errorBehaviour = ScriptLedgerClient.SubmissionErrorBehaviour.MustSucceed,
      )
      .flatMap {
        case Right((Seq(ScriptLedgerClient.CreateResult(cid)), _)) => Future.successful(cid)
        case e => Future.failed(new RuntimeException(s"Couldn't create contract: $e"))
      }

  private val globalRandom = new scala.util.Random(0)
  private val converter = Converter(LanguageMajorVersion.V2)

  private def allocateParty(name: String): Future[Party] =
    Future(
      converter
        .toPartyIdHint("", name, globalRandom)
        .getOrElse(throw new IllegalArgumentException("Bad party name"))
    )
      .flatMap(scriptClient.allocateParty(_))

  override def setup(testHelper: cases.TestHelper): Future[UpgradeTestCases.SetupData] =
    for {
      alice <- allocateParty("Alice")
      bob <- allocateParty("Bob")
      clientContractId <- createContract(
        alice,
        testHelper.clientTplId,
        testHelper.clientContractArg(alice, bob),
      )
      globalContractId <- createContract(
        alice,
        testHelper.v1TplId,
        testHelper.globalContractArg(alice, bob),
      )
    } yield UpgradeTestCases.SetupData(
      alice = alice,
      bob = bob,
      clientContractId = clientContractId,
      globalContractId = globalContractId,
    )

  override def execute(
      setupData: UpgradeTestCases.SetupData,
      testHelper: cases.TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradeTestCases.ContractOrigin,
  ): Future[Either[
    ScriptLedgerClient.SubmitFailure,
    (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
  ]] =
    for {
      disclosures <- contractOrigin match {
        case UpgradeTestCases.Disclosed =>
          scriptClient
            .queryContractId(
              OneAnd(setupData.alice, Set()),
              testHelper.v1TplId,
              setupData.globalContractId,
            )
            .flatMap {
              case None => Future.failed(new RuntimeException("Couldn't fetch disclosure?"))
              case Some(activeContract) =>
                Future.successful(
                  List(
                    Disclosure(
                      activeContract.templateId,
                      activeContract.contractId,
                      activeContract.blob,
                    )
                  )
                )
            }
        case _ => Future.successful(List())
      }
      tplRef = testHelper.tplRef
      // GrpcLedgerClient doesn't support Name refs, so we replace with pkgId and use the explicitPackageId flag instead
      commands = apiCommands.toList.map {
        case n @ ApiCommand.Create(`tplRef`, _) =>
          ScriptLedgerClient.CommandWithMeta(
            n.copy(templateRef = testHelper.v1TplId.toRef),
            false,
          )
        case n @ ApiCommand.Exercise(`tplRef`, _, _, _) =>
          ScriptLedgerClient.CommandWithMeta(
            n.copy(typeRef = testHelper.v1TplId.toRef),
            false,
          )
        case n @ ApiCommand.ExerciseByKey(`tplRef`, _, _, _) =>
          ScriptLedgerClient.CommandWithMeta(
            n.copy(templateRef = testHelper.v1TplId.toRef),
            false,
          )
        case n @ ApiCommand.CreateAndExercise(`tplRef`, _, _, _) =>
          ScriptLedgerClient.CommandWithMeta(
            n.copy(templateRef = testHelper.v1TplId.toRef),
            false,
          )
        case n => ScriptLedgerClient.CommandWithMeta(n, true)
      }
      result <- scriptClient.submit(
        actAs = OneAnd(setupData.alice, Set()),
        readAs = Set(),
        disclosures = disclosures,
        optPackagePreference =
          Some(List(cases.commonDefsPkgId, cases.templateDefsV2PkgId, cases.clientPkgId)),
        commands = commands,
        prefetchContractKeys = List(),
        optLocation = None,
        languageVersionLookup =
          _ => Right(LanguageVersion.defaultOrLatestStable(LanguageMajorVersion.V2)),
        errorBehaviour = ScriptLedgerClient.SubmissionErrorBehaviour.Try,
      )
    } yield result

  override def assertResultMatchesExpectedOutcome(
      result: Either[
        ScriptLedgerClient.SubmitFailure,
        (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
      ],
      expectedOutcome: UpgradeTestCases.ExpectedOutcome,
  ): Assertion = {
    expectedOutcome match {
      case UpgradeTestCases.ExpectSuccess =>
        result shouldBe a[Right[_, _]]
      case UpgradeTestCases.ExpectUpgradeError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error should (
            be(a[SubmitError.UpgradeError.ValidationFailed]) or
              be(a[SubmitError.UpgradeError.DowngradeDropDefinedField]) or
              be(a[SubmitError.UpgradeError.DowngradeFailed])
          )
        }
      case UpgradeTestCases.ExpectPreprocessingError =>
        inside(result) { case Left(_) =>
          // error shouldBe a[EE.Preprocessing] // I dont know what we mean by preprocessing here
          Succeeded
        }
      case UpgradeTestCases.ExpectPreconditionViolated =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.TemplatePreconditionViolated]
        }
      case UpgradeTestCases.ExpectUnhandledException =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.FailureStatusError]
        }
      case UpgradeTestCases.ExpectInternalInterpretationError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.UnknownError]
        // Probably also assert that the message contains the word internal?
        }
    }
  }
}
