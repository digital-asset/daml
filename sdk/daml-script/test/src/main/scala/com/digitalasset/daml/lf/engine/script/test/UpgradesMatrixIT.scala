// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.SdkVersion
import com.daml.integrationtest.CantonFixture
import com.digitalasset.daml.lf.archive.DamlLf._
import com.digitalasset.daml.lf.archive.{ArchiveParser, Dar, DarWriter}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.ScriptLedgerClient.ReadablePackageId
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.GrpcLedgerClient
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.{ScriptLedgerClient, SubmitError}
import com.digitalasset.daml.lf.engine.{
  UpgradesMatrix,
  UpgradesMatrixCases,
  UpgradesMatrixCasesV2Dev,
  UpgradesMatrixCasesV2MaxStable,
}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.value.Value._
import com.google.protobuf.ByteString
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.Assertion
import org.scalatest.Inside.inside
import scalaz.OneAnd

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

// Split the tests across eight suites with eight Canton runners, which brings
// down the runtime from ~4000s on a single suite to ~1400s
class UpgradesMatrixIntegration0
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2MaxStable, 3, 0)
class UpgradesMatrixIntegration1
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2MaxStable, 3, 1)
class UpgradesMatrixIntegration2
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2MaxStable, 3, 2)

class UpgradesMatrixIntegration3 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 0)
class UpgradesMatrixIntegration4 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 1)
class UpgradesMatrixIntegration5 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 2)
class UpgradesMatrixIntegration6 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 3)
class UpgradesMatrixIntegration7 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 4)

/** A test suite to run the UpgradesMatrix matrix on Canton.
  *
  * This takes a while (~5000s when running with a single suite), so we have a
  * different test [[UpgradesMatrixUnit]] to catch simple engine issues early which
  * takes only ~40s.
  */
abstract class UpgradesMatrixIntegration(upgradesMatrixCases: UpgradesMatrixCases, n: Int, k: Int)
    extends UpgradesMatrix[
      ScriptLedgerClient.SubmitFailure,
      (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
    ](upgradesMatrixCases, Some((n, k)))
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
  val clientLocalDar = encodeDar(
    cases.clientLocalDalfName,
    cases.clientLocalDalf,
    List(
      (cases.templateDefsV1DalfName, cases.templateDefsV1Dalf),
      (cases.templateDefsV2DalfName, cases.templateDefsV2Dalf),
      (cases.commonDefsDalfName, cases.commonDefsDalf),
      (primDATypesDalfName, primDATypesDalf),
    ),
  )
  val clientGlobalDar = encodeDar(
    cases.clientGlobalDalfName,
    cases.clientGlobalDalf,
    List(
      (cases.templateDefsV2DalfName, cases.templateDefsV2Dalf),
      (cases.commonDefsDalfName, cases.commonDefsDalf),
      (primDATypesDalfName, primDATypesDalf),
    ),
  )

  private var scriptClient: GrpcLedgerClient = null

  override protected def beforeAll(): scala.Unit = {
    implicit def executionContext: ExecutionContext = ExecutionContext.global
    super.beforeAll()
    scriptClient = Await.result(
      for {
        client <- defaultLedgerClient()
        _ <- Future.traverse(
          List(commonDefsDar, templateDefsV1Dar, templateDefsV2Dar, clientLocalDar, clientGlobalDar)
        )(dar => client.packageManagementClient.uploadDarFile(dar))
        scriptClient = new GrpcLedgerClient(
          client,
          Some(Ref.UserId.assertFromString("upgrade-test-matrix")),
          cases.compiledPackages,
        )
      } yield scriptClient,
      30.seconds,
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
        languageVersionLookup = _ => Right(LanguageVersion.latestStableLfVersion),
        errorBehaviour = ScriptLedgerClient.SubmissionErrorBehaviour.MustSucceed,
      )
      .flatMap {
        case Right((Seq(ScriptLedgerClient.CreateResult(cid)), _)) => Future.successful(cid)
        case e => Future.failed(new RuntimeException(s"Couldn't create contract: $e"))
      }

  private val globalRandom = new scala.util.Random(0)
  private val converter = Converter(LanguageVersion.Major.V2)

  private def allocateParty(name: String): Future[Party] =
    Future(
      converter
        .toPartyIdHint("", name, globalRandom)
        .getOrElse(throw new IllegalArgumentException("Bad party name"))
    )
      .flatMap(scriptClient.allocateParty(_))

  override def setup(testHelper: cases.TestHelper): Future[UpgradesMatrixCases.SetupData] =
    for {
      alice <- allocateParty("Alice")
      bob <- allocateParty("Bob")
      clientLocalContractId <- createContract(
        alice,
        testHelper.clientLocalTplId,
        testHelper.clientContractArg(alice, bob),
      )
      clientGlobalContractId <- createContract(
        alice,
        testHelper.clientGlobalTplId,
        testHelper.clientContractArg(alice, bob),
      )
      globalContractId <- createContract(
        alice,
        testHelper.v1TplId,
        testHelper.globalContractArg(alice, bob),
      )
    } yield UpgradesMatrixCases.SetupData(
      alice = alice,
      bob = bob,
      clientLocalContractId = clientLocalContractId,
      clientGlobalContractId = clientGlobalContractId,
      globalContractId = globalContractId,
    )

  private def withUnvettedPackages[A](
      packages: List[ReadablePackageId]
  )(action: => Future[A]): Future[A] = {
    if (packages.isEmpty)
      action
    else
      for {
        _ <- scriptClient.unvetPackages(packages)
        participantUid <- scriptClient.getParticipantUid()
        _ <- scriptClient.waitUntilUnvettingVisible(packages, participantUid)
        result <- action
        _ <- scriptClient.vetPackages(packages)
        _ <- scriptClient.waitUntilVettingVisible(packages, participantUid)
      } yield result
  }

  private val creationPackages = cases.allCreationPackages.view.values
    .map(pkg => ReadablePackageId(pkg.pkgName, pkg.pkgVersion))
    .toList

  override def execute(
      setupData: UpgradesMatrixCases.SetupData,
      testHelper: cases.TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradesMatrixCases.ContractOrigin,
      creationPackageStatus: UpgradesMatrixCases.CreationPackageStatus,
  ): Future[Either[
    ScriptLedgerClient.SubmitFailure,
    (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
  ]] =
    for {
      disclosures <- contractOrigin match {
        case UpgradesMatrixCases.Disclosed =>
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
      commands = apiCommands.toList.map { n =>
        n.typeRef.pkg match {
          case Ref.PackageRef.Id(_) =>
            ScriptLedgerClient.CommandWithMeta(n, true)
          case Ref.PackageRef.Name(_) =>
            ScriptLedgerClient.CommandWithMeta(n, false)
        }
      }
      result <- withUnvettedPackages(
        creationPackageStatus match {
          case UpgradesMatrixCases.CreationPackageVetted => List.empty
          case UpgradesMatrixCases.CreationPackageUnvetted => creationPackages
        }
      ) {
        scriptClient.submit(
          actAs = OneAnd(setupData.alice, Set()),
          readAs = Set(),
          disclosures = disclosures,
          optPackagePreference =
            Some(List(cases.commonDefsPkgId, cases.templateDefsV2PkgId, cases.clientLocalPkgId)),
          commands = commands,
          prefetchContractKeys = List(),
          optLocation = None,
          languageVersionLookup = _ => Right(LanguageVersion.latestStableLfVersion),
          errorBehaviour = ScriptLedgerClient.SubmissionErrorBehaviour.Try,
        )
      }
    } yield result

  override def assertResultMatchesExpectedOutcome(
      result: Either[
        ScriptLedgerClient.SubmitFailure,
        (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
      ],
      expectedOutcome: UpgradesMatrixCases.ExpectedOutcome,
  ): Assertion = {
    expectedOutcome match {
      case UpgradesMatrixCases.ExpectSuccess =>
        result shouldBe a[Right[_, _]]
      case UpgradesMatrixCases.ExpectUpgradeError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.UpgradeError.ValidationFailed]
        }
      case UpgradesMatrixCases.ExpectAuthenticationError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.UpgradeError.AuthenticationFailed]
        }
      case UpgradesMatrixCases.ExpectRuntimeTypeMismatchError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.UpgradeError.TranslationFailed]
        }
      case UpgradesMatrixCases.ExpectPreprocessingError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(statusError, submitError)) =>
          statusError shouldBe a[StatusRuntimeException]
          val status = statusError.asInstanceOf[StatusRuntimeException].getStatus
          status.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
          status.getDescription should startWith("COMMAND_PREPROCESSING_FAILED")
          submitError shouldBe a[SubmitError.UnknownError]
        }
      case UpgradesMatrixCases.ExpectPreconditionViolated =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.TemplatePreconditionViolated]
        }
      case UpgradesMatrixCases.ExpectUnhandledException =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.FailureStatusError]
        }
      case UpgradesMatrixCases.ExpectInternalInterpretationError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(statusError, submitError)) =>
          statusError shouldBe a[StatusRuntimeException]
          val status = statusError.asInstanceOf[StatusRuntimeException].getStatus
          status.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
          status.getDescription should startWith("DAML_INTERPRETATION_ERROR")
          submitError shouldBe a[SubmitError.UnknownError]
        }
    }
  }
}
