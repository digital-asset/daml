// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.daml.integrationtest.CantonFixture
import com.daml.SdkVersion
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.daml.lf.archive.{Dar, DarWriter}
import com.digitalasset.daml.lf.archive.DamlLf._
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.engine.UpgradeTest
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.grpcLedgerClient.GrpcLedgerClient
import com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction.{ScriptLedgerClient, SubmitError}
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.stablepackages._
import com.digitalasset.daml.lf.value.Value._
import com.google.protobuf.ByteString
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import scalaz.OneAnd
import org.scalatest.Inside.inside
import org.scalatest.{Assertion, Succeeded}

class UpgradeTestIntegration
    extends UpgradeTest[
      ScriptLedgerClient.SubmitFailure,
      (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
    ]
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
        (mainDalfName, mainDalf.toByteArray),
        deps.map { case (name, dalf) => (name, dalf.toByteArray) },
      ),
      os,
    )
    os.toByteString
  }

  override lazy val languageVersion: LanguageVersion = LanguageVersion.v2_dev

  override val cantonFixtureDebugMode = CantonFixtureDebugKeepTmpFiles
  override protected val disableUpgradeValidation: Boolean = true
  override protected lazy val devMode: Boolean = true

  // Compiled dars
  // TODO: Rethink how to get the archive here without modifying the StablePackage interface in daml-lf/language
  val (primDATypesDalfName, primDATypesDalf) =
    StablePackagesV2.allArchivesByPkgId(stablePackages.Tuple2.packageId)
  val commonDefsDar = encodeDar(commonDefsDalfName, commonDefsDalf, List())
  val templateDefsV1Dar = encodeDar(
    templateDefsV1DalfName,
    templateDefsV1Dalf,
    List((commonDefsDalfName, commonDefsDalf)),
  )
  val templateDefsV2Dar = encodeDar(
    templateDefsV2DalfName,
    templateDefsV2Dalf,
    List((commonDefsDalfName, commonDefsDalf)),
  )
  val clientDar = encodeDar(
    clientDalfName,
    clientDalf,
    List(
      (templateDefsV1DalfName, templateDefsV1Dalf),
      (templateDefsV2DalfName, templateDefsV2Dalf),
      (commonDefsDalfName, commonDefsDalf),
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
      Some(Ref.ApplicationId.assertFromString("upgrade-test-matrix")),
      None,
      compiledPackages,
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
        commands = List(ScriptLedgerClient.CommandWithMeta(ApiCommand.Create(tplId, arg), true)),
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

  override def setup(testHelper: TestHelper): Future[SetupData] =
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
    } yield SetupData(
      alice = alice,
      bob = bob,
      clientContractId = clientContractId,
      globalContractId = globalContractId,
    )

  override def execute(
      setupData: SetupData,
      testHelper: TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: ContractOrigin,
  ): Future[Either[
    ScriptLedgerClient.SubmitFailure,
    (Seq[ScriptLedgerClient.CommandResult], ScriptLedgerClient.TransactionTree),
  ]] =
    for {
      disclosures <- contractOrigin match {
        case Disclosed =>
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
            n.copy(templateRef = TypeConRef.fromIdentifier(testHelper.v1TplId)),
            false,
          )
        case n @ ApiCommand.Exercise(`tplRef`, _, _, _) =>
          ScriptLedgerClient.CommandWithMeta(
            n.copy(typeRef = TypeConRef.fromIdentifier(testHelper.v1TplId)),
            false,
          )
        case n @ ApiCommand.ExerciseByKey(`tplRef`, _, _, _) =>
          ScriptLedgerClient.CommandWithMeta(
            n.copy(templateRef = TypeConRef.fromIdentifier(testHelper.v1TplId)),
            false,
          )
        case n @ ApiCommand.CreateAndExercise(`tplRef`, _, _, _) =>
          ScriptLedgerClient.CommandWithMeta(
            n.copy(templateRef = TypeConRef.fromIdentifier(testHelper.v1TplId)),
            false,
          )
        case n => ScriptLedgerClient.CommandWithMeta(n, true)
      }
      result <- scriptClient.submit(
        actAs = OneAnd(setupData.alice, Set()),
        readAs = Set(),
        disclosures = disclosures,
        optPackagePreference = Some(List(commonDefsPkgId, templateDefsV2PkgId, clientPkgId)),
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
      expectedOutcome: ExpectedOutcome,
  ): Assertion = {
    expectedOutcome match {
      case ExpectSuccess =>
        result shouldBe a[Right[_, _]]
      case ExpectUpgradeError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error should (
            be(a[SubmitError.UpgradeError.ValidationFailed]) or
              be(a[SubmitError.UpgradeError.DowngradeDropDefinedField]) or
              be(a[SubmitError.UpgradeError.ViewMismatch]) or
              be(a[SubmitError.UpgradeError.DowngradeFailed])
          )
        }
      case ExpectPreprocessingError =>
        inside(result) { case Left(_) =>
          // error shouldBe a[EE.Preprocessing] // I dont know what we mean by preprocessing here
          Succeeded
        }
      case ExpectPreconditionViolated =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.TemplatePreconditionViolated]
        }
      case ExpectUnhandledException =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.UnhandledException]
        }
      case ExpectInternalInterpretationError =>
        inside(result) { case Left(ScriptLedgerClient.SubmitFailure(_, error)) =>
          error shouldBe a[SubmitError.UnknownError]
        // Probably also assert that the message contains the word internal?
        }
    }
  }
}
