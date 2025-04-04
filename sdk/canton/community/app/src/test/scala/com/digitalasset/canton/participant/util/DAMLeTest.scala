// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.data.EitherT
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory}
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.store.ContractLookupAndVerification
import com.digitalasset.canton.participant.store.memory.{
  InMemoryDamlPackageStore,
  NoopPackageMetadataView,
}
import com.digitalasset.canton.participant.util.DAMLe.{
  EngineAborted,
  ReInterpretationResult,
  ReinterpretationError,
}
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasActorSystem,
  HasExecutionContext,
  LfCreateCommand,
  LfPackageVersion,
  LfPartyId,
  protocol,
}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.Versioned
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext
import scala.math.Ordered.orderingToOrdered

trait DAMLeTest
    extends AsyncWordSpec
    with BaseTest
    with HasActorSystem
    with HasExecutionContext
    with FailOnShutdown {

  protected def cantonTestsPath: String
  protected def testDarFileName: String
  protected def enableLfDev: Boolean
  protected def enableLfBeta: Boolean

  private val engine =
    DAMLe.newEngine(
      enableLfDev = enableLfDev,
      enableLfBeta = enableLfBeta,
      enableStackTraces = false,
      // Increase granularity of interruptions so that command reinterpretation gets
      // some `ResultInterruption`s during its execution before completing.
      iterationsBetweenInterruptions = 10,
    )

  private val packageService = DAMLeTest.packageService(engine, loggerFactory)
  protected val damlE: DAMLe =
    new DAMLe(
      DAMLe.packageResolver(packageService),
      engine,
      EngineLoggingConfig(),
      loggerFactory,
    )

  protected val alice: LfPartyId = LfPartyId.assertFromString("Alice")
  protected val bob: LfPartyId = LfPartyId.assertFromString("Bob")

  protected def resolveTemplateIdPackageNameAndPackageVersion(
      module: String,
      template: String,
  ): FutureUnlessShutdown[(Identifier, PackageName, Option[LfPackageVersion])] = {
    val payload = BinaryFileUtil
      .readByteStringFromFile(cantonTestsPath)
      .valueOrFail("could not load test")
    for {
      _ <- packageService
        .upload(
          darBytes = payload,
          description = Some(testDarFileName),
          submissionIdO = None,
          vetAllPackages = false,
          synchronizeVetting = PackageVettingSynchronization.NoSync,
          expectedMainPackageId = None,
        )
        .value
      moduleName = DottedName.assertFromString(module)
      packageIdsWithState <- packageService.listPackages()
      packages <- packageIdsWithState
        .parTraverse(packageIdWithState =>
          packageService
            .getPackage(packageIdWithState.packageId)
            .map(_.map(p => (packageIdWithState.packageId, p)))
        )
        .map(_.flattenOption)
      (packageId, astPackage) = packages.find(_._2.modules.contains(moduleName)).value

      packageName = astPackage.metadata.name
      packageVersion = Option.when(
        astPackage.languageVersion > LanguageVersion.Features.persistedPackageVersion
      )(astPackage.metadata.version)
      templateId = Identifier(
        PackageId.assertFromString(packageId),
        QualifiedName(moduleName, DottedName.assertFromString(template)),
      )
    } yield (templateId, packageName, packageVersion)
  }
}

object DAMLeTest {
  val pureCrypto = new SymbolicPureCrypto

  def packageService(
      engine: Engine,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): PackageService = {
    val timeouts = ProcessingTimeout()
    val packageDependencyResolver = new PackageDependencyResolver(
      new InMemoryDamlPackageStore(loggerFactory),
      timeouts,
      loggerFactory,
    )
    val packageUploader = PackageUploader(
      clock = new SimClock(loggerFactory = loggerFactory),
      engine = engine,
      packageMetadataView = NoopPackageMetadataView,
      packageDependencyResolver = packageDependencyResolver,
      enableUpgradeValidation = false,
      exitOnFatalFailures = true,
      futureSupervisor = FutureSupervisor.Noop,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
    new PackageService(
      packageDependencyResolver = packageDependencyResolver,
      packageUploader = packageUploader,
      packageOps = new PackageOpsForTesting(DefaultTestIdentities.participant1, loggerFactory),
      packageMetadataView = NoopPackageMetadataView,
      metrics = ParticipantTestMetrics,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
  }
}

class DAMLeTestDefault extends DAMLeTest {

  override def cantonTestsPath: String = CantonTestsPath
  override def testDarFileName: String = "CantonTests"
  override def enableLfDev: Boolean = false
  override def enableLfBeta: Boolean = false

  private def mkContractInst(): FutureUnlessShutdown[Value.VersionedContractInstance] =
    for {
      (templateId, packageName, packageVersionO) <- resolveTemplateIdPackageNameAndPackageVersion(
        "FailedTransactionsDoNotDivulge",
        "Two",
      )
    } yield {
      val arg = Value.ValueRecord(
        None,
        ImmArray(
          (None /* sig */ -> Value.ValueParty(alice)),
          (None /* obs */ -> Value.ValueParty(bob)),
        ),
      )
      LfContractInst(
        packageName = packageName,
        packageVersion = packageVersionO,
        template = templateId,
        arg = Versioned(protocol.DummyTransactionVersion, arg),
      )
    }

  private lazy val zeroSeed =
    LfHash.assertFromByteArray(new Array[Byte](LfHash.underlyingHashLength))

  private def reinterpretCreateCmd(
      contractInst: Value.VersionedContractInstance,
      getEngineAbortStatus: GetEngineAbortStatus,
  ): EitherT[
    FutureUnlessShutdown,
    ReinterpretationError,
    ReInterpretationResult,
  ] = {
    val unversionedContractInst = contractInst.unversioned
    val createCmd = LfCreateCommand(unversionedContractInst.template, unversionedContractInst.arg)

    damlE.reinterpret(
      ContractLookupAndVerification.noContracts(loggerFactory),
      Set(alice, bob),
      createCmd,
      CantonTimestamp.Epoch,
      CantonTimestamp.Epoch,
      Some(zeroSeed),
      packageResolution = Map.empty,
      expectFailure = false,
      getEngineAbortStatus = getEngineAbortStatus,
    )
  }

  "DAMLe" should {

    "produce a result when reinterpretation continues freely" in {
      for {
        contractInst <- mkContractInst()
        _ <- reinterpretCreateCmd(
          contractInst,
          getEngineAbortStatus = () => EngineAbortStatus.notAborted,
        ).value
      } yield {
        succeed
      }
    }

    "fail with an abort error when reinterpretation is aborted" in {
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        for {
          contractInst <- mkContractInst()
          error <- reinterpretCreateCmd(
            contractInst,
            getEngineAbortStatus = () => EngineAbortStatus(Some("test abort")),
          ).swap.value
        } yield {
          error.value shouldBe EngineAborted("test abort")
        },
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.warningMessage should include(s"Aborting engine computation, reason = test abort"),
              "engine gets aborted",
            )
          )
        ),
      )
    }
  }
}
