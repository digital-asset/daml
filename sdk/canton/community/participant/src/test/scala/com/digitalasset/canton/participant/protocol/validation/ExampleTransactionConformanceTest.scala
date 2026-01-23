// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.Eval
import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactoryImpl
import com.digitalasset.canton.participant.protocol.validation.ExampleTransactionConformanceTest.HashReInterpretationCounter
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.*
import com.digitalasset.canton.participant.store.ContractAndKeyLookup
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.DAMLe.{HasReinterpret, ReInterpretationResult}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator.ContractAuthenticatorFn
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.util.{ContractValidator, TestContractHasher}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfCommand,
  LfKeyResolver,
  LfPackageName,
  LfPackageVersion,
  LfPartyId,
}
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import com.digitalasset.daml.lf.language.Ast.{DeclaredImports, Expr, GenPackage, PackageMetadata}
import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

class ExampleTransactionConformanceTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  val sequencerTimestamp: CantonTimestamp = CantonTimestamp.ofEpochSecond(0)

  val ledgerTimeRecordTimeTolerance: Duration = Duration.ofSeconds(10)

  val packageName: LfPackageName = PackageName.assertFromString("package-name")
  val packageVersion: LfPackageVersion = LfPackageVersion.assertFromString("1.0.0")
  val packageMetadata: PackageMetadata = PackageMetadata(packageName, packageVersion, None)
  val genPackage: GenPackage[Expr] =
    GenPackage(
      modules = Map.empty,
      directDeps = Set.empty,
      languageVersion = LanguageVersion.defaultLfVersion,
      metadata = packageMetadata,
      imports = DeclaredImports(Set.empty),
      isUtilityPackage = true,
    )
  val packageResolver: PackageResolver = _ => _ => FutureUnlessShutdown.pure(Some(genPackage))

  val pureCrypto = new SymbolicPureCrypto()

  forAll(Table("contract ID version", CantonContractIdVersion.all*)) { contractIdVersion =>
    val factory: ExampleTransactionFactory = new ExampleTransactionFactory()(
      cantonContractIdVersion = contractIdVersion
    )

    def reinterpretExample(
        example: ExampleTransaction,
        usedPackages: Set[PackageId] = Set.empty,
        timeBoundaries: LedgerTimeBoundaries = LedgerTimeBoundaries.unconstrained,
    ): HasReinterpret & HashReInterpretationCounter = new HasReinterpret
      with HashReInterpretationCounter {

      override def reinterpret(
          contracts: ContractAndKeyLookup,
          contractAuthenticator: ContractAuthenticatorFn,
          submitters: Set[LfPartyId],
          command: LfCommand,
          ledgerTime: CantonTimestamp,
          preparationTime: CantonTimestamp,
          rootSeed: Option[LfHash],
          packageResolution: Map[PackageName, PackageId],
          expectFailure: Boolean,
          getEngineAbortStatus: GetEngineAbortStatus,
      )(implicit traceContext: TraceContext): EitherT[
        FutureUnlessShutdown,
        DAMLe.ReinterpretationError,
        ReInterpretationResult,
      ] = {
        incrementInterpretations()
        ledgerTime shouldEqual factory.ledgerTime
        preparationTime shouldEqual factory.preparationTime

        val (_, (reinterpretedTx, metadata, keyResolver), _) =
          // The code below assumes that for reinterpretedSubtransactions the combination
          // of command and root-seed wil be unique. In the examples used to date this is
          // the case. A limitation of this approach is that only one LookupByKey transaction
          // can be returned as the root seed is unset in the ReplayCommand.
          example.reinterpretedSubtransactions.find { case (viewTree, (tx, md, _), _) =>
            viewTree.viewParticipantData.rootAction.command == command &&
            md.seeds.get(tx.roots(0)) == rootSeed
          }.value

        EitherT.rightT(
          ReInterpretationResult(
            reinterpretedTx,
            metadata,
            keyResolver,
            usedPackages,
            timeBoundaries,
          )
        )
      }
    }

    def viewsWithNoInputKeys(
        rootViews: Seq[FullTransactionViewTree]
    ): NonEmpty[Seq[(FullTransactionViewTree, Seq[(TransactionView, LfKeyResolver)])]] =
      NonEmptyUtil.fromUnsafe(rootViews.map { viewTree =>
        // Include resolvers for all the subviews
        val resolvers =
          viewTree.view.allSubviewsWithPosition(viewTree.viewPosition).map { case (view, _) =>
            view -> (Map.empty: LfKeyResolver)
          }
        (viewTree, resolvers)
      })

    val transactionTreeFactory: TransactionTreeFactoryImpl =
      TransactionTreeFactoryImpl(
        ExampleTransactionFactory.submittingParticipant,
        factory.psid,
        factory.cantonContractIdVersion,
        factory.cryptoOps,
        TestContractHasher.Async,
        loggerFactory,
      )

    def reInterpret(
        mcc: ModelConformanceChecker,
        view: TransactionView,
        keyResolver: LfKeyResolver,
        commonData: TransactionProcessingSteps.CommonData,
    ): EitherT[Future, Error, ConformanceReInterpretationResult] =
      mcc
        .reInterpret(
          view,
          keyResolver,
          commonData.ledgerTime,
          commonData.preparationTime,
          getEngineAbortStatus = () => EngineAbortStatus.notAborted,
        )
        .failOnShutdown

    def check(
        mcc: ModelConformanceChecker,
        views: NonEmpty[Seq[(FullTransactionViewTree, Seq[(TransactionView, LfKeyResolver)])]],
        ips: TopologySnapshot = factory.topologySnapshot,
        reInterpretedTopLevelViews: ModelConformanceChecker.LazyAsyncReInterpretationMap = Map.empty,
    ): EitherT[Future, ErrorWithSubTransaction[Unit], Result] = {
      val rootViewTrees = views.map(_._1)
      val commonData = TransactionProcessingSteps.tryCommonData(rootViewTrees)
      val keyResolvers = views.forgetNE.flatMap { case (_, resolvers) => resolvers }.toMap
      val rootViewTreesWithEffects =
        rootViewTrees.map(tree => (tree, tree.view.allSubviews.map(_ => ())))
      mcc
        .check(
          rootViewTreesWithEffects,
          keyResolvers,
          ips,
          commonData,
          getEngineAbortStatus = () => EngineAbortStatus.notAborted,
          reInterpretedTopLevelViews,
        )
        .failOnShutdown
    }

    def buildUnderTest(reinterpretCommand: HasReinterpret): ModelConformanceChecker =
      new ModelConformanceChecker(
        reinterpretCommand,
        transactionTreeFactory,
        submittingParticipant,
        ContractValidator.AllowAll,
        packageResolver,
        pureCrypto,
        loggerFactory,
      )

    s"A model conformance checker for version $contractIdVersion" when {
      val relevantExamples = factory.standardHappyCases.filter {
        // If the transaction is empty there is no transaction view message. Therefore, the checker is not invoked.
        case factory.EmptyTransaction => false
        case _ => true
      }

      forEvery(relevantExamples) { example =>
        s"checking $example" must {

          val sut = buildUnderTest(reinterpretExample(example))

          "yield the correct result" in {
            for {
              result <- valueOrFail(
                check(sut, viewsWithNoInputKeys(example.rootTransactionViewTrees))
              )(s"model conformance check for root views")
            } yield {
              val Result(updateId, absoluteTransaction) = result
              updateId should equal(example.updateId)

              absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
              absoluteTransaction.unwrap.version should equal(
                example.versionedSuffixedTransaction.version
              )
              assert(
                absoluteTransaction.withoutVersion.equalForest(
                  example.wellFormedSuffixedTransaction.withoutVersion
                ),
                s"\n$absoluteTransaction should equal\n${example.wellFormedSuffixedTransaction} up to nid renaming",
              )
            }
          }

          "re-use pre-interpreted transactions" in {
            val topLevelViewTrees = NonEmptyUtil.fromUnsafe(
              example.rootTransactionViewTrees
                .filter(_.isTopLevel)
            )
            val reInterpretedTopLevelViews = topLevelViewTrees.forgetNE
              .map({ viewTree =>
                viewTree.view.viewHash ->
                  Eval.now(
                    reInterpret(
                      sut,
                      viewTree.view,
                      Map.empty: LfKeyResolver,
                      TransactionProcessingSteps.tryCommonData(topLevelViewTrees),
                    ).mapK(FutureUnlessShutdown.outcomeK)
                  )
              })
              .toMap

            val reInterpreter = reinterpretExample(example)
            val mcc = buildUnderTest(reInterpreter)

            for {
              result <- valueOrFail(
                check(
                  mcc,
                  viewsWithNoInputKeys(example.rootTransactionViewTrees),
                  reInterpretedTopLevelViews = reInterpretedTopLevelViews,
                )
              )(s"model conformance check for root views")
            } yield {
              val Result(updateId, absoluteTransaction) = result
              updateId should equal(example.updateId)
              absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
              absoluteTransaction.unwrap.version should equal(
                example.versionedSuffixedTransaction.version
              )
              assert(
                absoluteTransaction.withoutVersion.equalForest(
                  example.wellFormedSuffixedTransaction.withoutVersion
                ),
                s"$absoluteTransaction should equal ${example.wellFormedSuffixedTransaction} up to nid renaming",
              )
              reInterpreter.getInterpretationCount shouldBe 0
            }
          }

          "reinterpret views individually" in {
            example.transactionViewTrees
              .parTraverse_ { viewTree =>
                for {
                  result <- valueOrFail(check(sut, viewsWithNoInputKeys(Seq(viewTree))))(
                    s"model conformance check for view at ${viewTree.viewPosition}"
                  )
                } yield {
                  val Result(updateId, absoluteTransaction) = result
                  updateId should equal(example.updateId)
                  absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
                }
              }
              .map(_ => succeed)
          }
        }
      }
    }
  }
}

object ExampleTransactionConformanceTest {
  private trait HashReInterpretationCounter {
    private val counter = new AtomicInteger(0)
    def incrementInterpretations(): Unit = discard(counter.getAndIncrement())
    def getInterpretationCount: Int = counter.get()
  }
}
