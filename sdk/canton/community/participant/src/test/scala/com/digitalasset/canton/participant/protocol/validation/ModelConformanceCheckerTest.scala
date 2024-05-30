// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{PackageId, PackageName}
import com.daml.lf.engine
import com.daml.lf.language.Ast.{Expr, GenPackage, PackageMetadata}
import com.daml.lf.language.LanguageVersion
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FreeKey,
  FullTransactionViewTree,
  TransactionView,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactoryImpl
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.*
import com.digitalasset.canton.participant.protocol.{
  SerializableContractAuthenticator,
  TransactionProcessingSteps,
}
import com.digitalasset.canton.participant.store.ContractLookup
import com.digitalasset.canton.participant.util.DAMLe.{EngineError, PackageResolver}
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{lfHash, submittingParticipant}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.topology.transaction.VettedPackages
import com.digitalasset.canton.topology.{TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{
  BaseTest,
  LfCommand,
  LfKeyResolver,
  LfPackageName,
  LfPackageVersion,
  LfPartyId,
  RequestCounter,
}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import pprint.Tree

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

class ModelConformanceCheckerTest extends AsyncWordSpec with BaseTest {

  implicit val ec: ExecutionContext = directExecutionContext

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  val sequencerTimestamp: CantonTimestamp = CantonTimestamp.ofEpochSecond(0)

  val ledgerTimeRecordTimeTolerance: Duration = Duration.ofSeconds(10)

  def validateContractOk(
      contract: SerializableContract,
      getEngineAbortStatus: GetEngineAbortStatus,
      context: TraceContext,
  ): EitherT[Future, ContractValidationFailure, Unit] = EitherT.pure(())

  def reinterpret(example: ExampleTransaction)(
      _contracts: ContractLookup,
      _submitters: Set[LfPartyId],
      cmd: LfCommand,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      rootSeed: Option[LfHash],
      _inRollback: Boolean,
      _viewHash: ViewHash,
      _traceContext: TraceContext,
      _packageResolution: Map[PackageName, PackageId],
      _getEngineAbortStatus: GetEngineAbortStatus,
  ): EitherT[
    Future,
    DAMLeError,
    (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
  ] = {

    ledgerTime shouldEqual factory.ledgerTime
    submissionTime shouldEqual factory.submissionTime

    val (_viewTree, (reinterpretedTx, metadata, keyResolver), _witnesses) =
      example.reinterpretedSubtransactions.find { case (viewTree, (tx, md, keyResolver), _) =>
        viewTree.viewParticipantData.rootAction.command == cmd &&
        // Commands are otherwise not sufficiently unique (whereas with nodes, we can produce unique nodes).
        rootSeed == md.seeds.get(tx.roots(0))
      }.value

    EitherT.rightT[Future, DAMLeError]((reinterpretedTx, metadata, keyResolver))
  }

  def failOnReinterpret(
      _contracts: ContractLookup,
      _submitters: Set[LfPartyId],
      cmd: LfCommand,
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      rootSeed: Option[LfHash],
      _inRollback: Boolean,
      _viewHash: ViewHash,
      _traceContext: TraceContext,
      _packageResolution: Map[PackageName, PackageId],
      _getEngineAbortStatus: GetEngineAbortStatus,
  ): EitherT[
    Future,
    DAMLeError,
    (LfVersionedTransaction, TransactionMetadata, LfKeyResolver),
  ] =
    fail("Reinterpret should not be called by this test case.")

  def viewsWithNoInputKeys(
      rootViews: Seq[FullTransactionViewTree]
  ): NonEmpty[Seq[(FullTransactionViewTree, Seq[(TransactionView, LfKeyResolver)])]] =
    NonEmptyUtil.fromUnsafe(rootViews.map { viewTree =>
      // Include resolvers for all the subviews
      val resolvers =
        viewTree.view.allSubviewsWithPosition(viewTree.viewPosition).map { case (view, _viewPos) =>
          view -> (Map.empty: LfKeyResolver)
        }
      (viewTree, resolvers)
    })

  val transactionTreeFactory: TransactionTreeFactoryImpl = {
    TransactionTreeFactoryImpl(
      ExampleTransactionFactory.submittingParticipant,
      factory.domainId,
      testedProtocolVersion,
      factory.cryptoOps,
      loggerFactory,
    )
  }

  object dummyAuthenticator extends SerializableContractAuthenticator {
    override def authenticate(contract: SerializableContract): Either[String, Unit] = Right(())
    override def verifyMetadata(
        contract: SerializableContract,
        metadata: ContractMetadata,
    ): Either[String, Unit] = Right(())
  }

  def check(
      mcc: ModelConformanceChecker,
      views: NonEmpty[Seq[(FullTransactionViewTree, Seq[(TransactionView, LfKeyResolver)])]],
      ips: TopologySnapshot = factory.topologySnapshot,
  ): EitherT[Future, ErrorWithSubTransaction, Result] = {
    val rootViewTrees = views.map(_._1)
    val commonData = TransactionProcessingSteps.tryCommonData(rootViewTrees)
    val keyResolvers = views.forgetNE.flatMap { case (_vt, resolvers) => resolvers }.toMap
    mcc
      .check(
        rootViewTrees,
        keyResolvers,
        RequestCounter(0),
        ips,
        commonData,
        getEngineAbortStatus = () => EngineAbortStatus.notAborted,
      )
      .failOnShutdown
  }

  val packageName: LfPackageName = PackageName.assertFromString("package-name")
  val packageVersion: LfPackageVersion = LfPackageVersion.assertFromString("1.0.0")
  val packageMetadata: PackageMetadata = PackageMetadata(packageName, packageVersion, None)
  val genPackage: GenPackage[Expr] =
    GenPackage(Map.empty, Set.empty, LanguageVersion.default, packageMetadata)
  val packageResolver: PackageResolver = pkgId =>
    traceContext => Future.successful(Some(genPackage))

  "A model conformance checker" when {
    val relevantExamples = factory.standardHappyCases.filter {
      // If the transaction is empty there is no transaction view message. Therefore, the checker is not invoked.
      case factory.EmptyTransaction => false
      case _ => true
    }

    forEvery(relevantExamples) { example =>
      s"checking $example" must {

        val sut =
          new ModelConformanceChecker(
            reinterpret(example),
            validateContractOk,
            transactionTreeFactory,
            submittingParticipant,
            dummyAuthenticator,
            packageResolver,
            loggerFactory,
          )

        "yield the correct result" in {
          for {
            result <- valueOrFail(
              check(sut, viewsWithNoInputKeys(example.rootTransactionViewTrees))
            )(s"model conformance check for root views")
          } yield {
            val Result(transactionId, absoluteTransaction) = result
            transactionId should equal(example.transactionId)
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
                val Result(transactionId, absoluteTransaction) = result
                transactionId should equal(example.transactionId)
                absoluteTransaction.metadata.ledgerTime should equal(factory.ledgerTime)
              }
            }
            .map(_ => succeed)
        }
      }
    }

    "transaction id is inconsistent" must {
      val sut = new ModelConformanceChecker(
        failOnReinterpret,
        validateContractOk,
        transactionTreeFactory,
        submittingParticipant,
        dummyAuthenticator,
        packageResolver,
        loggerFactory,
      )

      val singleCreate = factory.SingleCreate(seed = ExampleTransactionFactory.lfHash(0))
      val viewTreesWithInconsistentTransactionIds = Seq(
        factory.MultipleRootsAndViewNestings.rootTransactionViewTrees.headOption.value,
        singleCreate.rootTransactionViewTrees.headOption.value,
      )

      "yield an error" in {
        assertThrows[IllegalArgumentException] {
          check(sut, viewsWithNoInputKeys(viewTreesWithInconsistentTransactionIds))
        }
      }
    }

    "reinterpretation fails" must {
      import pprint.Tree.Apply

      // Without this, if the test fails, a NullPointerException shows up related to viewHash.pretty()
      val mockViewHash = mock[ViewHash]
      when(mockViewHash.pretty).thenAnswer(new Pretty[ViewHash] {
        override def treeOf(t: ViewHash): Tree = Apply("[ViewHash]", Seq.empty.iterator)
      })

      val error = DAMLeError(EngineError(mock[engine.Error]), mockViewHash)

      val sut = new ModelConformanceChecker(
        (_, _, _, _, _, _, _, _, _, _, _) =>
          EitherT.leftT[Future, (LfVersionedTransaction, TransactionMetadata, LfKeyResolver)](
            error
          ),
        validateContractOk,
        transactionTreeFactory,
        submittingParticipant,
        dummyAuthenticator,
        packageResolver,
        loggerFactory,
      )
      val example = factory.MultipleRootsAndViewNestings

      def countLeaves(views: NonEmpty[Seq[TransactionView]]): Int =
        views.foldLeft(0)((count, view) => {
          NonEmpty.from(view.subviews.unblindedElements) match {
            case Some(subviewsNE) => count + countLeaves(subviewsNE)
            case None => count + 1
          }
        })
      val nbLeafViews = countLeaves(NonEmptyUtil.fromUnsafe(example.rootViews))

      "yield an error" in {
        for {
          failure <- leftOrFail(
            check(
              sut,
              viewsWithNoInputKeys(example.rootTransactionViewTrees),
            )
          )("reinterpretation fails")
        } yield failure.errors shouldBe Seq.fill(nbLeafViews)(error) // One error per leaf
      }
    }

    "contract upgrading is enabled" should {

      val example: factory.UpgradedSingleExercise = factory.UpgradedSingleExercise(lfHash(0))

      "the choice package may differ from the contract package" in {

        val sut =
          new ModelConformanceChecker(
            reinterpret(example),
            validateContractOk,
            transactionTreeFactory,
            submittingParticipant,
            dummyAuthenticator,
            packageResolver,
            loggerFactory,
          )

        valueOrFail(
          check(sut, viewsWithNoInputKeys(example.rootTransactionViewTrees))
        )(s"failed to find upgraded contract").map(_ => succeed)

      }

    }

    "differences in the reconstructed transaction must yield an error" should {
      import ExampleTransactionFactory.*
      "subview missing" in {
        val subviewMissing = factory.SingleExercise(lfHash(0))
        val reinterpreted = transaction(
          Seq(0),
          subviewMissing.reinterpretedNode.copy(children = ImmArray(LfNodeId(1))),
          fetchNode(
            subviewMissing.contractId,
            actingParties = Set(submitter),
            signatories = Set(submitter, extra),
          ),
        )
        val sut = new ModelConformanceChecker(
          (_, _, _, _, _, _, _, _, _, _, _) =>
            EitherT.pure[Future, DAMLeError](
              (reinterpreted, subviewMissing.metadata, subviewMissing.keyResolver)
            ),
          validateContractOk,
          transactionTreeFactory,
          submittingParticipant,
          dummyAuthenticator,
          packageResolver,
          loggerFactory,
        )
        for {
          result <- leftOrFail(
            check(sut, viewsWithNoInputKeys(subviewMissing.rootTransactionViewTrees))
          )("detect missing subview")
        } yield result.errors.forgetNE.loneElement shouldBe a[TransactionTreeError]
      }

      /* TODO(#3202) further error cases to test:
       * - extra subview
       * - input contract not declared
       * - extra input contract
       * - input contract with wrong contract data
       * - missing created contract
       * - extra created contract
       * - wrong discriminator of created contract
       * - wrong unicum of created contract
       * - wrong data for created contract
       */
    }

    "a package (referenced by create) is not vetted by some participant" must {
      "yield an error" in {
        import ExampleTransactionFactory.*
        testVettingError(
          NonEmpty.from(factory.SingleCreate(lfHash(0)).rootTransactionViewTrees).value,
          // The package is not vetted for signatoryParticipant
          vettings = Seq(VettedPackages(submittingParticipant, None, Seq(packageId))),
          packageDependenciesLookup = new TestPackageResolver(Right(Set.empty)),
          expectedError = UnvettedPackages(Map(signatoryParticipant -> Set(packageId))),
        )
      }
    }

    "a package (referenced by key lookup) is not vetted by some participant" must {
      "yield an error" in {
        import ExampleTransactionFactory.*

        val key = defaultGlobalKey
        val maintainers = Set(submitter)
        val view = factory.view(
          lookupByKeyNode(key, Set(submitter), None),
          0,
          Set.empty,
          Seq.empty,
          Seq.empty,
          Map(key -> FreeKey(maintainers)),
          None,
          isRoot = true,
        )
        val viewTree = factory.rootTransactionViewTree(view)

        testVettingError(
          NonEmpty(Seq, viewTree),
          // The package is not vetted for submittingParticipant
          vettings = Seq.empty,
          packageDependenciesLookup = new TestPackageResolver(Right(Set.empty)),
          expectedError = UnvettedPackages(Map(submittingParticipant -> Set(key.packageId.value))),
        )
      }
    }

    def testVettingError(
        rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
        vettings: Seq[VettedPackages],
        packageDependenciesLookup: PackageDependencyResolverUS,
        expectedError: UnvettedPackages,
    ): Future[Assertion] = {
      import ExampleTransactionFactory.*

      val sut = new ModelConformanceChecker(
        reinterpret = failOnReinterpret,
        validateContract = validateContractOk,
        transactionTreeFactory = transactionTreeFactory,
        participantId = submittingParticipant,
        serializableContractAuthenticator = dummyAuthenticator,
        packageResolver = packageResolver,
        loggerFactory,
      )

      val snapshot = TestingIdentityFactory(
        TestingTopology(
        ).withTopology(Map(submitter -> submittingParticipant, observer -> signatoryParticipant))
          .withPackages(vettings.map(vetting => vetting.participantId -> vetting.packageIds).toMap),
        loggerFactory,
        TestDomainParameters.defaultDynamic,
      ).topologySnapshot(packageDependencyResolver = packageDependenciesLookup)

      for {
        error <- check(sut, viewsWithNoInputKeys(rootViewTrees), snapshot).value
      } yield error shouldBe Left(
        ErrorWithSubTransaction(
          NonEmpty(Seq, expectedError),
          None,
          Seq.empty,
        )
      )
    }

    "a package is not found in the package store" must {
      "yield an error" in {
        import ExampleTransactionFactory.*
        testVettingError(
          NonEmpty.from(factory.SingleCreate(lfHash(0)).rootTransactionViewTrees).value,
          vettings = Seq(
            VettedPackages(submittingParticipant, None, Seq(packageId)),
            VettedPackages(signatoryParticipant, None, Seq(packageId)),
          ),
          // Submitter participant is unable to lookup dependencies.
          // Therefore, the validation concludes that the package is not in the store
          // and thus that the package is not vetted.
          packageDependenciesLookup = new TestPackageResolver(Left(packageId)),
          expectedError = UnvettedPackages(Map(submittingParticipant -> Set(packageId))),
        )
      }
    }
  }

  class TestPackageResolver(result: Either[PackageId, Set[PackageId]])
      extends PackageDependencyResolverUS {
    import cats.syntax.either.*
    override def packageDependencies(packageId: PackageId)(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] = {
      result.toEitherT
    }
  }

}
