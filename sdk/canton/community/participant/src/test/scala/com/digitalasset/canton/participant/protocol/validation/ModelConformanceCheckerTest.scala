// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{PackageId, PackageName}
import com.daml.lf.engine.Error as LfError
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.data.{CantonTimestamp, FullTransactionViewTree, TransactionView}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactoryImpl
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.*
import com.digitalasset.canton.participant.protocol.{
  SerializableContractAuthenticator,
  TransactionProcessingSteps,
}
import com.digitalasset.canton.participant.store.ContractLookupAndVerification
import com.digitalasset.canton.participant.util.DAMLe.{HasReinterpret, PackageResolver}
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
import com.digitalasset.canton.topology.transaction.VettedPackages
import com.digitalasset.canton.topology.{TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, LfCommand, LfKeyResolver, LfPartyId, RequestCounter}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import pprint.Tree

import java.time.Duration
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}

class ModelConformanceCheckerTest extends AsyncWordSpec with BaseTest {

  implicit val ec: ExecutionContext = directExecutionContext

  val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  val sequencerTimestamp: CantonTimestamp = CantonTimestamp.ofEpochSecond(0)

  val ledgerTimeRecordTimeTolerance: Duration = Duration.ofSeconds(10)

  def validateContractOk(
      @unused _contract: SerializableContract,
      @unused _context: TraceContext,
  ): EitherT[Future, ContractValidationFailure, Unit] = EitherT.pure(())

  def reinterpretExample(
      example: ExampleTransaction,
      usedPackages: Set[PackageId] = Set.empty,
  ): HasReinterpret = new HasReinterpret {
    override def reinterpret(
        contracts: ContractLookupAndVerification,
        submitters: Set[LfPartyId],
        command: LfCommand,
        ledgerTime: CantonTimestamp,
        submissionTime: CantonTimestamp,
        rootSeed: Option[LfHash],
        packageResolution: Map[PackageName, PackageId],
        expectFailure: Boolean,
    )(traceContext: TraceContext): EitherT[
      Future,
      LfError,
      (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
    ] = {
      ledgerTime shouldEqual factory.ledgerTime
      submissionTime shouldEqual factory.submissionTime

      val (_, (reinterpretedTx, metadata, keyResolver), _) =
        example.reinterpretedSubtransactions.find { case (viewTree, (tx, md, _), _) =>
          viewTree.viewParticipantData.rootAction().command == command &&
          // Commands are otherwise not sufficiently unique (whereas with nodes, we can produce unique nodes, e.g.
          // based on LfNodeCreate.agreementText not part of LfCreateCommand.
          rootSeed == md.seeds.get(tx.roots(0))
        }.value

      EitherT.rightT[Future, LfError]((reinterpretedTx, metadata, keyResolver, usedPackages))
    }
  }

  val failOnReinterpret: HasReinterpret = new HasReinterpret {
    override def reinterpret(
        contracts: ContractLookupAndVerification,
        submitters: Set[LfPartyId],
        command: LfCommand,
        ledgerTime: CantonTimestamp,
        submissionTime: CantonTimestamp,
        rootSeed: Option[LfHash],
        packageResolution: Map[PackageName, PackageId],
        expectFailure: Boolean,
    )(traceContext: TraceContext): EitherT[
      Future,
      LfError,
      (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
    ] =
      fail("Reinterpret should not be called by this test case.")
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
      ExampleTransactionFactory.submitterParticipant,
      factory.domainId,
      testedProtocolVersion,
      factory.cryptoOps,
      uniqueContractKeys = true,
      loggerFactory,
    )

  object dummyAuthenticator extends SerializableContractAuthenticator {
    override private[protocol] def authenticate(
        authenticationPurpose: SerializableContractAuthenticator.AuthenticationPurpose,
        serializableContract: SerializableContract,
    ): Either[String, Unit] =
      Right(())
  }

  def check(
      mcc: ModelConformanceChecker,
      views: NonEmpty[Seq[(FullTransactionViewTree, Seq[(TransactionView, LfKeyResolver)])]],
      ips: TopologySnapshot = factory.topologySnapshot,
  ): EitherT[Future, ErrorWithSubTransaction, Result] = {
    val rootViewTrees = views.map(_._1)
    val commonData = TransactionProcessingSteps.tryCommonData(rootViewTrees)
    val keyResolvers = views.forgetNE.flatMap { case (_, resolvers) => resolvers }.toMap
    mcc
      .check(rootViewTrees, keyResolvers, RequestCounter(0), ips, commonData)
      .failOnShutdown
  }

  private val pkg = Ast.GenPackage[Ast.Expr](
    modules = Map.empty,
    directDeps = Set.empty,
    languageVersion = LanguageVersion.default,
    metadata = None,
    isUtilityPackage = true,
  )
  private val packageResolver: PackageResolver = _ => _ => Future.successful(Some(pkg))

  def buildUnderTest(reinterpretCommand: HasReinterpret): ModelConformanceChecker =
    new ModelConformanceChecker(
      reinterpretCommand,
      validateContractOk,
      transactionTreeFactory,
      submitterParticipant,
      dummyAuthenticator,
      packageResolver,
      loggerFactory,
    )

  "A model conformance checker" when {
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

      val sut = buildUnderTest(failOnReinterpret)

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

      val lfError = mock[LfError]
      val error = DAMLeError(lfError, mockViewHash)

      val sut = buildUnderTest(new HasReinterpret {
        override def reinterpret(
            contracts: ContractLookupAndVerification,
            submitters: Set[LfPartyId],
            command: LfCommand,
            ledgerTime: CantonTimestamp,
            submissionTime: CantonTimestamp,
            rootSeed: Option[LfHash],
            packageResolution: Map[PackageName, PackageId],
            expectFailure: Boolean,
        )(traceContext: TraceContext): EitherT[
          Future,
          LfError,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
        ] =
          EitherT.leftT(lfError)
      })

      val example = factory.MultipleRootsAndViewNestings

      def countLeaves(views: NonEmpty[Seq[TransactionView]]): Int =
        views.foldLeft(0) { (count, view) =>
          NonEmpty.from(view.subviews.unblindedElements) match {
            case Some(subviewsNE) => count + countLeaves(subviewsNE)
            case None => count + 1
          }
        }

      val nbLeafViews = countLeaves(NonEmptyUtil.fromUnsafe(example.rootViews))

      "yield an error" in {
        for {
          failure <- leftOrFail(
            check(
              sut,
              viewsWithNoInputKeys(example.rootTransactionViewTrees),
            )
          )("reinterpretation fails")
        } yield failure.errors.size shouldBe Seq.fill(nbLeafViews)(error).size // One error per leaf
      }
    }

    "checking an upgraded contract" should {

      val example: factory.UpgradedSingleExercise = factory.UpgradedSingleExercise(lfHash(0))

      "allow the choice package may differ from the contract package" in {

        val sut = buildUnderTest(reinterpretExample(example))

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

        val sut = buildUnderTest(new HasReinterpret {
          override def reinterpret(
              contracts: ContractLookupAndVerification,
              submitters: Set[LfPartyId],
              command: LfCommand,
              ledgerTime: CantonTimestamp,
              submissionTime: CantonTimestamp,
              rootSeed: Option[LfHash],
              packageResolution: Map[PackageName, PackageId],
              expectFailure: Boolean,
          )(traceContext: TraceContext): EitherT[
            Future,
            LfError,
            (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
          ] =
            EitherT.pure[Future, LfError](
              (reinterpreted, subviewMissing.metadata, subviewMissing.keyResolver, Set.empty)
            )
        })

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

    // TODO(#21671): Unit test tri-state vetting
    "package vetting" must {

      import ExampleTransactionFactory.*

      def testVetting(
          example: ExampleTransaction,
          usedPackages: Set[PackageId],
          vettings: Seq[VettedPackages],
          unknownPackage: Option[PackageId],
          expectedErrorO: Option[PackageError],
      ): Future[Assertion] = {

        val sut = buildUnderTest(reinterpretExample(example, usedPackages))
        val rootViewTrees = NonEmpty.from(example.rootTransactionViewTrees).value

        val snapshot = TestingIdentityFactory(
          TestingTopology(
          ).withTopology(
            Map(
              submitter -> submitterParticipant,
              signatory -> signatoryParticipant,
            )
          ).withPackages(vettings),
          loggerFactory,
          TestDomainParameters.defaultDynamic,
        ).topologySnapshot(packageDependencies = new TestPackageResolver(unknownPackage))

        for {
          error <- check(sut, viewsWithNoInputKeys(rootViewTrees), snapshot).value
        } yield error match {
          case Right(_) if expectedErrorO.isEmpty => succeed
          case Left(ErrorWithSubTransaction(actual, _, _)) =>
            actual.forgetNE shouldBe expectedErrorO.toList
          case other => fail(s"Did not expect $other")
        }
      }

      "succeed if all package are vetted" in {

        val example: factory.UpgradedSingleExercise = factory.UpgradedSingleExercise(lfHash(0))

        val usedPackageId = example.upgradedTemplateId.packageId
        val referencedPackageId = example.contractInstance.unversioned.template.packageId

        testVetting(
          example,
          usedPackages = Set(usedPackageId),
          vettings = Seq(
            VettedPackages(signatoryParticipant, Seq(usedPackageId, referencedPackageId)),
            VettedPackages(submitterParticipant, Seq(usedPackageId, referencedPackageId)),
          ),
          unknownPackage = None,
          expectedErrorO = None, // no error expected,
        )

      }

      "fail if an un-vetted package is used" in {

        val unexpectedPackageId = PackageId.assertFromString("unexpected-pkg")
        val example = factory.SingleCreate(seed = factory.deriveNodeSeed(0))
        val expected =
          UnvettedPackages(
            Map(
              submitterParticipant -> Set(unexpectedPackageId)
            )
          )
        testVetting(
          example,
          usedPackages = Set(unexpectedPackageId),
          vettings = Seq.empty,
          unknownPackage = None,
          expectedErrorO = Some(expected),
        )

      }

      "fail if an un-vetted contract package is referenced" in {

        val example: factory.UpgradedSingleExercise = factory.UpgradedSingleExercise(lfHash(0))
        val contractPackageId = example.contractInstance.unversioned.template.packageId
        val expected = UnknownPackages(Map(submitterParticipant -> Set(contractPackageId)))

        testVetting(
          example,
          usedPackages = Set.empty,
          vettings = Seq.empty,
          unknownPackage = None,
          expectedErrorO = Some(expected),
        )

      }

      "fail if a package is not vetted by all participants" in {
        val example = factory.SingleExercise(lfHash(0))
        testVetting(
          example,
          usedPackages = Set(packageId),
          vettings = Seq(VettedPackages(signatoryParticipant, Seq(packageId))),
          unknownPackage = None,
          expectedErrorO = Some(
            UnvettedPackages(
              Map(submitterParticipant -> Set(packageId))
            )
          ),
        )
      }

      "fail if a package is not found in the package store" in {
        testVetting(
          factory.SingleCreate(lfHash(0)),
          usedPackages = Set(packageId),
          vettings = Seq(
            VettedPackages(submitterParticipant, Seq(packageId)),
            VettedPackages(signatoryParticipant, Seq(packageId)),
          ),
          unknownPackage = Some(packageId),
          expectedErrorO = Some(PackageNotFound(submitterParticipant, Set(packageId))),
        )
      }
    }
  }

  class TestPackageResolver(unknown: Option[PackageId]) extends PackageDependencyResolverUS {
    override def packageDependencies(packages: List[PackageId])(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, PackageId, Set[PackageId]] =
      EitherT.fromEither(unknown.toLeft(packages.toSet))
  }

}
