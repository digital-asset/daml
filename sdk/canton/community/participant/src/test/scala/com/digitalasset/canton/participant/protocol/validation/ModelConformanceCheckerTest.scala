// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.data.*
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
import com.digitalasset.canton.participant.store.ContractLookupAndVerification
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.participant.util.DAMLe.{EngineError, HasReinterpret, PackageResolver}
import com.digitalasset.canton.protocol.ExampleTransactionFactory.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.store.PackageDependencyResolverUS
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
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.language.Ast.{Expr, GenPackage, PackageMetadata}
import com.digitalasset.daml.lf.language.LanguageVersion
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
      @unused _getEngineAbortStatus: GetEngineAbortStatus,
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
        getEngineAbortStatus: GetEngineAbortStatus,
    )(implicit traceContext: TraceContext): EitherT[
      Future,
      DAMLe.ReinterpretationError,
      (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
    ] = {
      ledgerTime shouldEqual factory.ledgerTime
      submissionTime shouldEqual factory.submissionTime

      val (_, (reinterpretedTx, metadata, keyResolver), _) = {
        // The code below assumes that for reinterpretedSubtransactions the combination
        // of command and root-seed wil be unique. In the examples used to date this is
        // the case. A limitation of this approach is that only one LookupByKey transaction
        // can be returned as the root seed is unset in the ReplayCommand.
        example.reinterpretedSubtransactions.find { case (viewTree, (tx, md, _), _) =>
          viewTree.viewParticipantData.rootAction.command == command &&
          md.seeds.get(tx.roots(0)) == rootSeed
        }.value
      }

      EitherT.rightT((reinterpretedTx, metadata, keyResolver, usedPackages))
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
        getEngineAbortStatus: GetEngineAbortStatus,
    )(implicit traceContext: TraceContext): EitherT[
      Future,
      DAMLe.ReinterpretationError,
      (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
    ] = fail("Reinterpret should not be called by this test case.")
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
    val keyResolvers = views.forgetNE.flatMap { case (_, resolvers) => resolvers }.toMap
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
  val packageResolver: PackageResolver = _ => _ => Future.successful(Some(genPackage))

  def buildUnderTest(reinterpretCommand: HasReinterpret): ModelConformanceChecker =
    new ModelConformanceChecker(
      reinterpretCommand,
      validateContractOk,
      transactionTreeFactory,
      submittingParticipant,
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
      val error = EngineError(lfError)

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
            getEngineAbortStatus: GetEngineAbortStatus,
        )(implicit traceContext: TraceContext): EitherT[
          Future,
          DAMLe.ReinterpretationError,
          (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
        ] = EitherT.leftT(error)
      })

      val example = factory.MultipleRootsAndViewNestings

      "yield an error" in {
        val exampleTree = example.transactionViewTree0
        for {
          failure <- leftOrFail(
            check(
              sut,
              viewsWithNoInputKeys(Seq(exampleTree)),
            )
          )("reinterpretation fails")
        } yield {
          failure.errors.forgetNE shouldBe Seq(
            DAMLeError(EngineError(lfError), exampleTree.viewHash)
          )
        }
      }
    }

    "checking an upgraded contract" should {

      val example: factory.UpgradedSingleExercise = factory.UpgradedSingleExercise(lfHash(0))

      "allow the choice package to differ from the contract package" in {

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
              getEngineAbortStatus: GetEngineAbortStatus,
          )(implicit traceContext: TraceContext): EitherT[
            Future,
            DAMLe.ReinterpretationError,
            (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
          ] = EitherT.pure(
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

    "reinterpretation used package vetting" must {

      "fail conformance if an un-vetted package is used" in {
        val unexpectedPackageId = PackageId.assertFromString("unexpected-pkg")
        val example = factory.SingleCreate(seed = factory.deriveNodeSeed(0))
        val sut =
          buildUnderTest(reinterpretExample(example, usedPackages = Set(unexpectedPackageId)))
        val expected = Left(
          ErrorWithSubTransaction(
            NonEmpty(
              Seq,
              UnvettedPackages(
                Map(
                  submittingParticipant -> Set(unexpectedPackageId),
                  observerParticipant -> Set(unexpectedPackageId),
                )
              ),
            ),
            None,
            Seq.empty,
          )
        )
        for {
          actual <- check(sut, viewsWithNoInputKeys(example.rootTransactionViewTrees)).value
        } yield {
          actual shouldBe expected
        }
      }
    }

    "a package is not found in the package store" must {

      "fail with an engine error" in {

        val missingPackageId = PackageId.assertFromString("missing-pkg")
        val example = factory.SingleCreate(seed = factory.deriveNodeSeed(0))
        val engineError =
          EngineError(new LfError.Package(LfError.Package.MissingPackage(missingPackageId)))
        val sut =
          buildUnderTest(new HasReinterpret {
            override def reinterpret(
                contracts: ContractLookupAndVerification,
                submitters: Set[LfPartyId],
                command: LfCommand,
                ledgerTime: CantonTimestamp,
                submissionTime: CantonTimestamp,
                rootSeed: Option[LfHash],
                packageResolution: Map[PackageName, PackageId],
                expectFailure: Boolean,
                getEngineAbortStatus: GetEngineAbortStatus,
            )(implicit traceContext: TraceContext): EitherT[
              Future,
              DAMLe.ReinterpretationError,
              (LfVersionedTransaction, TransactionMetadata, LfKeyResolver, Set[PackageId]),
            ] = EitherT.fromEither(Left(engineError))
          })
        val viewHash = example.transactionViewTrees.head.viewHash
        val expected = Left(
          ErrorWithSubTransaction(
            NonEmpty(
              Seq,
              DAMLeError(engineError, viewHash),
            ),
            None,
            Seq.empty,
          )
        )
        for {
          actual <- check(sut, viewsWithNoInputKeys(example.rootTransactionViewTrees)).value
        } yield {
          actual shouldBe expected
        }
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
