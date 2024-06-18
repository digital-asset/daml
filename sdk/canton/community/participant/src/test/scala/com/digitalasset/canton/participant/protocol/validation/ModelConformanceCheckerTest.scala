// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.syntax.parallel.*
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{PackageId, PackageName}
import com.daml.lf.engine.Error as LfError
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.value.Value
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.{HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FreeKey,
  FullTransactionViewTree,
  SerializableKeyResolution,
  TransactionView,
}
import com.digitalasset.canton.logging.NamedLoggingContext
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
import com.digitalasset.canton.topology.transaction.VettedPackages
import com.digitalasset.canton.topology.{TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.LfTransactionBuilder.{defaultKeyPackageName, defaultTemplateId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  LfCommand,
  LfKeyResolver,
  LfPackageId,
  LfPartyId,
  RequestCounter,
}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import pprint.Tree

import java.time.Duration
import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Left

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
    ] = {
      fail("Reinterpret should not be called by this test case.")
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

  val transactionTreeFactory: TransactionTreeFactoryImpl = {
    TransactionTreeFactoryImpl(
      ExampleTransactionFactory.submitterParticipant,
      factory.domainId,
      testedProtocolVersion,
      factory.cryptoOps,
      uniqueContractKeys = true,
      loggerFactory,
    )
  }

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
    mcc.check(rootViewTrees, keyResolvers, RequestCounter(0), ips, commonData)
  }

  private val pkg = Ast.GenPackage[Ast.Expr](
    modules = Map.empty,
    directDeps = Set.empty,
    languageVersion = LanguageVersion.default,
    metadata = None,
    isUtilityPackage = true,
  )
  private val packageResolver: PackageResolver = _ => _ => Future.successful(Some(pkg))

  val preReinterpretationPackageIds: PackageIdsOfView =
    if (testedProtocolVersion >= ProtocolVersion.v6)
      packageIdsOfActionDescription
    else
      legacyPackageIdsOfView

  def buildUnderTest(reinterpretCommand: HasReinterpret): ModelConformanceChecker =
    new ModelConformanceChecker(
      reinterpretCommand,
      validateContractOk,
      transactionTreeFactory,
      submitterParticipant,
      dummyAuthenticator,
      packageResolver,
      preReinterpretationPackageIds,
      checkUsedPackages = (testedProtocolVersion >= ProtocolVersion.v6),
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
        ] = {
          EitherT.leftT(lfError)
        }
      })

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
          ] = {
            EitherT.pure[Future, LfError](
              (reinterpreted, subviewMissing.metadata, subviewMissing.keyResolver, Set.empty)
            )
          }
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

    "a package (referenced by create) is not vetted by some participant" must {
      "yield an error" in {
        import ExampleTransactionFactory.*
        testVettingError(
          NonEmpty.from(factory.SingleCreate(lfHash(0)).rootTransactionViewTrees).value,
          // The package is not vetted for signatoryParticipant
          vettings = Seq(VettedPackages(submitterParticipant, Seq(packageId))),
          packageDependenciesLookup = _ => EitherT.rightT(Set()),
          expectedError = UnvettedPackages(Map(signatoryParticipant -> Set(packageId))),
        )
      }
    }

    "a package (referenced by exercise) is not vetted by some participant" must {
      "yield an error" in {
        import ExampleTransactionFactory.*
        val exercise = factory.SingleExercise(lfHash(0))
        val view = factory.view(
          node = exerciseNode(exercise.contractId, signatories = Set(signatory)),
          viewIndex = 0,
          consumed = Set.empty,
          coreInputs = exercise.inputContracts.values.toSeq,
          created = Seq.empty,
          resolvedKeys = Map.empty,
          seed = Some(exercise.seed),
          isRoot = true,
          Set.empty,
        )
        val viewTree = factory.rootTransactionViewTree(view)

        testVettingError(
          NonEmpty(Seq, viewTree),
          // The package is not vetted for submitterParticipant
          vettings = Seq.empty,
          packageDependenciesLookup = _ => EitherT.rightT(Set()),
          expectedError = UnvettedPackages(
            Map(
              submitterParticipant -> Set(exercise.contractInstance.unversioned.template.packageId)
            )
          ),
        )
      }
    }

    def testVettingError(
        rootViewTrees: NonEmpty[Seq[FullTransactionViewTree]],
        vettings: Seq[VettedPackages],
        packageDependenciesLookup: PackageId => EitherT[Future, PackageId, Set[PackageId]],
        expectedError: UnvettedPackages,
    ): Future[Assertion] = {
      import ExampleTransactionFactory.*

      val sut = buildUnderTest(failOnReinterpret)

      val snapshot = TestingIdentityFactory(
        TestingTopology(
        ).withTopology(Map(submitter -> submitterParticipant, observer -> signatoryParticipant))
          .withPackages(vettings),
        loggerFactory,
        TestDomainParameters.defaultDynamic,
      ).topologySnapshot(packageDependencies = packageDependenciesLookup)

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
            VettedPackages(submitterParticipant, Seq(packageId)),
            VettedPackages(signatoryParticipant, Seq(packageId)),
          ),
          // Submitter participant is unable to lookup dependencies.
          // Therefore, the validation concludes that the package is not in the store
          // and thus that the package is not vetted.
          packageDependenciesLookup = EitherT.leftT(_),
          expectedError = UnvettedPackages(Map(submitterParticipant -> Set(packageId))),
        )
      }
    }

    "package ids used for pre-reinterpretation vetting" must {

      val seed: LfHash = lfHash(0)

      def mkPackageContractInstance(i: Int): (PackageId, SerializableContract) = {
        val packageId = PackageId.assertFromString(s"package-$i")
        val lfH = lfHash(i)
        val cH = com.digitalasset.canton.crypto.Hash
          .digest(HashPurpose.Unicum, lfH.bytes.toByteString, HashAlgorithm.Sha256)
        val cId = NonAuthenticatedContractId.fromDiscriminator(lfH, Unicum(cH))
        val contract =
          asSerializable(cId, contractInstance(templateId = defaultTemplateId.copy(packageId)))
        packageId -> contract
      }

      val (p1, c1) = mkPackageContractInstance(1)
      val (p2, c2) = mkPackageContractInstance(2)
      val (p3, c3) = mkPackageContractInstance(3)
      val (p4, c4) = mkPackageContractInstance(4)
      val (_, c5) = mkPackageContractInstance(5)
      val (p6, _) = mkPackageContractInstance(6)

      def keyOf(c: SerializableContract): LfGlobalKey = LfGlobalKey.assertBuild(
        c.contractInstance.unversioned.template,
        Value.ValueUnit,
        defaultKeyPackageName,
      )

      def mkView(
          node: LfActionNode,
          coreInputs: Seq[SerializableContract] = Seq.empty,
          created: Seq[SerializableContract] = Seq.empty,
          resolvedKeys: Map[LfGlobalKey, SerializableKeyResolution] = Map.empty,
          packagePreference: Set[LfPackageId] = Set.empty,
          seedO: Option[LfHash] = Some(seed),
      ) = {
        factory.view(
          node = node,
          viewIndex = 0,
          consumed = Set.empty,
          coreInputs = coreInputs,
          created = created,
          resolvedKeys = resolvedKeys,
          seed = seedO,
          isRoot = true,
          packagePreference = packagePreference,
        )
      }

      "support legacy package id computation" in {
        val view = mkView(
          node = exerciseNode(c1.contractId, signatories = Set(submitter)),
          created = Seq(c2),
          coreInputs = Seq(c3),
          resolvedKeys = Map(keyOf(c4) -> FreeKey(Set(submitter))(c4.contractInstance.version)),
        )
        val actual = legacyPackageIdsOfView(view, implicitly[NamedLoggingContext])
        actual shouldBe Set(p2, p3, p4)
      }

      "support upgraded creations" in {
        val view = mkView(
          node = createNode(c1.contractId, c1.contractInstance, signatories = Set(submitter)),
          created = Seq(c1),
        )
        val actual = packageIdsOfActionDescription(view, implicitly[NamedLoggingContext])
        actual shouldBe Set(p1)
      }

      "support upgraded executions" in {
        val view = mkView(
          node = exerciseNode(
            targetCoid = c1.contractId,
            templateId = c2.contractInstance.unversioned.template,
            signatories = Set(submitter),
          ),
          created = Seq(c3),
          coreInputs = Seq(c4),
          resolvedKeys = Map(keyOf(c5) -> FreeKey(Set(submitter))(LfTransactionVersion.maxVersion)),
          packagePreference = Set(p6),
        )
        val actual = packageIdsOfActionDescription(view, implicitly[NamedLoggingContext])
        actual shouldBe Set(p2, p6)
      }

      "support upgraded lookupByKey" in {
        val node =
          lookupByKeyNode(keyOf(c2), maintainers = Set(submitter), resolution = Some(c1.contractId))
        val view = mkView(node, seedO = None)
        val actual = packageIdsOfActionDescription(view, implicitly[NamedLoggingContext])
        actual shouldBe Set(p2)
      }

      if (testedProtocolVersion >= ProtocolVersion.v6) {
        "support upgraded fetch" in {
          // Here the templateId is coming from the (possibly upgraded) package the fetch is targeted at
          val node = fetchNode(
            c1.contractId,
            signatories = Set(submitter),
            actingParties = Set(signatory),
            templateId = c2.contractInstance.unversioned.template,
          )
          val view = mkView(node, coreInputs = Seq(c1), seedO = None)
          val actual = packageIdsOfActionDescription(view, implicitly[NamedLoggingContext])
          actual shouldBe Set(p2)
        }
      }

    }

    if (testedProtocolVersion >= ProtocolVersion.v6) {

      "post-reinterpretation used package vetting" must {

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
                    submitterParticipant -> Set(unexpectedPackageId),
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
    }
  }
}
