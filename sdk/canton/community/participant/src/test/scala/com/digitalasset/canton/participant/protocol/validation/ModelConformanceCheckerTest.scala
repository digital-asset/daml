// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import cats.implicits.toTraverseOps
import com.daml.ledger.javaapi.data.Command
import com.daml.ledger.javaapi.data.codegen.{Created, Update}
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.BaseTest.getResourcePath
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{LoggingConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.TestSalt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription
import com.digitalasset.canton.examples.java.cycle.Cycle
import com.digitalasset.canton.examples.java.paint.OfferToPaintHouseByPainter
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.protocol.EngineController.{
  EngineAbortStatus,
  GetEngineAbortStatus,
}
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentDataHelpers.TestValidator
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.ContractLookupError
import com.digitalasset.canton.participant.protocol.submission.{
  SeedGenerator,
  TransactionConfirmationRequestFactory,
  TransactionTreeFactory,
}
import com.digitalasset.canton.participant.protocol.validation.CheckOutcome.*
import com.digitalasset.canton.participant.protocol.validation.ModelConformanceChecker.{
  DAMLeError,
  ErrorWithSubTransaction,
  LazyAsyncReInterpretationMap,
  Result,
  ViewReconstructionError,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.configuration.EngineLoggingConfig
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.{ParticipantPermission, VettedPackage}
import com.digitalasset.canton.util.{ContractHasher, ContractValidator, RoseTree, TestEngine}
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfKeyResolver,
  LfPackageId,
  LfPartyId,
  LfVersioned,
  config,
}
import com.digitalasset.daml.lf
import com.digitalasset.daml.lf.data.Ref.FullReference
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.engine.Error.Interpretation.DamlException
import com.digitalasset.daml.lf.engine.{Error, Error as LfError}
import com.digitalasset.daml.lf.interpretation.Error.ContractNotFound
import com.digitalasset.daml.lf.transaction.{Node, SubmittedTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value
import monocle.macros.GenLens
import monocle.{Lens, Traversal}
import org.mockito.MockitoSugar
import org.scalatest.LoneElement.convertToCollectionLoneElementWrapper
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, EitherValues, OptionValues}

import java.lang.IllegalArgumentException
import scala.collection.convert.AsJavaExtensions
import scala.util.{Failure, Random, Success, Try}

class ModelConformanceCheckerTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with MockitoSugar
    with FailOnShutdown {

  import ModelConformanceCheckerTest.*

  private val keyResolver: LfKeyResolver = Map.empty
  private val getEngineAbortStatus: GetEngineAbortStatus = () => EngineAbortStatus.notAborted
  private val symbolicCrypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, ProcessingTimeout(), loggerFactory)
  private val pureCrypto = symbolicCrypto.pureCrypto
  private val seedGenerator = new SeedGenerator(pureCrypto)
  private val participantId: ParticipantId = DefaultTestIdentities.participant1
  private val mediatorGroup = MediatorGroupRecipient(NonNegativeInt.zero)
  private val modelConformanceExamplesPath: String = getResourcePath(
    "ModelConformanceExamples-1.0.0.dar"
  )
  private val testEngine = new TestEngine(Seq(CantonExamplesPath, modelConformanceExamplesPath))
  private val contractHasher = ContractHasher(testEngine.engine, testEngine.packageResolver)
  private val transactionTreeFactory: TransactionTreeFactory =
    TransactionConfirmationRequestFactory(
      participantId,
      DefaultTestIdentities.physicalSynchronizerId,
      wallClock,
    )(
      pureCrypto,
      contractHasher,
      seedGenerator,
      LoggingConfig(),
      loggerFactory,
    ).transactionTreeFactory

  private val exampleFactory = new ExampleFactory(testEngine)

  // All futures should evaluate (almost) immediately, as everything is in memory
  private val nonZeroDuration = config.NonNegativeDuration.ofSeconds(10)
  private def awaitFUS[R](fus: FutureUnlessShutdown[R]): R =
    nonZeroDuration.awaitUS("awaitFUS")(fus).failOnShutdown

  private def awaitEitherT[L, R](eitherT: EitherT[FutureUnlessShutdown, L, R]): Either[L, R] =
    awaitFUS(eitherT.value)

  private val partyParticipants: Map[LfPartyId, ParticipantId] = Map(
    DefaultTestIdentities.party1.toLf -> DefaultTestIdentities.participant1,
    DefaultTestIdentities.party2.toLf -> DefaultTestIdentities.participant2,
    DefaultTestIdentities.party3.toLf -> DefaultTestIdentities.participant3,
  )

  private def buildTopologySnapshotFor(example: Example): TopologySnapshot = {

    val vettedPackages = example.metadata.usedPackages.map(p => VettedPackage(p, None, None)).toSeq
    val informees = example.tx.informees
    val participants = informees.map(partyParticipants)

    val topology = informees.view.map { partyId =>
      val participantId = partyParticipants(partyId)
      partyId -> Map(participantId -> ParticipantPermission.Confirmation)
    }.toMap
    val packages = participants.view.map(pId => pId -> vettedPackages).toMap

    buildTopologySnapshot(topology, packages)
  }

  private def buildTopologySnapshot(
      topology: Map[LfPartyId, Map[ParticipantId, ParticipantPermission]],
      packages: Map[ParticipantId, Seq[VettedPackage]] = Map.empty,
  ): TopologySnapshot = {
    val testingIdentityFactory = TestingIdentityFactory(
      TestingTopology.from(
        topology = topology,
        packages = packages,
      ),
      loggerFactory,
      TestSynchronizerParameters.defaultDynamic,
      symbolicCrypto,
    )
    testingIdentityFactory.topologySnapshot()
  }

  private def buildUnderTest(
      contractValidator: ContractValidator = ContractValidator.AllowAll
  ): ModelConformanceChecker = {

    val damlE: DAMLe = new DAMLe(
      resolvePackage = testEngine.packageResolver,
      engine = testEngine.engine,
      engineLoggingConfig = EngineLoggingConfig(),
      loggerFactory = loggerFactory,
    )

    ModelConformanceChecker(
      participantId = participantId,
      damlE = damlE,
      transactionTreeFactory = transactionTreeFactory,
      contractValidator = contractValidator,
      packageResolver = testEngine.packageResolver,
      hashOps = symbolicCrypto.pureCrypto,
      loggerFactory = loggerFactory,
    )
  }

  "When provided with valid input" should {

    val underTest: ModelConformanceChecker = buildUnderTest()

    "pass create Cycle example" in {
      verifyExample(underTest, exampleFactory.createCycle())
    }

    "pass exercise Cycle example" in {
      verifyExample(underTest, exampleFactory.exerciseCycle())
    }

    "pass accept paint offer example" in {
      verifyExample(underTest, exampleFactory.acceptPaintOffer())
    }

    "pass multi reader example" in {
      verifyExample(underTest, exampleFactory.multiReaderLookup())
    }

    "pass multi create example" in {
      verifyExample(underTest, exampleFactory.multiReaderCreate())
    }

    "pass with projected views" in {

      def projections(
          filter: Set[Int]
      )(fvt: FullTransactionViewTree): Seq[FullTransactionViewTree] = {
        val views = for {
          (rootView, index) <- fvt.tree.rootViews.unblindedElementsWithIndex
          (_, viewPos) <- rootView.allSubviewsWithPosition(index +: ViewPosition.root)
          genTransactionTree = fvt.tree.tryBlindForTransactionViewTree(viewPos.reverse)
        } yield FullTransactionViewTree.tryCreate(genTransactionTree)
        views.zipWithIndex.collect { case (t, i) if filter(i) => t }
      }

      // This example has a top level view visible to Alice and two subviews visible to Bob
      val example = exampleFactory.multiReaderCreate()

      // Top level subview is complete
      verifyExample(underTest, example, projections(Set(0)))

      // Although incomplete the following examples should pass model conformance
      verifyExample(underTest, example, projections(Set(1)))
      verifyExample(underTest, example, projections(Set(2)))

      // What Bob actually sees
      verifyExample(underTest, example, projections(Set(1, 2)))

    }

  }

  "When transaction tree has been manipulated" should {

    val underTest: ModelConformanceChecker = buildUnderTest()

    "fail if package is not in store" in {

      val duffPackageId = LfPackageId.assertFromString("duff-package-id")

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.actionDescriptionUnsafe)
          .andThen(ActionDescription.Optics.exercise)
          .andThen(ExerciseActionDescription.Optics.templateIdUnsafe)
          .andThen(LfLenses.fullReferencePkg[LfPackageId])
          .replace(duffPackageId)

      val expected = Ref.PackageRef.assertFromString(duffPackageId)

      inside(checkExample(underTest, exampleFactory.exerciseCycle(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) {
            case ModelConformanceChecker.DAMLeError(
                  DAMLe.EngineError(Error.Package(LfError.Package.MissingPackage(actual, _))),
                  _,
                ) =>
              actual shouldBe expected
          }
      }

    }

    "fail if an un-vetted package is used" in {
      val example = exampleFactory.exerciseCycle()
      val topologySnapshot = buildTopologySnapshot(
        topology =
          Map(example.actAs.toLf -> Map(participantId -> ParticipantPermission.Confirmation))
      )
      inside(checkExample(underTest, example, topologySnapshot, Seq(_))) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case ModelConformanceChecker.UnvettedPackages(actual) =>
            actual shouldBe Map(participantId -> example.metadata.usedPackages)
          }
      }
    }

    "fail if the view cannot be reconstructed" in {

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.saltUnsafe)
          .replace(TestSalt.generateSalt(Random.nextInt()))

      inside(checkExample(underTest, exampleFactory.exerciseCycle(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case _: ModelConformanceChecker.ViewReconstructionError =>
            succeed
          }
      }
    }

    "reject if the transaction tree contains blinded subview" in {

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.subviewsUnsafe)
          .andThen(TransactionSubviews.Optics.subviewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.subviewsUnsafe)
          .andThen(TransactionSubviews.Optics.subviewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewCommonDataUnsafe)
          .modify(_.blindFully)

      inside(checkExample(underTest, exampleFactory.acceptPaintOffer(), mutation)) {
        case InvalidMutation(reason) =>
          reason should include("A transaction view tree must contain a fully unblinded view")
      }

    }

    "reject the transaction if a subview is removed" in {

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.subviewsUnsafe)
          .andThen(TransactionSubviews.Optics.subviewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.subviewsUnsafe)
          .andThen(TransactionSubviews.Optics.subviewsUnsafe)
          .modify(_ => MerkleSeq.empty(testedProtocolVersion, pureCrypto))

      inside(checkExample(underTest, exampleFactory.acceptPaintOffer(), mutation)) {
        case ExceptionDuringProcessing(e: IllegalArgumentException) =>
          e.getMessage should include(
            "Number of subviews (1) and child effects (0) do not match for view"
          )
      }

    }

    "reject if an extra subview is added" in {
      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.subviewsUnsafe)
          .andThen(TransactionSubviews.Optics.subviewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .modify(s => s ++ s)

      inside(checkExample(underTest, exampleFactory.acceptPaintOffer(), mutation)) {
        case ExceptionDuringProcessing(e: TransactionView.InvalidView) =>
          e.getMessage should include regex "Contract.*is created multiple times in view"
      }
    }

    "reject if reinterpretation fails" in {
      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.actionDescriptionUnsafe)
          .andThen(ActionDescription.Optics.exercise)
          .andThen(ExerciseActionDescription.Optics.choiceUnsafe)
          .replace(LfChoiceName.assertFromString("NonExistentChoice"))

      inside(checkExample(underTest, exampleFactory.exerciseCycle(), mutation)) {
        case ModelConformanceRejection(ErrorWithSubTransaction(errors, _, _)) =>
          inside(errors.head) { case ModelConformanceChecker.DAMLeError(_, _) =>
            succeed
          }
      }
    }

    "reject if command contract is missing" in {

      val example = exampleFactory.multiReaderLookup()

      val missingCid = inside(example.tx.nodes(example.tx.roots.head)) {
        case exercise: Node.Exercise => exercise.targetCoid
      }

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.coreInputsUnsafe)
          .modify(input => input.filterNot(_._1 == missingCid))

      // When constructing the ViewParticipantData the population of the rootAction field results
      // in a `InvalidViewParticipantData` exception being thrown. If an attacker was to
      // pass a proto message mutated as above an exception would be thrown at proto deserialization time.
      intercept[ViewParticipantData.InvalidViewParticipantData] {
        checkExample(underTest, example, mutation)
      }.getMessage should include(s"the Exercise root action is not declared as core input")

    }

    "reject if fetched contract is missing" in {

      val example = exampleFactory.multiReaderLookup()
      val targetCid = inside(example.tx.nodes(example.tx.roots.head)) {
        case exercise: Node.Exercise => exercise.targetCoid
      }
      val missingCid = (example.contracts.keySet - targetCid).head

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.coreInputsUnsafe)
          .modify(input => input.filterNot(_._1 == missingCid))

      inside(checkExample(underTest, example, mutation)) { case ModelConformanceRejection(err) =>
        inside(err.errors.head) {
          case DAMLeError(
                DAMLe.EngineError(
                  lf.engine.Error.Interpretation(
                    DamlException(
                      ContractNotFound(actual)
                    ),
                    _,
                  )
                ),
                _,
              ) =>
            actual shouldBe missingCid
        }
      }

    }

    "reject when an extra input contract is provided" in {

      val extra = ExampleContractFactory.build()

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.coreInputsUnsafe)
          .modify(input => input + (extra.contractId -> InputContract(extra, consumed = false)))

      inside(checkExample(underTest, exampleFactory.multiReaderLookup(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case ViewReconstructionError(_, _) =>
            succeed
          }
      }

    }

    "reject invalid input contract" in {

      val example = exampleFactory.exerciseCycle()

      val contractValidator = new TestValidator(
        invalid = example.contracts.view.values
          .map(c => (c.contractId, c.inst.templateId.packageId) -> "invalid")
          .toMap
      )

      val underTest = buildUnderTest(contractValidator = contractValidator)

      inside(checkExample(underTest, example, identity)) { case ModelConformanceRejection(err) =>
        inside(err.errors.head) {
          case DAMLeError(
                DAMLe.EngineError(
                  lf.engine.Error.Interpretation(
                    DamlException(
                      lf.interpretation.Error.Upgrade(
                        lf.interpretation.Error.Upgrade.AuthenticationFailed(_, _, _, _, _)
                      )
                    ),
                    _,
                  )
                ),
                _,
              ) =>
            succeed
        }
      }
    }

    "reject if created contract is missing" in {

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.createdCoreUnsafe)
          .replace(Seq.empty)

      inside(checkExample(underTest, exampleFactory.multiReaderClone(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case ViewReconstructionError(_, _) =>
            succeed
          }
      }
    }

    "reject if extra created contract is presented" in {

      val extra = CreatedContract.tryCreate(
        contract = ExampleContractFactory.build(),
        consumedInCore = false,
        rolledBack = false,
      )

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.createdCoreUnsafe)
          .modify(_ :+ extra)

      inside(checkExample(underTest, exampleFactory.multiReaderClone(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case ViewReconstructionError(_, _) =>
            succeed
          }
      }
    }

    "reject wrong discriminator of created contract" in {
      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.createdCoreUnsafe)
          .andThen(Traversal.fromTraverse[Seq, CreatedContract])
          .andThen(CreatedContract.Optics.contractUnsafe)
          .modify(c =>
            inside(c.contractId) { case cid: Value.ContractId.V1 =>
              val newCid = Value.ContractId.V1(
                discriminator = ExampleContractFactory.lfHash(),
                suffix = cid.suffix,
              )
              ExampleContractFactory.modify(c, contractId = Some(newCid))
            }
          )

      inside(checkExample(underTest, exampleFactory.multiReaderClone(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case ViewReconstructionError(_, _) =>
            succeed
          }
      }

    }

    "reject wrong unicum of created contract" in {

      val wrongSuffix = inside(ExampleContractFactory.buildContractId()) {
        case cid: Value.ContractId.V1 => cid.suffix
      }

      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.createdCoreUnsafe)
          .andThen(Traversal.fromTraverse[Seq, CreatedContract])
          .andThen(CreatedContract.Optics.contractUnsafe)
          .modify(c =>
            inside(c.contractId) { case cid: Value.ContractId.V1 =>
              val newCid = Value.ContractId.V1(
                discriminator = cid.discriminator,
                suffix = wrongSuffix,
              )
              ExampleContractFactory.modify(c, contractId = Some(newCid))
            }
          )

      inside(checkExample(underTest, exampleFactory.multiReaderClone(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case ViewReconstructionError(_, _) =>
            succeed
          }
      }

    }

    "reject wrong authentication data for created contract" in {
      val mutation: FullTransactionViewTree => FullTransactionViewTree =
        FullTransactionViewTree.Optics.tree
          .andThen(GenTransactionTree.rootViewsUnsafe)
          .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
          .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
          .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
          .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
          .andThen(ViewParticipantData.Optics.createdCoreUnsafe)
          .andThen(Traversal.fromTraverse[Seq, CreatedContract])
          .andThen(CreatedContract.Optics.contractUnsafe)
          .modify(c =>
            ExampleContractFactory
              .modify(c, authenticationData = Some(Bytes.assertFromString("abcdef")))
          )

      inside(checkExample(underTest, exampleFactory.multiReaderClone(), mutation)) {
        case ModelConformanceRejection(err) =>
          inside(err.errors.head) { case ViewReconstructionError(_, _) =>
            succeed
          }
      }
    }
  }

  def unsuffixed(
      suffixed: WellFormedTransaction[WellFormedTransaction.Stage]
  ): WellFormedTransaction[WellFormedTransaction.WithoutSuffixes] =
    WellFormedTransaction.checkOrThrow(
      suffixed.unwrap.mapCid {
        case cid: Value.ContractId.V1 => Value.ContractId.V1(cid.discriminator)
        case cid: Value.ContractId.V2 => Value.ContractId.V2(cid.local, Bytes.Empty)
      },
      suffixed.metadata,
      WellFormedTransaction.WithoutSuffixes,
    )

  // Verify that an (un-mutated) example passes model conformance checking
  def verifyExample(
      underTest: ModelConformanceChecker,
      example: Example,
      projections: FullTransactionViewTree => Seq[FullTransactionViewTree] = Seq(_),
  ): Assertion = {

    val topologySnapshot: TopologySnapshot = buildTopologySnapshotFor(example)

    val pop = awaitFUS(
      topologySnapshot.activeParticipantsOfPartiesWithInfo(example.tx.informees.toSeq)
    )

    // Check all parties have a participant
    forEvery(pop.values)(_.participants should not be empty)

    val expectedUnsuffixedWfTx: WellFormedTransaction[WellFormedTransaction.WithoutSuffixes] =
      unsuffixed(
        WellFormedTransaction.checkOrThrow(
          example.tx,
          TransactionMetadata.fromLf(example.ledgerTime, example.metadata),
          WellFormedTransaction.WithoutSuffixes,
        )
      )

    inside(checkExample(underTest, example, topologySnapshot, projections)) {
      case TransactionTreeValid(result, tree) =>
        if (tree.exists(_.isTopLevel))
          unsuffixed(result.suffixedTransaction) shouldBe expectedUnsuffixedWfTx
        forEvery(result.suffixedTransaction.unwrap.transaction.nodes.values.collect {
          case create: LfNodeCreate => create
        })(create => create.coid.isAbsolute shouldBe true)
    }

  }

  def checkExample(
      underTest: ModelConformanceChecker,
      example: Example,
      mutation: FullTransactionViewTree => FullTransactionViewTree,
  ): CheckOutcome =
    checkExample(underTest, example, buildTopologySnapshotFor(example), t => Seq(mutation(t)))

  def checkExample(
      underTest: ModelConformanceChecker,
      example: Example,
      topologySnapshot: TopologySnapshot,
      mutation: FullTransactionViewTree => Seq[FullTransactionViewTree],
  ): CheckOutcome = {

    val wfTransaction: WellFormedTransaction[WellFormedTransaction.WithoutSuffixes] =
      WellFormedTransaction.checkOrThrow(
        lfTransaction = example.tx,
        metadata = TransactionMetadata.fromLf(example.ledgerTime, example.metadata),
        state = WellFormedTransaction.WithoutSuffixes,
      )

    val submitterInfo = SubmitterInfo(
      actAs = List(example.actAs.toLf),
      readAs = Nil,
      userId = Ref.UserId.assertFromString("test-user"),
      commandId = Ref.CommandId.assertFromString("test-command"),
      deduplicationPeriod = DeduplicationPeriod.DeduplicationOffset(None),
      submissionId = None,
      externallySignedSubmission = None,
    )

    val contractOfId
        : LfContractId => EitherT[FutureUnlessShutdown, ContractLookupError, GenContractInstance] =
      cId =>
        EitherT.fromEither[FutureUnlessShutdown](
          example.contracts.get(cId).toRight(ContractLookupError(cId, "Not found"))
        )

    val genTransactionTree: GenTransactionTree = awaitEitherT(
      transactionTreeFactory
        .createTransactionTree(
          transaction = wfTransaction,
          submitterInfo = submitterInfo,
          workflowId = None,
          mediator = mediatorGroup,
          transactionSeed = seedGenerator.generateSaltSeed(),
          transactionUuid = seedGenerator.generateUuid(),
          topologySnapshot = topologySnapshot,
          contractOfId = contractOfId,
          keyResolver = keyResolver,
          maxSequencingTime = CantonTimestamp.MaxValue,
          validatePackageVettings = false,
        )
    ).value

    val unmutatedTree: FullTransactionViewTree =
      FullTransactionViewTree.tryCreate(genTransactionTree)

    val manipulatedTrees: Seq[FullTransactionViewTree] = mutation(unmutatedTree)

    loggerFactory.suppressErrors {
      manipulatedTrees.traverse(_.validated) match {
        case Left(reason) =>
          InvalidMutation(reason)

        case Right(_) =>
          Try(checkTrees(underTest, manipulatedTrees, topologySnapshot)) match {
            case Success(Right(result)) =>
              TransactionTreeValid(result, manipulatedTrees)

            case Success(Left(err)) =>
              ModelConformanceRejection(err)

            case Failure(throwable) =>
              ExceptionDuringProcessing(throwable)
          }
      }
    }

  }

  val dummyViewAbsoluteLedgerEffect = RoseTree(
    ViewAbsoluteLedgerEffect(
      coreInputs = Map.empty[LfContractId, InputContract],
      createdCore = Seq.empty[CreatedContract],
      createdInSubviewArchivedInCore = Set.empty[LfContractId],
      resolvedKeys = Map.empty[LfGlobalKey, LfVersioned[SerializableKeyResolution]],
      inRollback = false,
      informees = Set.empty[LfPartyId],
    )
  )

  def checkTree(
      underTest: ModelConformanceChecker,
      fullTransactionViewTree: FullTransactionViewTree,
      topologySnapshot: TopologySnapshot,
  ): Either[ErrorWithSubTransaction[ViewAbsoluteLedgerEffect], Result] =
    checkTrees(underTest, Seq(fullTransactionViewTree), topologySnapshot)

  def checkTrees(
      underTest: ModelConformanceChecker,
      fullTransactionViewTrees: Seq[FullTransactionViewTree],
      topologySnapshot: TopologySnapshot,
  ): Either[ErrorWithSubTransaction[ViewAbsoluteLedgerEffect], Result] = {

    val commonData =
      TransactionProcessingSteps.tryCommonData(NonEmptyUtil.fromUnsafe(fullTransactionViewTrees))

    val rootViewTreesWithEffects =
      NonEmptyUtil.fromUnsafe(fullTransactionViewTrees.map(t => (t, dummyViewAbsoluteLedgerEffect)))

    val reInterpretedTopLevelViews: LazyAsyncReInterpretationMap =
      fullTransactionViewTrees
        .filter(_.isTopLevel)
        .map(fullTransactionViewTree =>
          fullTransactionViewTree.viewHash -> cats.Eval.later {
            underTest
              .reInterpret(
                view = fullTransactionViewTree.view,
                ledgerTime = fullTransactionViewTree.ledgerTime,
                preparationTime = fullTransactionViewTree.preparationTime,
                resolverFromView = keyResolver,
                getEngineAbortStatus = getEngineAbortStatus,
              )
          }
        )
        .toMap

    awaitEitherT(
      underTest
        .check(
          rootViewTrees = rootViewTreesWithEffects,
          topologySnapshot = topologySnapshot,
          commonData = commonData,
          reInterpretedTopLevelViews = reInterpretedTopLevelViews,
          keyResolverFor = _ => keyResolver,
          getEngineAbortStatus = getEngineAbortStatus,
        )
        .map {
          case valid if valid.updateId == commonData.updateId => valid
          case result => fail(s"Unexpected updateId: $result")
        }
    )
  }

}

sealed trait CheckOutcome
object CheckOutcome {

  // Indicates that the mutation made the transaction tree resulted in an invalid structure
  final case class InvalidMutation(reason: String) extends CheckOutcome

  // An exception was thrown during model conformance checking
  final case class ExceptionDuringProcessing(throwable: Throwable) extends CheckOutcome

  // Model conformance checking completed with a rejection
  final case class ModelConformanceRejection(
      error: ErrorWithSubTransaction[ViewAbsoluteLedgerEffect]
  ) extends CheckOutcome

  // Model conformance checking completed successfully
  final case class TransactionTreeValid(result: Result, tree: Seq[FullTransactionViewTree])
      extends CheckOutcome
}

object ModelConformanceCheckerTest extends OptionValues {

  object LfLenses {
    def fullReferencePkg[M]: Lens[FullReference[M], M] = GenLens[FullReference[M]](_.pkg)
  }

  final case class Example(
      actAs: PartyId,
      tx: SubmittedTransaction,
      metadata: Transaction.Metadata,
      ledgerTime: CantonTimestamp,
      contracts: Map[LfContractId, GenContractInstance],
  )

  class ExampleFactory(testEngine: TestEngine) extends EitherValues with AsJavaExtensions {

    val alice: PartyId = DefaultTestIdentities.party1
    val bob: PartyId = DefaultTestIdentities.party2

    def createCycle(): Example = {

      val command: Command = new Cycle("id", alice.toLf).create().commands.loneElement

      val (tx, txMeta) = testEngine.submitAndConsume(command, alice.toLf)

      Example(
        actAs = alice,
        tx = tx,
        metadata = txMeta,
        ledgerTime = CantonTimestamp.now(),
        contracts = Map.empty,
      )
    }

    def exerciseCycle(): Example = {

      val create = createCycle()
      val createNode = create.tx.nodes.values.collect { case e: Node.Create => e }.loneElement
      val contract = ContractInstance.create(testEngine.suffix(createNode)).value
      val command =
        new Cycle.ContractId(contract.contractId.coid).exerciseRepeat().commands().loneElement

      val (tx, txMeta) =
        testEngine.submitAndConsume(command, alice.toLf, contracts = Seq(contract.inst))

      Example(
        actAs = alice,
        tx = tx,
        metadata = txMeta,
        ledgerTime = CantonTimestamp.now(),
        contracts = Map(contract.contractId -> contract),
      )

    }

    private def createdContract[CID](
        create: Update[Created[CID]],
        actAs: PartyId,
        builder: String => CID,
    ): (CID, GenContractInstance) = {
      val command = create.commands.loneElement
      val (tx, _) = testEngine.submitAndConsume(command, actAs.toLf)
      val createNode = tx.nodes.values.collect { case e: Node.Create => e }.loneElement
      val inst = ContractInstance.create(testEngine.suffix(createNode)).value
      val cid: CID = builder.apply(inst.contractId.coid)
      (cid, inst)
    }

    def acceptPaintOffer(): Example = {

      import com.digitalasset.canton.examples.java.iou

      val houseOwner = DefaultTestIdentities.party1
      val bank = DefaultTestIdentities.party2
      val painter = DefaultTestIdentities.party3

      val gbp100 = new iou.Amount(java.math.BigDecimal.valueOf(100), "GBP")

      val createIou =
        new iou.Iou(bank.toLf, houseOwner.toLf, gbp100, List.empty.asJava).create()
      val (iouCid, iouInst) =
        createdContract[iou.Iou.ContractId](createIou, bank, s => new iou.Iou.ContractId(s))

      val createOffer =
        new OfferToPaintHouseByPainter(houseOwner.toLf, painter.toLf, bank.toLf, gbp100).create()
      val (offerCid, offerInst) =
        createdContract[OfferToPaintHouseByPainter.ContractId](
          createOffer,
          painter,
          s => new OfferToPaintHouseByPainter.ContractId(s),
        )

      val acceptOfferCmd = offerCid.exerciseAcceptByOwner(iouCid).commands().loneElement
      val (tx, txMeta) = testEngine.submitAndConsume(
        acceptOfferCmd,
        houseOwner.toLf,
        contracts = Seq(offerInst.inst, iouInst.inst),
      )

      Example(
        actAs = houseOwner,
        tx = tx,
        metadata = txMeta,
        ledgerTime = CantonTimestamp.now(),
        contracts = Map(offerInst.contractId -> offerInst, iouInst.contractId -> iouInst),
      )

    }

    def multiReaderLookup(): Example = {

      import com.digitalasset.canton.damltests.modelconformance.v1.java.modelconformanceexamples.*

      val multiReader = new MultiReader(alice.toLf, alice.toLf).create()

      val (mrCid1, mr1) = createdContract(multiReader, alice, s => new MultiReader.ContractId(s))
      val (mrCid2, mr2) = createdContract(multiReader, alice, s => new MultiReader.ContractId(s))
      val (mrCidR, mrR) = createdContract(multiReader, alice, s => new MultiReader.ContractId(s))
      val contracts = Seq(mrR, mr1, mr2)

      val command = mrCidR.exerciseMultiReader_Lookup(mrCid1, mrCid2).commands().loneElement
      val (tx, txMeta) =
        testEngine.submitAndConsume(command, alice.toLf, contracts = contracts.map(_.inst))

      Example(
        actAs = alice,
        tx = tx,
        metadata = txMeta,
        ledgerTime = CantonTimestamp.now(),
        contracts = contracts.map(c => c.contractId -> c).toMap,
      )

    }

    def multiReaderCreate(): Example = {

      import com.digitalasset.canton.damltests.modelconformance.v1.java.modelconformanceexamples.*

      val multiReader = new MultiReader(alice.toLf, alice.toLf).create()

      val (mrCid, mr) = createdContract(multiReader, alice, s => new MultiReader.ContractId(s))
      val contracts = Seq(mr)

      val command = mrCid.exerciseMultiReader_NewObserver(bob.toLf).commands().loneElement
      val (tx, txMeta) =
        testEngine.submitAndConsume(command, alice.toLf, contracts = contracts.map(_.inst))

      Example(
        actAs = alice,
        tx = tx,
        metadata = txMeta,
        ledgerTime = CantonTimestamp.now(),
        contracts = contracts.map(c => c.contractId -> c).toMap,
      )

    }

    def multiReaderClone(): Example = {

      import com.digitalasset.canton.damltests.modelconformance.v1.java.modelconformanceexamples.*

      val multiReader = new MultiReader(alice.toLf, alice.toLf).create()

      val (mrCidR, mrR) = createdContract(multiReader, alice, s => new MultiReader.ContractId(s))
      val contracts = Seq(mrR)

      val command = mrCidR.exerciseMultiReader_Clone().commands().loneElement
      val (tx, txMeta) =
        testEngine.submitAndConsume(command, alice.toLf, contracts = contracts.map(_.inst))

      Example(
        actAs = alice,
        tx = tx,
        metadata = txMeta,
        ledgerTime = CantonTimestamp.now(),
        contracts = contracts.map(c => c.contractId -> c).toMap,
      )

    }

  }

}
