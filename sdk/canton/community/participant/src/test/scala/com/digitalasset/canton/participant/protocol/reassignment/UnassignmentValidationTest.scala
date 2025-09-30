// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.implicits.catsSyntaxEitherId
import com.daml.logging.LoggingContext
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Signature, SigningKeyUsage}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  FullUnassignmentTree,
  ReassignmentRef,
  UnassignmentViewTree,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers.mkActivenessResult
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ParsedReassignmentRequest,
  ReassignmentProcessorError,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.{
  ContractAuthenticationFailure,
  ReassigningParticipantsMismatch,
  StakeholdersMismatch,
  SubmitterMustBeStakeholder,
}
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.sequencing.protocol.{MediatorGroupRecipient, Recipients}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.NotImplementedException
import org.scalatest.wordspec.AnyWordSpec

import java.util.UUID
import scala.concurrent.ExecutionContext

class UnassignmentValidationTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  private val sourceSynchronizer = Source(
    SynchronizerId.tryFromString("synchronizer::source").toPhysical
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.tryCreate(100))
  private val targetSynchronizer = Target(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("synchronizer::target")).toPhysical
  )

  private val signatory: LfPartyId = LfPartyId.assertFromString("signatory::party")
  private val observer: LfPartyId = LfPartyId.assertFromString("observer::party")

  private val nonStakeholder: LfPartyId = LfPartyId.assertFromString("nonStakeholder::party")

  private val receiverParty2: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("receiverParty2::party")
  ).toLf

  private val confirmingParticipant =
    ParticipantId.tryFromProtoPrimitive("PAR::bothsynchronizers::confirmingParticipant")
  private val observingParticipant =
    ParticipantId.tryFromProtoPrimitive("PAR::bothsynchronizers::observingParticipant")
  private val otherParticipant = ParticipantId.tryFromProtoPrimitive("PAR::sync::participant")

  private val uuid = new UUID(3L, 4L)
  private val pureCrypto = new SymbolicPureCrypto
  private val seedGenerator = new SeedGenerator(pureCrypto)
  private val seed = seedGenerator.generateSaltSeed()

  private val wrongTemplateId =
    LfTemplateId.assertFromString("unassignmentvalidatoionpackage:wrongtemplate:id")

  private def testMetadata(
      signatories: Set[LfPartyId] = Set(signatory),
      stakeholders: Set[LfPartyId] = Set(signatory, observer),
      maybeKeyWithMaintainersVersioned: Option[LfVersioned[LfGlobalKeyWithMaintainers]] = None,
  ): ContractMetadata =
    ContractMetadata.tryCreate(
      stakeholders = stakeholders,
      signatories = signatories,
      maybeKeyWithMaintainersVersioned = maybeKeyWithMaintainersVersioned,
    )

  private val baseMetadata = testMetadata()

  private val stakeholders: Stakeholders = Stakeholders(baseMetadata)

  private val contract = ExampleContractFactory.build(
    signatories = baseMetadata.signatories,
    stakeholders = baseMetadata.stakeholders,
  )

  private val reassigningParticipants: Set[ParticipantId] =
    Set(confirmingParticipant, observingParticipant)

  private val identityFactory: TestingIdentityFactory = TestingTopology()
    .withSynchronizers(sourceSynchronizer.unwrap)
    .withReversedTopology(
      Map(
        confirmingParticipant -> Map(
          signatory -> ParticipantPermission.Submission,
          receiverParty2 -> ParticipantPermission.Submission,
        ),
        observingParticipant -> Map(observer -> ParticipantPermission.Observation),
      )
    )
    .withSimpleParticipants(
      confirmingParticipant
    ) // required such that `participant` gets a signing key
    .withPackages(
      Map(
        confirmingParticipant -> Seq(
          ExampleTransactionFactory.packageId,
          wrongTemplateId.packageId,
        ),
        observingParticipant -> Seq(ExampleTransactionFactory.packageId, wrongTemplateId.packageId),
      )
    )
    .build(loggerFactory)

  "unassignment validation" should {
    "succeed without errors" in {
      val validation = performValidation().futureValueUS.value

      validation.isSuccessful.futureValueUS shouldBe true
    }

    "report non-validatable when cannot fetch target topology" in {
      val validation = performValidation(targetTopology = None).futureValueUS.value

      validation.reassigningParticipantValidationResult.isTargetTsValidatable shouldBe false
    }

    "fail if contract validation fails" in {

      val expected = "bad-contract"

      val failingContractValidator = new ContractValidator {
        override def authenticate(contract: FatContractInstance, targetPackageId: PackageId)(
            implicit
            ec: ExecutionContext,
            traceContext: TraceContext,
            loggingContext: LoggingContext,
        ): EitherT[FutureUnlessShutdown, String, Unit] =
          EitherT.fromEither[FutureUnlessShutdown](expected.asLeft[Unit])
        override def authenticateHash(
            contract: FatContractInstance,
            contractHash: LfHash,
        ): Either[String, Unit] = throw new NotImplementedException()
      }

      inside(
        performValidation(
          contract,
          contractValidator = failingContractValidator,
        ).futureValueUS.value.commonValidationResult.contractAuthenticationResultF.futureValueUS.left.value
      ) {
        case ContractAuthenticationFailure(ref, reason, contractId) =>
          ref shouldBe ReassignmentRef(contract.contractId)
          contractId shouldBe contract.contractId
          reason should include(expected)
        case other => fail(s"Did not expect $other")
      }

    }

    "fail when inconsistent stakeholders are given" in {
      /*
      We construct in this test an inconsistent `inconsistentTree: FullUnassignmentTree` :
      - inconsistentTree.tree.commonData.stakeholders is incorrect
      - inconsistentTree.view.contract.metadata is correct
       */

      def createUnassignmentTree(metadata: ContractMetadata): FullUnassignmentTree =
        ReassignmentDataHelpers(
          ExampleContractFactory.modify(contract, metadata = Some(metadata)),
          sourceSynchronizer,
          targetSynchronizer,
          identityFactory,
        ).unassignmentRequest(
          submitter = signatory,
          submittingParticipant = confirmingParticipant,
          sourceMediator = sourceMediator,
        )(reassigningParticipants)
          .toFullUnassignmentTree(
            pureCrypto,
            pureCrypto,
            seed,
            uuid,
          )

      def test(
          viewContractMetadata: ContractMetadata,
          commonDataContractMetadata: ContractMetadata,
      ): Either[ReassignmentValidationError, Unit] = {
        val view = createUnassignmentTree(viewContractMetadata).tree.view
        val commonData = createUnassignmentTree(commonDataContractMetadata).tree.commonData

        validateUnassignmentTree(
          FullUnassignmentTree(
            UnassignmentViewTree(commonData, view, Source(testedProtocolVersion), pureCrypto)
          ),
          Some(Target(identityFactory.topologySnapshot())),
        ).futureValueUS.value.commonValidationResult.contractAuthenticationResultF.futureValueUS
      }

      val incorrectMetadata = ContractMetadata.tryCreate(
        stakeholders = stakeholders.all + receiverParty2,
        signatories = stakeholders.signatories,
        maybeKeyWithMaintainersVersioned = None,
      )

      val correctMetadata = contract.metadata

      test(correctMetadata, correctMetadata).isRight shouldBe true
      test(correctMetadata, incorrectMetadata).left.value shouldBe StakeholdersMismatch(
        reassignmentRef = ReassignmentRef(contract.contractId),
        declaredViewStakeholders = Stakeholders(incorrectMetadata),
        expectedStakeholders = Stakeholders(correctMetadata),
      )
    }

    "detect non-stakeholder submitter" in {
      def unassignmentValidation(submitter: LfPartyId) = {
        val validation = performValidation(submitter = submitter)
        validation.futureValueUS.value.commonValidationResult.submitterCheckResult
      }

      assert(!stakeholders.all.contains(nonStakeholder))

      unassignmentValidation(signatory) shouldBe None
      unassignmentValidation(
        nonStakeholder
      ) shouldBe Some(
        SubmitterMustBeStakeholder(
          ReassignmentRef(contract.contractId),
          submittingParty = nonStakeholder,
          stakeholders = stakeholders.all,
        )
      )
    }

    "detect reassigning participant mismatch" in {
      def unassignmentValidation(reassigningParticipants: Set[ParticipantId]) =
        performValidation(
          reassigningParticipantsOverride = reassigningParticipants
        ).futureValueUS.value.reassigningParticipantValidationResult.errors

      // Happy path / control
      unassignmentValidation(reassigningParticipants = reassigningParticipants) shouldBe Seq()

      // Additional/extra reassigning participant
      val additionalReassigningParticipant = reassigningParticipants + otherParticipant

      unassignmentValidation(
        reassigningParticipants = additionalReassigningParticipant
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = additionalReassigningParticipant,
        )
      )

      // Additional/extra reassigning participant
      val additionalConfirmingReassigningParticipant = reassigningParticipants + otherParticipant

      unassignmentValidation(
        reassigningParticipants = additionalConfirmingReassigningParticipant
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = additionalConfirmingReassigningParticipant,
        )
      )

      // Missing reassigning participant
      val missingConfirmingReassigningParticipant = Set.empty[ParticipantId]
      unassignmentValidation(
        reassigningParticipants = missingConfirmingReassigningParticipant
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = missingConfirmingReassigningParticipant,
        )
      )

      // Empty set
      unassignmentValidation(
        reassigningParticipants = Set.empty
      ) shouldBe Seq(
        ReassigningParticipantsMismatch(
          ReassignmentRef(contract.contractId),
          expected = reassigningParticipants,
          declared = Set.empty,
        )
      )
    }
  }

  private val cryptoSnapshot = identityFactory
    .forOwnerAndSynchronizer(confirmingParticipant, sourceSynchronizer.unwrap)
    .currentSnapshotApproximation

  private val reassignmentId = ReassignmentId.tryCreate("00")

  private def mkParsedRequest(
      view: FullUnassignmentTree,
      recipients: Recipients,
      signatureO: Option[Signature],
  ): ParsedReassignmentRequest[FullUnassignmentTree] = ParsedReassignmentRequest(
    RequestCounter(1),
    CantonTimestamp.Epoch,
    SequencerCounter(1),
    view,
    recipients,
    signatureO,
    None,
    isFreshOwnTimelyRequest = true,
    areContractsUnknown = false,
    Seq.empty,
    sourceMediator,
    cryptoSnapshot,
    cryptoSnapshot.ipsSnapshot.findDynamicSynchronizerParameters().futureValueUS.value,
    reassignmentId,
  )

  private def validateUnassignmentTree(
      fullUnassignmentTree: FullUnassignmentTree,
      targetTopology: Option[Target[TopologySnapshot]],
      contractValidator: ContractValidator = ContractValidator.AllowAll,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val recipients = Recipients.cc(
      reassigningParticipants.toSeq.head,
      reassigningParticipants.toSeq.tail*
    )
    val signature = cryptoSnapshot
      .sign(fullUnassignmentTree.rootHash.unwrap, SigningKeyUsage.ProtocolOnly)
      .futureValueUS
      .value
    val parsed = mkParsedRequest(fullUnassignmentTree, recipients, Some(signature))

    val getTopologyAtTs = new GetTopologyAtTimestamp {
      import com.digitalasset.canton.tracing.TraceContext
      override def maybeAwaitTopologySnapshot(
          targetPSId: Target[PhysicalSynchronizerId],
          requestedTimestamp: Target[CantonTimestamp],
      )(implicit tc: TraceContext) = EitherT.rightT(targetTopology)
    }

    val unassignmentValidation = UnassignmentValidation(
      isReassigningParticipant = true,
      participantId = confirmingParticipant,
      contractValidator = contractValidator,
      activenessF = FutureUnlessShutdown.pure(mkActivenessResult()),
      getTopologyAtTs = getTopologyAtTs,
    )

    unassignmentValidation.perform(parsedRequest = parsed)
  }

  private def performValidation(
      contract: ContractInstance = contract,
      reassigningParticipantsOverride: Set[ParticipantId] = reassigningParticipants,
      submitter: LfPartyId = signatory,
      identityFactory: TestingIdentityFactory = identityFactory,
      targetTopology: Option[Target[TopologySnapshot]] = Some(
        Target(identityFactory.topologySnapshot())
      ),
      contractValidator: ContractValidator = ContractValidator.AllowAll,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, UnassignmentValidationResult] = {
    val unassignmentRequest =
      ReassignmentDataHelpers(contract, sourceSynchronizer, targetSynchronizer, identityFactory)
        .unassignmentRequest(
          submitter,
          submittingParticipant = confirmingParticipant,
          sourceMediator = sourceMediator,
        )(reassigningParticipantsOverride)

    val fullUnassignmentTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    validateUnassignmentTree(
      fullUnassignmentTree,
      targetTopology,
      contractValidator,
    )
  }
}
