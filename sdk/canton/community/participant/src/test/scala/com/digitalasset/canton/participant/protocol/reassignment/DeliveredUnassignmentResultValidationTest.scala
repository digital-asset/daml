// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.SignatureCheckError.SignatureWithWrongKey
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, HashPurpose}
import com.digitalasset.canton.data.ViewType.{AssignmentViewType, UnassignmentViewType}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.participant.protocol.reassignment.DeliveredUnassignmentResultValidation.{
  IncorrectDomain,
  IncorrectInformees,
  IncorrectRequestId,
  IncorrectRootHash,
  IncorrectSignatures,
  ResultTimestampExceedsDecisionTime,
  StakeholderNotHostedReassigningParticipant,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.LocalRejectError.ConsistencyRejections.LockedContracts
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult.InvalidUnassignmentResult
import com.digitalasset.canton.protocol.messages.Verdict.MediatorReject
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResultMessage,
  DeliveredUnassignmentResult,
  EmptyRootHashMessagePayload,
  RootHashMessage,
  Verdict,
}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

class DeliveredUnassignmentResultValidationTest
    extends AsyncWordSpec
    with BaseTest
    with HasActorSystem
    with HasExecutionContext {
  private val sourceDomain = Source(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::source"))
  )
  private val sourceMediator = MediatorGroupRecipient(MediatorGroupIndex.zero)
  private val targetDomain = Target(
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::target"))
  )

  private val signatory: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("signatory::party")
  ).toLf

  // TODO(#21072) Revisit when confirmation is required only from signatories
  private val observer: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("observer::party")
  ).toLf

  private val otherParty: LfPartyId = PartyId(
    UniqueIdentifier.tryFromProtoPrimitive("other::party")
  ).toLf

  private val submittingParticipant = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("bothdomains::participant")
  )

  private val identityFactory: TestingIdentityFactory = TestingTopology()
    .withDomains(sourceDomain.unwrap)
    .withReversedTopology(
      Map(
        submittingParticipant -> Map(
          signatory -> ParticipantPermission.Submission,
          observer -> ParticipantPermission.Observation,
        )
      )
    )
    .withSimpleParticipants(submittingParticipant)
    .build(loggerFactory)

  private lazy val mediatorCrypto = identityFactory
    .forOwnerAndDomain(DefaultTestIdentities.mediatorId, sourceDomain.unwrap)
    .currentSnapshotApproximation

  private lazy val sequencerCrypto = identityFactory
    .forOwnerAndDomain(DefaultTestIdentities.sequencerId, sourceDomain.unwrap)
    .currentSnapshotApproximation

  private val cryptoClient = identityFactory
    .forOwnerAndDomain(submittingParticipant, sourceDomain.unwrap)

  private val cryptoSnapshot = cryptoClient.currentSnapshotApproximation

  private lazy val contractId = ExampleTransactionFactory.suffixedId(10, 0)
  private lazy val contract = ExampleTransactionFactory.asSerializable(
    contractId,
    contractInstance = ExampleTransactionFactory.contractInstance(),
  )

  private lazy val reassignmentId = ReassignmentId(sourceDomain, CantonTimestamp.Epoch)

  private lazy val reassignmentDataHelpers = new ReassignmentDataHelpers(
    contract,
    reassignmentId.sourceDomain,
    targetDomain,
    identityFactory,
  )

  private lazy val reassigningParticipants = NonEmpty.mk(Seq, submittingParticipant)
  private lazy val stakeholders = Set(signatory, observer)

  private lazy val unassignmentRequest = reassignmentDataHelpers.unassignmentRequest(
    submitter = signatory,
    submittingParticipant = submittingParticipant,
    sourceMediator = sourceMediator,
  )(
    stakeholders = stakeholders,
    reassigningParticipants = reassigningParticipants.toSet,
  )

  private lazy val reassignmentData =
    reassignmentDataHelpers.reassignmentData(reassignmentId, unassignmentRequest)(None)

  private lazy val unassignmentResult =
    reassignmentDataHelpers.unassignmentResult(reassignmentData).futureValue

  private lazy val requestId: RequestId = RequestId(CantonTimestamp.Epoch)
  private lazy val decisionTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(10)

  private def validateResult(
      result: DeliveredUnassignmentResult
  ): Either[DeliveredUnassignmentResultValidation.Error, Unit] =
    DeliveredUnassignmentResultValidation(
      unassignmentRequest = reassignmentData.unassignmentRequest,
      unassignmentRequestTs = requestId.unwrap,
      unassignmentDecisionTime = decisionTime,
      sourceTopology = Source(cryptoSnapshot),
      targetTopology = Target(cryptoSnapshot.ipsSnapshot),
    )(result).validate.value.futureValue

  // Transform the ConfirmationResultMessage and apply validation
  private def updateAndValidate(
      transform: ConfirmationResultMessage => ConfirmationResultMessage = identity,
      sequencingTime: CantonTimestamp = CantonTimestamp.Epoch,
      overrideCryptoSnapshotMediator: Option[DomainSnapshotSyncCryptoApi] = None,
      overrideCryptoSnapshotSequencer: Option[DomainSnapshotSyncCryptoApi] = None,
  ): Either[DeliveredUnassignmentResultValidation.Error, Unit] = {
    val result = reassignmentDataHelpers
      .unassignmentResult(
        result = transform(unassignmentResult.unwrap),
        recipients = reassigningParticipants,
        sequencingTime = sequencingTime,
        overrideCryptoSnapshotMediator = overrideCryptoSnapshotMediator,
        overrideCryptoSnapshotSequencer = overrideCryptoSnapshotSequencer,
      )
      .futureValue

    validateResult(result)
  }

  "DeliveredUnassignmentResultValidation: factories" should {
    "detect incorrect view type" in {
      val error = reassignmentDataHelpers
        .unassignmentResult(
          unassignmentResult.unwrap.copy(viewType = AssignmentViewType),
          reassigningParticipants,
        )
        .value
        .futureValue
        .left
        .value

      error.message shouldBe "The deliver event must contain exactly one unassignment result, but found 0."

      reassignmentDataHelpers
        .unassignmentResult(
          unassignmentResult.unwrap.copy(viewType = UnassignmentViewType),
          reassigningParticipants,
        )
        .futureValue shouldBe a[DeliveredUnassignmentResult]
    }

    "detect incorrect number of envelopes" in {
      val validEnvelope = unassignmentResult.result.content.batch.envelopes.loneElement

      val rootHash = unassignmentResult.unwrap.rootHash
      val rootHashMessage = RootHashMessage(
        rootHash,
        sourceDomain.unwrap,
        ViewType.AssignmentViewType,
        CantonTimestamp.Epoch,
        EmptyRootHashMessagePayload,
      )(RootHashMessage.protocolVersionRepresentativeFor(testedProtocolVersion))

      val error = reassignmentDataHelpers
        .unassignmentResult(
          unassignmentResult.unwrap,
          reassigningParticipants,
          // The batch will then contain two envelopes
          additionalEnvelopes = List((rootHashMessage, validEnvelope.recipients)),
        )
        .value
        .futureValue
        .left
        .value

      error.message should include(
        "The deliver event must contain exactly one envelope, but found 2"
      )

      reassignmentDataHelpers
        .unassignmentResult(
          unassignmentResult.unwrap,
          reassigningParticipants,
        )
        .futureValue shouldBe a[DeliveredUnassignmentResult]
    }

    "detect incorrect verdict" in {
      val approve = Verdict.Approve(testedProtocolVersion)
      val mediatorReject = MediatorReject.tryCreate(
        MediatorError.MalformedMessage.Reject("").rpcStatusWithoutLoggingContext(),
        isMalformed = true,
        testedProtocolVersion,
      )
      lazy val localReject: LockedContracts.Reject =
        LocalRejectError.ConsistencyRejections.LockedContracts.Reject(Seq.empty)
      val participantReject: Verdict.ParticipantReject =
        Verdict.ParticipantReject(
          NonEmpty(List, Set.empty[LfPartyId] -> localReject.toLocalReject(testedProtocolVersion)),
          testedProtocolVersion,
        )

      def updateAndValidate(verdict: Verdict) = reassignmentDataHelpers
        .unassignmentResult(
          unassignmentResult.unwrap.copy(verdict = verdict),
          reassigningParticipants,
        )
        .value
        .futureValue

      def error(verdict: Verdict) = InvalidUnassignmentResult(
        unassignmentResult.result.content,
        s"The unassignment result must be approving; found: $verdict",
      ).message

      updateAndValidate(approve).value shouldBe a[DeliveredUnassignmentResult]
      updateAndValidate(mediatorReject).left.value.message shouldBe error(mediatorReject)
      updateAndValidate(participantReject).left.value.message shouldBe error(participantReject)
    }
  }

  "DeliveredUnassignmentResultValidation: confirmation result message" should {
    "succeed without errors in the basic case" in {
      validateResult(unassignmentResult).value shouldBe ()
    }

    "detect incorrect domain id" in {
      updateAndValidate(_.copy(domainId = sourceDomain.unwrap)).value shouldBe ()
      updateAndValidate(_.copy(domainId = targetDomain.unwrap)).left.value shouldBe IncorrectDomain(
        sourceDomain.unwrap,
        targetDomain.unwrap,
      )
    }

    "detect incorrect request id" in {
      val incorrectRequestId = RequestId(requestId.unwrap.plusSeconds(1))

      updateAndValidate(_.copy(requestId = requestId)).value shouldBe ()
      updateAndValidate(
        _.copy(requestId = incorrectRequestId)
      ).left.value shouldBe IncorrectRequestId(requestId, incorrectRequestId)
    }

    "detect incorrect root hash" in {
      val incorrectRootHash = RootHash(
        cryptoSnapshot.pureCrypto.digest(
          HashPurpose.SequencedEventSignature,
          ByteString.copyFromUtf8("Hey!"),
        )
      )
      val expectedRootHash = unassignmentResult.unwrap.rootHash

      updateAndValidate(_.copy(rootHash = expectedRootHash)).value shouldBe ()
      updateAndValidate(
        _.copy(rootHash = incorrectRootHash)
      ).left.value shouldBe IncorrectRootHash(expectedRootHash, incorrectRootHash)
    }

    "detect incorrect informees" in {
      val missingParty = Set(signatory)
      val tooManyParties = stakeholders + otherParty
      val expectedRootHash = unassignmentResult.unwrap.rootHash

      updateAndValidate(_.copy(rootHash = expectedRootHash)).value shouldBe ()
      updateAndValidate(_.copy(informees = missingParty)).left.value shouldBe IncorrectInformees(
        stakeholders,
        missingParty,
      )
      updateAndValidate(_.copy(informees = tooManyParties)).left.value shouldBe IncorrectInformees(
        stakeholders,
        tooManyParties,
      )
    }

    "detect sequencing time which is after decision time" in {
      val incorrectSequencingTime = decisionTime.plusMillis(1)

      updateAndValidate(sequencingTime = decisionTime).value shouldBe ()
      updateAndValidate(sequencingTime =
        incorrectSequencingTime
      ).left.value shouldBe ResultTimestampExceedsDecisionTime(
        timestamp = incorrectSequencingTime,
        decisionTime = decisionTime,
      )
    }

    "detect incorrect mediator signature" in {
      val mediatorCrypto = identityFactory
        .forOwnerAndDomain(DefaultTestIdentities.mediatorId, sourceDomain.unwrap)
        .currentSnapshotApproximation

      val sequencerCrypto = identityFactory
        .forOwnerAndDomain(DefaultTestIdentities.sequencerId, sourceDomain.unwrap)
        .currentSnapshotApproximation

      updateAndValidate(
        overrideCryptoSnapshotMediator = Some(mediatorCrypto)
      ).value shouldBe ()

      updateAndValidate(
        overrideCryptoSnapshotMediator = Some(sequencerCrypto)
      ).left.value.error should (include(SignatureWithWrongKey("").message) and include(
        IncorrectSignatures(kind = "mediators", msg = "").error
      ))
    }

    "detect stakeholder not hosted on a reassigning participant" in {
      // Stakeholder observer is not in this topology, which means that it will not have a reassigning participant
      val observerMissing = TestingTopology()
        .withDomains(targetDomain.unwrap)
        .withReversedTopology(
          Map(
            submittingParticipant -> Map(
              signatory -> ParticipantPermission.Submission
            )
          )
        )
        .withSimpleParticipants(submittingParticipant)
        .build(loggerFactory)
        .forOwnerAndDomain(submittingParticipant, targetDomain.unwrap)
        .currentSnapshotApproximation
        .ipsSnapshot

      observerMissing
        .activeParticipantsOfAll(stakeholders.toList)
        .value
        .futureValue
        .left
        .value shouldBe Set(observer)

      cryptoSnapshot.ipsSnapshot
        .activeParticipantsOfAll(stakeholders.toList)
        .value
        .futureValue
        .value shouldBe reassigningParticipants.toSet

      def validate(targetTopology: TopologySnapshot) = DeliveredUnassignmentResultValidation(
        unassignmentRequest = reassignmentData.unassignmentRequest,
        unassignmentRequestTs = requestId.unwrap,
        unassignmentDecisionTime = decisionTime,
        sourceTopology = Source(cryptoSnapshot),
        targetTopology = Target(targetTopology),
      )(unassignmentResult).validate.value.futureValue

      validate(cryptoSnapshot.ipsSnapshot).value shouldBe ()
      validate(observerMissing).left.value shouldBe StakeholderNotHostedReassigningParticipant(
        observer
      )
    }
  }

  "DeliveredUnassignmentResultValidation: deliver event" should {
    "succeed without errors in the basic case" in {
      validateResult(unassignmentResult).value shouldBe ()
    }

    "detect incorrect domain id" in {
      def validate(domainId: DomainId) = {
        val result = ReassignmentDataHelpers
          .unassignmentResult(
            unassignmentResult.unwrap,
            recipients = reassigningParticipants,
            protocolVersion = testedProtocolVersion,
            cryptoSnapshotMediator = mediatorCrypto,
            cryptoSnapshotSequencer = sequencerCrypto,
          )(domainId)
          .futureValue

        validateResult(result)
      }

      validate(sourceDomain.unwrap).value shouldBe ()
      validate(targetDomain.unwrap).left.value shouldBe IncorrectDomain(
        sourceDomain.unwrap,
        targetDomain.unwrap,
      )
    }

    "detect incorrect sequencer signature" in {
      updateAndValidate(
        overrideCryptoSnapshotSequencer = Some(sequencerCrypto)
      ).value shouldBe ()

      updateAndValidate(
        overrideCryptoSnapshotSequencer = Some(mediatorCrypto)
      ).left.value.error should (include(SignatureWithWrongKey("").message) and include(
        IncorrectSignatures(kind = "sequencers", msg = "").error
      ))
    }
  }
}
