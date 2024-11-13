// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{
  DomainCryptoPureApi,
  DomainSnapshotSyncCryptoApi,
  DomainSyncCryptoClient,
  HashPurpose,
  Signature,
  TestHash,
}
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata, ViewType}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentData.UnassignmentGlobalOffset
import com.digitalasset.canton.participant.protocol.submission.SeedGenerator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResultMessage,
  DeliveredUnassignmentResult,
  ProtocolMessage,
  SignedProtocolMessage,
  Verdict,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.time.TimeProofTestUtil
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

final case class ReassignmentDataHelpers(
    contract: SerializableContract,
    sourceDomain: Source[DomainId],
    targetDomain: Target[DomainId],
    pureCrypto: DomainCryptoPureApi,
    // mediatorCryptoClient and sequencerCryptoClient need to be defined for computation of the DeliveredUnassignmentResult
    mediatorCryptoClient: Option[DomainSyncCryptoClient] = None,
    sequencerCryptoClient: Option[DomainSyncCryptoClient] = None,
    targetTime: CantonTimestamp = CantonTimestamp.Epoch,
)(implicit executionContext: ExecutionContext) {
  import org.scalatest.OptionValues.*

  private val targetTimeProof: TimeProof = TimeProofTestUtil.mkTimeProof(
    timestamp = targetTime,
    targetDomain = targetDomain,
  )

  private val seedGenerator: SeedGenerator =
    new SeedGenerator(pureCrypto)

  private val protocolVersion: ProtocolVersion = BaseTest.testedProtocolVersion

  private def submitterInfo(
      submitter: LfPartyId,
      submittingParticipant: ParticipantId,
  ): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      LedgerCommandId.assertFromString("assignment-validation-command-id"),
      submissionId = None,
      LedgerApplicationId.assertFromString("tests"),
      workflowId = None,
    )

  def unassignmentRequest(
      submitter: LfPartyId,
      submittingParticipant: ParticipantId,
      sourceMediator: MediatorGroupRecipient,
  )(
      reassigningParticipants: Set[ParticipantId] = Set(submittingParticipant)
  ): UnassignmentRequest =
    UnassignmentRequest(
      submitterMetadata = submitterInfo(submitter, submittingParticipant),
      reassigningParticipants = reassigningParticipants,
      contract = contract,
      sourceDomain = sourceDomain,
      sourceProtocolVersion = Source(protocolVersion),
      sourceMediator = sourceMediator,
      targetDomain = targetDomain,
      targetProtocolVersion = Target(protocolVersion),
      targetTimeProof = targetTimeProof,
      reassignmentCounter = ReassignmentCounter(1),
    )

  def reassignmentData(reassignmentId: ReassignmentId, unassignmentRequest: UnassignmentRequest)(
      unassignmentGlobalOffset: Option[GlobalOffset] = None
  ): ReassignmentData = {
    val uuid = new UUID(10L, 0L)
    val seed = seedGenerator.generateSaltSeed()

    val fullUnassignmentViewTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    ReassignmentData(
      sourceProtocolVersion = Source(BaseTest.testedProtocolVersion),
      unassignmentTs = reassignmentId.unassignmentTs,
      unassignmentRequestCounter = RequestCounter(0),
      unassignmentRequest = fullUnassignmentViewTree,
      unassignmentDecisionTime = CantonTimestamp.ofEpochSecond(10),
      contract = contract,
      unassignmentResult = None,
      reassignmentGlobalOffset = unassignmentGlobalOffset.map(UnassignmentGlobalOffset.apply),
    )
  }

  def unassignmentResult(
      reassignmentData: ReassignmentData
  )(implicit traceContext: TraceContext): EitherT[
    Future,
    DeliveredUnassignmentResult.InvalidUnassignmentResult,
    DeliveredUnassignmentResult,
  ] = {

    val result =
      ConfirmationResultMessage.create(
        domainId = sourceDomain.unwrap,
        viewType = ViewType.UnassignmentViewType,
        requestId = RequestId(reassignmentData.unassignmentTs),
        rootHash = reassignmentData.unassignmentRequest.rootHash,
        verdict = Verdict.Approve(protocolVersion),
        informees = reassignmentData.unassignmentRequest.stakeholders.all,
        protocolVersion,
      )

    val recipients = NonEmptyUtil
      .fromUnsafe(reassignmentData.unassignmentRequest.reassigningParticipants)
      .toSeq

    unassignmentResult(result, recipients)
  }

  /** From the result, constructs the DeliveredUnassignmentResult (mostly add signatures)
    */
  def unassignmentResult(
      result: ConfirmationResultMessage,
      recipients: NonEmpty[Seq[ParticipantId]],
      sequencingTime: CantonTimestamp = CantonTimestamp.Epoch,
      overrideCryptoSnapshotMediator: Option[DomainSnapshotSyncCryptoApi] = None,
      overrideCryptoSnapshotSequencer: Option[DomainSnapshotSyncCryptoApi] = None,
      additionalEnvelopes: List[(ProtocolMessage, Recipients)] = Nil,
  )(implicit traceContext: TraceContext): EitherT[
    Future,
    DeliveredUnassignmentResult.InvalidUnassignmentResult,
    DeliveredUnassignmentResult,
  ] = {

    val cryptoSnapshotMediator = overrideCryptoSnapshotMediator.getOrElse(
      mediatorCryptoClient.value.currentSnapshotApproximation
    )

    val cryptoSnapshotSequencer = overrideCryptoSnapshotSequencer.getOrElse(
      sequencerCryptoClient.value.currentSnapshotApproximation
    )

    ReassignmentDataHelpers.unassignmentResult(
      result,
      recipients,
      protocolVersion,
      cryptoSnapshotMediator,
      cryptoSnapshotSequencer,
      sequencingTime,
    )(additionalEnvelopes = additionalEnvelopes)
  }

  def assignmentResult(): ConfirmationResultMessage =
    ConfirmationResultMessage.create(
      targetDomain.unwrap,
      ViewType.AssignmentViewType,
      RequestId(CantonTimestamp.Epoch),
      TestHash.dummyRootHash,
      Verdict.Approve(protocolVersion),
      Set(),
      protocolVersion,
    )
}

object ReassignmentDataHelpers {
  import org.scalatest.EitherValues.*

  def apply(
      contract: SerializableContract,
      sourceDomain: Source[DomainId],
      targetDomain: Target[DomainId],
      identityFactory: TestingIdentityFactory,
  )(implicit executionContext: ExecutionContext) = {
    val pureCrypto = identityFactory
      .forOwnerAndDomain(DefaultTestIdentities.mediatorId, sourceDomain.unwrap)
      .pureCrypto

    new ReassignmentDataHelpers(
      contract = contract,
      sourceDomain = sourceDomain,
      targetDomain = targetDomain,
      pureCrypto = pureCrypto,
      mediatorCryptoClient = Some(
        identityFactory
          .forOwnerAndDomain(DefaultTestIdentities.mediatorId, sourceDomain.unwrap)
      ),
      sequencerCryptoClient = Some(
        identityFactory
          .forOwnerAndDomain(DefaultTestIdentities.sequencerId, sourceDomain.unwrap)
      ),
    )
  }

  /**  From the result, constructs the DeliveredUnassignmentResult (mostly add mediator and sequencer signatures)
    */
  def unassignmentResult(
      result: ConfirmationResultMessage,
      recipients: NonEmpty[Seq[ParticipantId]],
      protocolVersion: ProtocolVersion,
      cryptoSnapshotMediator: DomainSnapshotSyncCryptoApi,
      cryptoSnapshotSequencer: DomainSnapshotSyncCryptoApi,
      sequencingTime: CantonTimestamp = CantonTimestamp.Epoch,
  )(
      domainId: DomainId = result.domainId,
      additionalEnvelopes: List[(ProtocolMessage, Recipients)] = Nil,
      additionalMediatorSignatures: Seq[Signature] = Nil,
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): EitherT[
    Future,
    DeliveredUnassignmentResult.InvalidUnassignmentResult,
    DeliveredUnassignmentResult,
  ] = {

    val signedResultF: Future[SignedProtocolMessage[ConfirmationResultMessage]] =
      SignedProtocolMessage
        .trySignAndCreate(result, cryptoSnapshotMediator, protocolVersion)
        .onShutdown(sys.error("aborted due to shutdown"))

    for {
      signedResult <- EitherT
        .liftF(signedResultF)
        .map(_.focus(_.signatures).modify(_ ++ additionalMediatorSignatures))

      deliveredUnassignmentResult <- addSequencerSignature(
        signedResult,
        recipients.map(MemberRecipient(_)),
        protocolVersion,
        cryptoSnapshotSequencer,
        sequencingTime,
      )(domainId, additionalEnvelopes)

    } yield deliveredUnassignmentResult
  }

  /** From the result, constructs the DeliveredUnassignmentResult (mostly add sequencer signature)
    */
  def addSequencerSignature(
      signedResult: SignedProtocolMessage[ConfirmationResultMessage],
      recipients: NonEmpty[Seq[Recipient]],
      protocolVersion: ProtocolVersion,
      cryptoSnapshotSequencer: DomainSnapshotSyncCryptoApi,
      sequencingTime: CantonTimestamp = CantonTimestamp.Epoch,
  )(
      domainId: DomainId = signedResult.domainId,
      additionalEnvelopes: List[(ProtocolMessage, Recipients)] = Nil,
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): EitherT[
    Future,
    DeliveredUnassignmentResult.InvalidUnassignmentResult,
    DeliveredUnassignmentResult,
  ] = {

    val allEnvelopes =
      (signedResult, Recipients.cc(recipients.head1, recipients.tail1*)) +: additionalEnvelopes

    val batch = Batch.of(protocolVersion, allEnvelopes*)

    val deliver = Deliver.create(
      SequencerCounter(0),
      sequencingTime,
      domainId,
      Some(MessageId.tryCreate("msg-0")),
      batch,
      None,
      protocolVersion,
      Option.empty[TrafficReceipt],
    )

    val hash = cryptoSnapshotSequencer.pureCrypto.digest(
      HashPurpose.SequencedEventSignature,
      deliver.getCryptographicEvidence,
    )

    val res = cryptoSnapshotSequencer
      .sign(hash)
      .value
      .onShutdown(sys.error("aborted due to shutdown"))
      .map(_.value)
      .map { signature =>
        DeliveredUnassignmentResult.create(
          SignedContent(
            deliver,
            signature,
            None,
            BaseTest.testedProtocolVersion,
          )
        )
      }

    EitherT(res)
  }
}
