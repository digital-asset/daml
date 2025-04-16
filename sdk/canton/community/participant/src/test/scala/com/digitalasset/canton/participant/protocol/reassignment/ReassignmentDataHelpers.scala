// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SigningKeyUsage,
  SynchronizerCryptoClient,
  SynchronizerCryptoPureApi,
  SynchronizerSnapshotSyncCryptoApi,
  TestHash,
}
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata, ViewType}
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
    sourceSynchronizer: Source[SynchronizerId],
    targetSynchronizer: Target[SynchronizerId],
    pureCrypto: SynchronizerCryptoPureApi,
    // mediatorCryptoClient and sequencerCryptoClient need to be defined for computation of the DeliveredUnassignmentResult
    mediatorCryptoClient: Option[SynchronizerCryptoClient] = None,
    sequencerCryptoClient: Option[SynchronizerCryptoClient] = None,
    targetTime: CantonTimestamp = CantonTimestamp.Epoch,
)(implicit executionContext: ExecutionContext) {
  import org.scalatest.OptionValues.*

  private val targetTimeProof: TimeProof = TimeProofTestUtil.mkTimeProof(
    timestamp = targetTime,
    targetSynchronizer = targetSynchronizer,
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
      LedgerUserId.assertFromString("tests"),
      workflowId = None,
    )

  def unassignmentRequest(
      submitter: LfPartyId,
      submittingParticipant: ParticipantId,
      sourceMediator: MediatorGroupRecipient,
      sourceProtocolVersion: ProtocolVersion = BaseTest.testedProtocolVersion,
  )(
      reassigningParticipants: Set[ParticipantId] = Set(submittingParticipant)
  ): UnassignmentRequest =
    UnassignmentRequest(
      submitterMetadata = submitterInfo(submitter, submittingParticipant),
      reassigningParticipants = reassigningParticipants,
      contract = contract,
      sourceSynchronizer = sourceSynchronizer,
      sourceProtocolVersion = Source(sourceProtocolVersion),
      sourceMediator = sourceMediator,
      targetSynchronizer = targetSynchronizer,
      targetProtocolVersion = Target(protocolVersion),
      targetTimeProof = targetTimeProof,
      reassignmentCounter = ReassignmentCounter(1),
    )

  def reassignmentData(
      reassignmentId: ReassignmentId,
      unassignmentRequest: UnassignmentRequest,
  ): UnassignmentData = {
    val uuid = new UUID(10L, 0L)
    val seed = seedGenerator.generateSaltSeed()

    val fullUnassignmentViewTree = unassignmentRequest
      .toFullUnassignmentTree(
        pureCrypto,
        pureCrypto,
        seed,
        uuid,
      )

    UnassignmentData(
      reassignmentId = reassignmentId,
      unassignmentRequest = fullUnassignmentViewTree,
      unassignmentDecisionTime = CantonTimestamp.ofEpochSecond(10),
      unassignmentResult = None,
    )
  }

  def unassignmentResult(
      reassignmentData: UnassignmentData
  )(implicit traceContext: TraceContext): EitherT[
    Future,
    DeliveredUnassignmentResult.InvalidUnassignmentResult,
    DeliveredUnassignmentResult,
  ] = {

    val result =
      ConfirmationResultMessage.create(
        synchronizerId = sourceSynchronizer.unwrap,
        viewType = ViewType.UnassignmentViewType,
        requestId = RequestId(reassignmentData.unassignmentTs),
        rootHash = reassignmentData.unassignmentRequest.rootHash,
        verdict = Verdict.Approve(protocolVersion),
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
      overrideCryptoSnapshotMediator: Option[SynchronizerSnapshotSyncCryptoApi] = None,
      overrideCryptoSnapshotSequencer: Option[SynchronizerSnapshotSyncCryptoApi] = None,
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
      targetSynchronizer.unwrap,
      ViewType.AssignmentViewType,
      RequestId(CantonTimestamp.Epoch),
      TestHash.dummyRootHash,
      Verdict.Approve(protocolVersion),
      protocolVersion,
    )
}

object ReassignmentDataHelpers {

  import org.scalatest.EitherValues.*

  def apply(
      contract: SerializableContract,
      sourceSynchronizer: Source[SynchronizerId],
      targetSynchronizer: Target[SynchronizerId],
      identityFactory: TestingIdentityFactory,
  )(implicit executionContext: ExecutionContext) = {
    val pureCrypto = identityFactory
      .forOwnerAndSynchronizer(DefaultTestIdentities.mediatorId, sourceSynchronizer.unwrap)
      .pureCrypto

    new ReassignmentDataHelpers(
      contract = contract,
      sourceSynchronizer = sourceSynchronizer,
      targetSynchronizer = targetSynchronizer,
      pureCrypto = pureCrypto,
      mediatorCryptoClient = Some(
        identityFactory
          .forOwnerAndSynchronizer(DefaultTestIdentities.mediatorId, sourceSynchronizer.unwrap)
      ),
      sequencerCryptoClient = Some(
        identityFactory
          .forOwnerAndSynchronizer(DefaultTestIdentities.sequencerId, sourceSynchronizer.unwrap)
      ),
    )
  }

  /** From the result, constructs the DeliveredUnassignmentResult (mostly add mediator and sequencer
    * signatures)
    */
  def unassignmentResult(
      result: ConfirmationResultMessage,
      recipients: NonEmpty[Seq[ParticipantId]],
      protocolVersion: ProtocolVersion,
      cryptoSnapshotMediator: SynchronizerSnapshotSyncCryptoApi,
      cryptoSnapshotSequencer: SynchronizerSnapshotSyncCryptoApi,
      sequencingTime: CantonTimestamp = CantonTimestamp.Epoch,
  )(
      synchronizerId: SynchronizerId = result.synchronizerId,
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
      )(synchronizerId, additionalEnvelopes)

    } yield deliveredUnassignmentResult
  }

  /** From the result, constructs the DeliveredUnassignmentResult (mostly add sequencer signature)
    */
  def addSequencerSignature(
      signedResult: SignedProtocolMessage[ConfirmationResultMessage],
      recipients: NonEmpty[Seq[Recipient]],
      protocolVersion: ProtocolVersion,
      cryptoSnapshotSequencer: SynchronizerSnapshotSyncCryptoApi,
      sequencingTime: CantonTimestamp = CantonTimestamp.Epoch,
  )(
      synchronizerId: SynchronizerId = signedResult.synchronizerId,
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
      None,
      sequencingTime,
      synchronizerId,
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
      .sign(hash, SigningKeyUsage.ProtocolOnly)
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
