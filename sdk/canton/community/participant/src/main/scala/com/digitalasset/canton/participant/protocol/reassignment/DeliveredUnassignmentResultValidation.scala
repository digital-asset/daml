// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.foldable.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.reassignment.DeliveredUnassignmentResultValidation.{
  Error,
  *,
}
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResultMessage,
  DeliveredUnassignmentResult,
}
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.ExecutionContext

private[reassignment] final case class DeliveredUnassignmentResultValidation(
    unassignmentRequest: FullUnassignmentTree,
    unassignmentRequestTs: CantonTimestamp,
    unassignmentDecisionTime: CantonTimestamp,
    sourceTopology: Source[SyncCryptoApi],
    targetTopology: Target[TopologySnapshot],
)(
    deliveredUnassignmentResult: DeliveredUnassignmentResult
)(implicit executionContext: ExecutionContext, traceContext: TraceContext) {
  private def result: ConfirmationResultMessage = deliveredUnassignmentResult.unwrap

  private val stakeholders = unassignmentRequest.stakeholders
  private val sourceSynchronizerId = unassignmentRequest.sourceSynchronizer
  private val reassigningParticipants = unassignmentRequest.reassigningParticipants

  def validate: EitherT[FutureUnlessShutdown, Error, Unit] =
    for {
      _ <- confirmationResultMessageValidation
      _ <- deliverEventValidation
    } yield ()

  private def confirmationResultMessageValidation: EitherT[FutureUnlessShutdown, Error, Unit] =
    for {
      _ <- validateSynchronizerId(result.synchronizerId)
      _ <- validateRequestId
      _ <- validateRootHash
      _ <- validateInformees
      _ <- validateSignatures
    } yield ()

  private def deliverEventValidation: EitherT[FutureUnlessShutdown, Error, Unit] = {
    val deliver = deliveredUnassignmentResult.result.content

    for {
      _ <- validateSynchronizerId(deliver.synchronizerId)
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown][Error](
          deliver.timestamp <= unassignmentDecisionTime,
          ResultTimestampExceedsDecisionTime(
            timestamp = deliver.timestamp,
            decisionTime = unassignmentDecisionTime,
          ),
        )
    } yield ()
  }

  private def validateSynchronizerId(
      synchronizerId: SynchronizerId
  ): EitherT[FutureUnlessShutdown, Error, Unit] =
    EitherTUtil.condUnitET[FutureUnlessShutdown](
      synchronizerId == sourceSynchronizerId.unwrap,
      IncorrectDomain(sourceSynchronizerId.unwrap, synchronizerId),
    )

  private def validateRequestId: EitherT[FutureUnlessShutdown, Error, Unit] = {
    val expectedRequestId = RequestId(unassignmentRequestTs)

    EitherTUtil.condUnitET[FutureUnlessShutdown](
      result.requestId == expectedRequestId,
      IncorrectRequestId(expectedRequestId, result.requestId),
    )
  }

  private def validateRootHash: EitherT[FutureUnlessShutdown, Error, Unit] = {
    val expectedRootHash = unassignmentRequest.rootHash

    EitherTUtil.condUnitET[FutureUnlessShutdown](
      result.rootHash == expectedRootHash,
      IncorrectRootHash(expectedRootHash, result.rootHash),
    )
  }

  private def validateInformees: EitherT[FutureUnlessShutdown, Error, Unit] = {
    val partyToParticipantsSourceF =
      sourceTopology.unwrap.ipsSnapshot.activeParticipantsOfPartiesWithInfo(
        stakeholders.all.toSeq
      )
    val partyToParticipantsTargetF =
      targetTopology.unwrap.activeParticipantsOfPartiesWithInfo(stakeholders.all.toSeq)

    // Check that each stakeholder is hosted on at least one reassigning participant
    val stakeholdersHostedReassigningParticipants: FutureUnlessShutdown[Either[Error, Unit]] = for {
      partyToParticipantsSource <- partyToParticipantsSourceF
      partyToParticipantsTarget <- partyToParticipantsTargetF

      res = stakeholders.all.toSeq.traverse_ { stakeholder =>
        val hostingParticipantsSource =
          partyToParticipantsSource.get(stakeholder).map(_.participants.keySet).getOrElse(Set.empty)
        val hostingParticipantsTarget =
          partyToParticipantsTarget.get(stakeholder).map(_.participants.keySet).getOrElse(Set.empty)

        val hostingReassigningParticipants = hostingParticipantsSource
          .intersect(hostingParticipantsTarget)
          .intersect(reassigningParticipants)

        Either.cond(
          hostingReassigningParticipants.nonEmpty,
          (),
          StakeholderNotHostedReassigningParticipant(stakeholder),
        )
      }
    } yield res

    val expectedInformees =
      unassignmentRequest.stakeholders.all + unassignmentRequest.submitterMetadata.submittingAdminParty
    for {
      _ <- EitherT(stakeholdersHostedReassigningParticipants)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown][Error](
        result.informees == expectedInformees,
        IncorrectInformees(expectedInformees, result.informees),
      )
    } yield ()
  }

  private def validateSignatures: EitherT[FutureUnlessShutdown, Error, Unit] =
    for {
      // Mediators signatures on the confirmation result
      _ <- deliveredUnassignmentResult.signedConfirmationResult
        .verifyMediatorSignatures(
          sourceTopology.unwrap,
          unassignmentRequest.mediator.group,
        )
        .leftMap(err => IncorrectSignatures("mediators", err.show))

      // Sequencers signatures on the deliver message
      signatures = deliveredUnassignmentResult.result.signatures

      signedByteString = deliveredUnassignmentResult.result.content.getCryptographicEvidence
      hash = sourceTopology.unwrap.pureCrypto.digest(
        HashPurpose.SequencedEventSignature,
        signedByteString,
      )

      _ <- sourceTopology.unwrap
        .unsafePartialVerifySequencerSignatures(hash, signatures)
        .leftMap[Error](err => IncorrectSignatures("sequencers", err.show))
    } yield ()
}

object DeliveredUnassignmentResultValidation {
  sealed trait Error {
    def error: String
  }

  final case class IncorrectRequestId(
      expected: RequestId,
      found: RequestId,
  ) extends Error {
    override def error: String = s"Incorrect request id: found $found but expected $expected"
  }

  final case class IncorrectRootHash(
      expected: RootHash,
      found: RootHash,
  ) extends Error {
    override def error: String = s"Incorrect root hash: found $found but expected $expected"
  }

  final case class IncorrectDomain(
      expected: SynchronizerId,
      found: SynchronizerId,
  ) extends Error {
    override def error: String = s"Incorrect domain: found $found but expected $expected"
  }

  final case class IncorrectInformees(
      expected: Set[LfPartyId],
      found: Set[LfPartyId],
  ) extends Error {
    override def error: String =
      s"Informees should match the stakeholders. Found: $found but expected $expected"
  }

  final case class IncorrectSignatures(
      kind: String,
      msg: String,
  ) extends Error {
    override def error: String = s"Incorrect signatures for $kind: $msg"
  }

  final case class ResultTimestampExceedsDecisionTime(
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  ) extends Error {
    override def error: String = s"Result time $timestamp exceeds decision time $decisionTime"
  }

  final case class StakeholderNotHostedReassigningParticipant(
      stakeholder: LfPartyId
  ) extends Error {
    override def error: String =
      s"Stakeholder $stakeholder is not hosted on any reassigning participant"
  }
}
