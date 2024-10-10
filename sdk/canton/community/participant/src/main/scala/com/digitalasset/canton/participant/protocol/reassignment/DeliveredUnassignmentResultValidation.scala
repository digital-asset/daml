// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.foldable.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.{CantonTimestamp, FullUnassignmentTree}
import com.digitalasset.canton.participant.protocol.reassignment.DeliveredUnassignmentResultValidation.*
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResultMessage,
  DeliveredUnassignmentResult,
}
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil}

import scala.concurrent.{ExecutionContext, Future}

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
  private val sourceDomainId = unassignmentRequest.sourceDomain
  private val reassigningParticipants = unassignmentRequest.reassigningParticipants

  def validate: EitherT[Future, Error, Unit] =
    for {
      _ <- confirmationResultMessageValidation
      _ <- deliverEventValidation
    } yield ()

  private def confirmationResultMessageValidation: EitherT[Future, Error, Unit] = for {
    _ <- validateDomainId(result.domainId)
    _ <- validateRequestId
    _ <- validateRootHash
    _ <- validateInformees
    _ <- validateSignatures
  } yield ()

  private def deliverEventValidation: EitherT[Future, Error, Unit] = {
    val deliver = deliveredUnassignmentResult.result.content

    for {
      _ <- validateDomainId(deliver.domainId)
      _ <- EitherTUtil
        .condUnitET[Future][Error](
          deliver.timestamp <= unassignmentDecisionTime,
          ResultTimestampExceedsDecisionTime(
            timestamp = deliver.timestamp,
            decisionTime = unassignmentDecisionTime,
          ),
        )
    } yield ()
  }

  private def validateDomainId(domainId: DomainId): EitherT[Future, Error, Unit] =
    EitherTUtil.condUnitET[Future](
      domainId == sourceDomainId.unwrap,
      IncorrectDomain(sourceDomainId.unwrap, domainId),
    )

  private def validateRequestId: EitherT[Future, Error, Unit] = {
    val expectedRequestId = RequestId(unassignmentRequestTs)

    EitherTUtil.condUnitET[Future](
      result.requestId == expectedRequestId,
      IncorrectRequestId(expectedRequestId, result.requestId),
    )
  }

  private def validateRootHash: EitherT[Future, Error, Unit] = {
    val expectedRootHash = unassignmentRequest.rootHash

    EitherTUtil.condUnitET[Future](
      result.rootHash == expectedRootHash,
      IncorrectRootHash(expectedRootHash, result.rootHash),
    )
  }

  private def validateInformees = {
    val partyToParticipantsSourceF =
      sourceTopology.unwrap.ipsSnapshot.activeParticipantsOfPartiesWithInfo(stakeholders.toSeq)
    val partyToParticipantsTargetF =
      targetTopology.unwrap.activeParticipantsOfPartiesWithInfo(stakeholders.toSeq)

    // Check that each stakeholder is hosted on at least one reassigning participant
    // TODO(#21072) Revisit when confirmation is required only from signatories
    val stakeholdersHostedReassigningParticipants: Future[Either[Error, Unit]] = for {
      partyToParticipantsSource <- partyToParticipantsSourceF
      partyToParticipantsTarget <- partyToParticipantsTargetF
      res = stakeholders.toSeq.traverse_ { stakeholder =>
        val hostingParticipantsSource =
          partyToParticipantsSource.get(stakeholder).map(_.participants.keySet).getOrElse(Set.empty)
        val hostingParticipantsTarget =
          partyToParticipantsTarget.get(stakeholder).map(_.participants.keySet).getOrElse(Set.empty)

        val hostingReassigningParticipants = hostingParticipantsSource
          .intersect(hostingParticipantsTarget)
          .intersect(reassigningParticipants)

        EitherUtil.condUnitE(
          hostingReassigningParticipants.nonEmpty,
          StakeholderNotHostedReassigningParticipant(stakeholder),
        )
      }
    } yield res

    for {
      _ <- EitherT(stakeholdersHostedReassigningParticipants)
      _ <- EitherTUtil.condUnitET[Future][Error](
        result.informees == stakeholders,
        IncorrectInformees(stakeholders, result.informees),
      )
    } yield ()
  }

  private def validateSignatures: EitherT[Future, Error, Unit] =
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
      expected: DomainId,
      found: DomainId,
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
