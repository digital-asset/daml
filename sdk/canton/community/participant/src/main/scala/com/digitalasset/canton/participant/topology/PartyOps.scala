// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.bifunctor.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.{
  ExternalPartyAlreadyExists,
  IdentityManagerParentError,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.TopologyManagerError.{
  InconsistentTopologySnapshot,
  InvalidTopologyMapping,
  ParticipantErrorGroup,
}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class PartyOps(
    topologyManagerLookup: PhysicalSynchronizerId => Option[SynchronizerTopologyManager],
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def allocateParty(
      partyId: PartyId,
      participantId: ParticipantId,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    for {
      topologyManager <- EitherT.fromOption[FutureUnlessShutdown](
        topologyManagerLookup(synchronizerId),
        ParticipantTopologyManagerError.IdentityManagerParentError(
          TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(synchronizerId))
        ),
      )
      storedTransactions <- EitherT
        .right(
          topologyManager.store.findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(PartyToParticipant.code),
            filterUid = Some(NonEmpty(Seq, partyId.uid)),
            filterNamespace = None,
          )
        )

      uniqueByKey = storedTransactions
        .collectOfMapping[PartyToParticipant]
        .collectLatestByUniqueKey
      updateResult <- uniqueByKey.result match {
        case Seq() =>
          // no positive (i.e. REPLACE) transaction could mean:
          // 1. this party has never existed before
          // 2. this party has been created and deactivated (i.e. REMOVE)
          EitherT
            .fromEither[FutureUnlessShutdown](
              PartyToParticipant
                .create(
                  partyId,
                  threshold = PositiveInt.one,
                  participants =
                    Seq(HostingParticipant(participantId, ParticipantPermission.Submission)),
                )
            )
            .bimap(
              err =>
                ParticipantTopologyManagerError.IdentityManagerParentError(
                  InvalidTopologyMapping.Reject(err)
                ),
              // leaving serial to None, because in case of a REMOVE we let the serial
              // auto detection mechanism figure out the correct next serial
              ptp => (None, ptp),
            )

        case Seq(existingPtpTx) =>
          EitherT
            .fromEither[FutureUnlessShutdown](
              PartyToParticipant.create(
                existingPtpTx.mapping.partyId,
                existingPtpTx.mapping.threshold,
                existingPtpTx.mapping.participants
                  .filterNot(_.participantId == participantId) :+ HostingParticipant(
                  participantId,
                  ParticipantPermission.Submission,
                ),
              )
            )
            .bimap(
              err =>
                ParticipantTopologyManagerError.IdentityManagerParentError(
                  InvalidTopologyMapping.Reject(err)
                ),
              ptp => (Some(existingPtpTx.serial.increment), ptp),
            )

        case multiple =>
          EitherT.leftT[FutureUnlessShutdown, (Option[PositiveInt], PartyToParticipant)](
            ParticipantTopologyManagerError.IdentityManagerParentError(
              InconsistentTopologySnapshot
                .MultipleEffectiveMappingsPerUniqueKey(
                  Seq("multiple effective transactions at the same time" -> multiple)
                )
            )
          )
      }

      (nextSerial, updatedPTP) = updateResult

      _ <-
        topologyManager
          .proposeAndAuthorize(
            TopologyChangeOp.Replace,
            updatedPTP,
            serial = nextSerial,
            signingKeys = Seq.empty,
            synchronizerId.protocolVersion,
            expectFullAuthorization = true,
            waitToBecomeEffective = None,
          )
          .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
    } yield ()

  def allocateExternalParty(
      participantId: ParticipantId,
      externalPartyOnboardingDetails: ExternalPartyOnboardingDetails,
      synchronizerId: PhysicalSynchronizerId,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    for {
      topologyManager <- EitherT.fromOption[FutureUnlessShutdown](
        topologyManagerLookup(synchronizerId),
        ParticipantTopologyManagerError.IdentityManagerParentError(
          TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(synchronizerId))
        ),
      )
      // If the party already has a fully authorized P2P mapping, then it is allocated.
      // Since this function only supports allocation of fresh parties, we fail here.
      // Futher changes to the party topology should be handled via the admin API for now,
      // or through party replication for hosting relationship updates.
      // We can't rely on the topology manager failing with a MappingAlreadyExists here,
      // because the "add" method simply ignores duplicate transactions.
      // This is actually useful for us as it allows this endpoint to accept the same set of onboarding
      // transactions being submitted on all hosting nodes and makes multi-hosted party onboarding easier from a client
      // app.
      // However we still want to fail if the party is already allocated, hence this check.
      existingAuthorizedP2Ps <- EitherT
        .right(
          topologyManager.store.findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(PartyToParticipant.code),
            filterUid = Some(NonEmpty(Seq, externalPartyOnboardingDetails.partyId.uid)),
            filterNamespace = None,
          )
        )
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          existingAuthorizedP2Ps.result.isEmpty,
          (),
          ExternalPartyAlreadyExists.Failure(
            externalPartyOnboardingDetails.partyId,
            synchronizerId.logical,
          ),
        )
        .leftWiden[ParticipantTopologyManagerError]
      // Sign the party to participant tx with this participant
      // Validation that this participant is a hosting node should already be done in ExternalPartyOnboardingDetails
      // If somehow that's not done, authorization will fail in the topology manager
      partyToParticipantSignedO <-
        externalPartyOnboardingDetails.optionallySignedPartyToParticipant match {
          // If it's already signed, extend the signature
          case ExternalPartyOnboardingDetails.SignedPartyToParticipant(signed) =>
            topologyManager
              .extendSignature(
                signed,
                Seq(participantId.fingerprint),
                ForceFlags.none,
              )
              .map(Some(_))
              .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
          case ExternalPartyOnboardingDetails.UnsignedPartyToParticipant(unsigned) =>
            // Otherwise add the mapping as a proposal
            topologyManager
              .proposeAndAuthorize(
                op = TopologyChangeOp.Replace,
                mapping = unsigned.mapping,
                serial = Some(unsigned.serial),
                signingKeys = Seq(participantId.fingerprint),
                protocolVersion = synchronizerId.protocolVersion,
                expectFullAuthorization = false,
                waitToBecomeEffective = None,
              )
              .map(_ => None)
              .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
        }
      // Add all transactions at once
      _ <-
        topologyManager
          .add(
            externalPartyOnboardingDetails.partyNamespace.toList
              .flatMap(_.signedTransactions) ++ Seq(
              partyToParticipantSignedO
            ).flatten,
            ForceFlags.none,
            expectFullAuthorization = externalPartyOnboardingDetails.fullyAllocatesParty,
          )
          .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
    } yield ()

}

sealed trait ParticipantTopologyManagerError extends ContextualizedCantonError
object ParticipantTopologyManagerError extends ParticipantErrorGroup {

  final case class IdentityManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends ParticipantTopologyManagerError
      with ParentCantonError[TopologyManagerError] {
    override def logOnCreation: Boolean = false
  }

  @Explanation(
    """This error occurs when a request to allocate an external party is made for a party that already exists."""
  )
  @Resolution(
    """Allocate a new party with unique keys. If you're trying to change the hosting nodes of the party,
       follow the party replication procedure instead."""
  )
  object ExternalPartyAlreadyExists
      extends ErrorCode(
        id = "EXTERNAL_PARTY_ALREADY_EXISTS",
        ErrorCategory.InvalidGivenCurrentSystemStateResourceExists,
      ) {
    final case class Failure(partyId: PartyId, synchronizerId: SynchronizerId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Party $partyId already exists on synchronizer $synchronizerId"
        )
        with ParticipantTopologyManagerError
  }

}
