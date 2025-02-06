// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.TopologyManagerError.{
  InconsistentTopologySnapshot,
  InvalidTopologyMapping,
  ParticipantErrorGroup,
}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

class PartyOps(
    topologyManager: AuthorizedTopologyManager,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def allocateParty(
      partyId: PartyId,
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, ParticipantTopologyManagerError, Unit] =
    for {
      storedTransactions <- EitherT
        .right(
          topologyManager.store.findPositiveTransactions(
            asOf = CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(PartyToParticipant.code),
            filterUid = Some(Seq(partyId.uid)),
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
              // leaving serial to None, because in case of a REMOVE we let the serial
              // auto detection mechanism figure out the correct next serial
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
            protocolVersion,
            expectFullAuthorization = true,
            waitToBecomeEffective = None,
          )
          .leftMap(IdentityManagerParentError(_): ParticipantTopologyManagerError)
    } yield ()

}

sealed trait ParticipantTopologyManagerError extends CantonError
object ParticipantTopologyManagerError extends ParticipantErrorGroup {

  final case class IdentityManagerParentError(parent: TopologyManagerError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends ParticipantTopologyManagerError
      with ParentCantonError[TopologyManagerError] {
    override def logOnCreation: Boolean = false
  }

}
