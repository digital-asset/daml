// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.future.*
import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.crypto.KeyPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyStoreX,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.Code
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordered.*

trait TopologyMappingXChecks {
  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit]
}

object NoopTopologyMappingXChecks extends TopologyMappingXChecks {
  override def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] =
    EitherTUtil.unit
}

class ValidatingTopologyMappingXChecks(
    store: TopologyStoreX[TopologyStoreId],
    loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingXChecks {

  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {

    val checkOpt = (toValidate.mapping.code, inStore.map(_.mapping.code)) match {
      case (Code.DomainTrustCertificateX, None | Some(Code.DomainTrustCertificateX)) =>
        toValidate
          .selectMapping[DomainTrustCertificateX]
          .map(checkDomainTrustCertificate(effective, _))

      case (Code.PartyToParticipantX, None | Some(Code.PartyToParticipantX)) =>
        toValidate
          .select[TopologyChangeOpX.Replace, PartyToParticipantX]
          .map(checkPartyToParticipant(_, inStore.flatMap(_.selectMapping[PartyToParticipantX])))

      case (Code.TrafficControlStateX, None | Some(Code.TrafficControlStateX)) =>
        toValidate
          .select[TopologyChangeOpX.Replace, TrafficControlStateX]
          .map(
            checkTrafficControl(
              _,
              inStore.flatMap(_.selectMapping[TrafficControlStateX]),
            )
          )

      case otherwise => None
    }
    checkOpt.getOrElse(EitherTUtil.unit)
  }

  /** Checks that the DTC is not being removed if the participant still hosts a party.
    * This check is potentially quite expensive: we have to fetch all party to participant mappings, because
    * we cannot index by the hosting participants.
    */
  private def checkDomainTrustCertificate(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransactionX[TopologyChangeOpX, DomainTrustCertificateX],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    if (toValidate.transaction.op == TopologyChangeOpX.Remove) {
      for {
        storedPartyToParticipantMappings <- EitherT
          .right[TopologyTransactionRejection](
            store
              .findPositiveTransactions(
                effective.value,
                asOfInclusive = false,
                isProposal = false,
                types = Seq(PartyToParticipantX.code),
                filterUid = None,
                filterNamespace = None,
              )
          )
        participantToOffboard = toValidate.mapping.participantId
        participantHostsParties = storedPartyToParticipantMappings.result.view
          .flatMap(_.selectMapping[PartyToParticipantX])
          .collect {
            case tx if tx.mapping.participants.exists(_.participantId == participantToOffboard) =>
              tx.mapping.partyId
          }
          .toSeq

        _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
          participantHostsParties.isEmpty,
          TopologyTransactionRejection.ParticipantStillHostsParties(
            participantToOffboard,
            participantHostsParties,
          ),
        )
      } yield ()

    } else {
      EitherTUtil.unit
    }
  }

  private val requiredKeyPurposes = Set(KeyPurpose.Encryption, KeyPurpose.Signing)

  /** Checks the following:
    * - threshold is less than or equal to the number of confirming participants
    * - new participants have a valid DTC
    * - new participants have an OTK with at least 1 signing key and 1 encryption key
    */
  private def checkPartyToParticipant(
      toValidate: SignedTopologyTransactionX[TopologyChangeOpX, PartyToParticipantX],
      inStore: Option[SignedTopologyTransactionX[TopologyChangeOpX, PartyToParticipantX]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, Unit] = {
    import toValidate.mapping
    val numConfirmingParticipants =
      mapping.participants.count(_.permission >= ParticipantPermissionX.Confirmation)

    for {
      // check the threshold
      _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        mapping.threshold.value <= numConfirmingParticipants,
        TopologyTransactionRejection.ThresholdTooHigh(
          mapping.threshold.value,
          numConfirmingParticipants,
        ),
      )

      newParticipants = mapping.participants.map(_.participantId).toSet --
        inStore.toList.flatMap(_.mapping.participants.map(_.participantId))
      participantTransactions <- EitherT.right[TopologyTransactionRejection](
        store
          .findPositiveTransactions(
            CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(DomainTrustCertificateX.code, OwnerToKeyMappingX.code),
            filterUid = Some(newParticipants.toSeq.map(_.uid)),
            filterNamespace = None,
          )
      )

      // check that all participants are known on the domain
      missingParticipantCertificates = newParticipants -- participantTransactions
        .collectOfMapping[DomainTrustCertificateX]
        .result
        .map(_.mapping.participantId)

      _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        missingParticipantCertificates.isEmpty,
        TopologyTransactionRejection.UnknownMembers(missingParticipantCertificates.toSeq),
      )

      // check that all known participants have keys registered
      participantsWithInsufficientKeys =
        newParticipants -- participantTransactions
          .collectOfMapping[OwnerToKeyMappingX]
          .result
          .view
          .filter { tx =>
            val keyPurposes = tx.mapping.keys.map(_.purpose).toSet
            requiredKeyPurposes.forall(keyPurposes)
          }
          .map(_.mapping.member)
          .collect { case pid: ParticipantId => pid }
          .toSeq

      _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        participantsWithInsufficientKeys.isEmpty,
        TopologyTransactionRejection.InsufficientKeys(participantsWithInsufficientKeys.toSeq),
      )
    } yield {
      ()
    }
  }

  /** Checks that the extraTrafficLimit is monotonically increasing */
  private def checkTrafficControl(
      toValidate: SignedTopologyTransactionX[TopologyChangeOpX.Replace, TrafficControlStateX],
      inStore: Option[SignedTopologyTransactionX[TopologyChangeOpX, TrafficControlStateX]],
  ): EitherT[Future, TopologyTransactionRejection, Unit] = {
    val minimumExtraTrafficLimit = inStore match {
      case None => PositiveLong.one
      case Some(TopologyChangeOpX(TopologyChangeOpX.Remove)) =>
        // if the transaction in the store is a removal, we "reset" the monotonicity requirement
        PositiveLong.one
      case Some(tx) => tx.mapping.totalExtraTrafficLimit
    }

    EitherTUtil.condUnitET(
      toValidate.mapping.totalExtraTrafficLimit >= minimumExtraTrafficLimit,
      TopologyTransactionRejection.ExtraTrafficLimitTooLow(
        toValidate.mapping.member,
        toValidate.mapping.totalExtraTrafficLimit,
        minimumExtraTrafficLimit,
      ),
    )

  }
}
