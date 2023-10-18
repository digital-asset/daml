// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.parallel.*
import com.digitalasset.canton.crypto.KeyPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyStoreX,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*

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

    val checkDomainTrustCertificateX = toValidate
      .selectMapping[DomainTrustCertificateX]
      .map(checkDomainTrustCertificate(effective, _))
      .getOrElse(EitherTUtil.unit[TopologyTransactionRejection])

    val checkPartyToParticipantX = toValidate
      .select[TopologyChangeOpX.Replace, PartyToParticipantX]
      .map(checkPartyToParticipantMapping(_, inStore.flatMap(_.selectMapping[PartyToParticipantX])))
      .getOrElse(EitherTUtil.unit)

    Seq(checkDomainTrustCertificateX, checkPartyToParticipantX).parSequence_
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
          .toSet

        _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
          participantHostsParties.isEmpty,
          TopologyTransactionRejection.Other(
            s"Cannot remove domain trust certificate for $participantToOffboard because it still hosts parties ${participantHostsParties
                .mkString(",")}"
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
  private def checkPartyToParticipantMapping(
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

      newParticipants = mapping.participants.map(_.participantId.uid).toSet --
        inStore.toList.flatMap(_.mapping.participants.map(_.participantId.uid))
      participantTransactions <- EitherT.right[TopologyTransactionRejection](
        store
          .findPositiveTransactions(
            CantonTimestamp.MaxValue,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(DomainTrustCertificateX.code, OwnerToKeyMappingX.code),
            filterUid = Some(newParticipants.toSeq),
            filterNamespace = None,
          )
      )

      // check that all participants are known on the domain
      missingParticipantCertificates = newParticipants -- participantTransactions
        .collectOfMapping[DomainTrustCertificateX]
        .result
        .map(_.transaction.transaction.mapping.participantId.uid)

      _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        missingParticipantCertificates.isEmpty,
        TopologyTransactionRejection
          .Other(s"Participants ${missingParticipantCertificates} are not known"),
      )

      // check that all known participants have keys registered
      participantsWithInsufficientKeys =
        newParticipants -- participantTransactions
          .collectOfMapping[OwnerToKeyMappingX]
          .result
          .filter { tx =>
            val keyPurposes = tx.mapping.keys.map(_.purpose).toSet
            requiredKeyPurposes.forall(keyPurposes)
          }
          .map(_.mapping.member.uid)

      _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        participantsWithInsufficientKeys.isEmpty,
        TopologyTransactionRejection.Other(
          s"Participants ${participantsWithInsufficientKeys.mkString(",")} are missing a singing key and/or an encryption key"
        ),
      )
    } yield {
      ()
    }
  }

}
