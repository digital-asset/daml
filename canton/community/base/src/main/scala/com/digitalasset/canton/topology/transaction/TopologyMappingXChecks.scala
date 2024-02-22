// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.future.*
import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.crypto.KeyPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.OnboardingRestriction
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.PositiveStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyStoreX,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.Code
import com.digitalasset.canton.topology.{ParticipantId, UniqueIdentifier}
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
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingXChecks
    with NamedLogging {

  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {

    val checkOpt = (toValidate.mapping.code, inStore.map(_.mapping.code)) match {
      case (Code.DomainTrustCertificateX, None | Some(Code.DomainTrustCertificateX)) =>
        toValidate
          .selectMapping[DomainTrustCertificateX]
          .map(checkDomainTrustCertificate(effective, inStore.isEmpty, _))

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

  private def loadFromStore(
      effective: EffectiveTime,
      code: Code,
      filterUid: Option[Seq[UniqueIdentifier]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, PositiveStoredTopologyTransactionsX] =
    EitherT
      .right[TopologyTransactionRejection](
        store
          .findPositiveTransactions(
            effective.value,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(code),
            filterUid = filterUid,
            filterNamespace = None,
          )
      )

  private def checkDomainTrustCertificate(
      effective: EffectiveTime,
      isFirst: Boolean,
      toValidate: SignedTopologyTransactionX[TopologyChangeOpX, DomainTrustCertificateX],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] =
    if (toValidate.transaction.op == TopologyChangeOpX.Remove && isFirst) {
      EitherT.leftT(TopologyTransactionRejection.Other("Cannot have a remove as the first DTC"))
    } else if (toValidate.transaction.op == TopologyChangeOpX.Remove) {

      /* Checks that the DTC is not being removed if the participant still hosts a party.
       * This check is potentially quite expensive: we have to fetch all party to participant mappings, because
       * we cannot index by the hosting participants.
       */
      for {
        storedPartyToParticipantMappings <- loadFromStore(effective, PartyToParticipantX.code, None)
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
    } else if (isFirst) {

      // Checks if the participant is allowed to submit its domain trust certificate
      val participantId = toValidate.mapping.participantId
      for {
        domainParamCandidates <- loadFromStore(effective, DomainParametersStateX.code, None)
        restrictions = domainParamCandidates.result.view
          .flatMap(_.selectMapping[DomainParametersStateX])
          .collect { case tx =>
            tx.transaction.mapping.parameters.onboardingRestriction
          }
          .toList match {
          case Nil =>
            logger.error(
              "Can not determine the onboarding restriction. Assuming the domain is locked."
            )
            OnboardingRestriction.RestrictedLocked
          case param :: Nil => param
          case param :: rest =>
            logger.error(
              s"Multiple domain parameters at ${effective} ${rest.size + 1}. Using first one with restriction ${param}."
            )
            param
        }
        _ <- (restrictions match {
          case OnboardingRestriction.RestrictedLocked | OnboardingRestriction.UnrestrictedLocked =>
            logger.info(s"Rejecting onboarding of new participant ${toValidate.mapping}")
            EitherT.leftT(
              TopologyTransactionRejection
                .OnboardingRestrictionInPlace(
                  participantId,
                  restrictions,
                  None,
                ): TopologyTransactionRejection
            )
          case OnboardingRestriction.UnrestrictedOpen =>
            EitherT.rightT(())
          case OnboardingRestriction.RestrictedOpen =>
            loadFromStore(
              effective,
              ParticipantDomainPermissionX.code,
              filterUid = Some(Seq(toValidate.mapping.participantId.uid)),
            ).subflatMap { storedPermissions =>
              val isAllowlisted = storedPermissions.result.view
                .flatMap(_.selectMapping[ParticipantDomainPermissionX])
                .collectFirst {
                  case x if x.mapping.domainId == toValidate.mapping.domainId =>
                    x.mapping.loginAfter
                }
              isAllowlisted match {
                case Some(Some(loginAfter)) if loginAfter > effective.value =>
                  // this should not happen except under race conditions, as sequencers should not let participants login
                  logger.warn(
                    s"Rejecting onboarding of ${toValidate.mapping.participantId} as the participant still has a login ban until ${loginAfter}"
                  )
                  Left(
                    TopologyTransactionRejection
                      .OnboardingRestrictionInPlace(participantId, restrictions, Some(loginAfter))
                  )
                case Some(_) =>
                  logger.info(
                    s"Accepting onboarding of ${toValidate.mapping.participantId} as it is allow listed"
                  )
                  Right(())
                case None =>
                  logger.info(
                    s"Rejecting onboarding of ${toValidate.mapping.participantId} as it is not allow listed as of ${effective.value}"
                  )
                  Left(
                    TopologyTransactionRejection
                      .OnboardingRestrictionInPlace(participantId, restrictions, None)
                  )
              }
            }
        }): EitherT[Future, TopologyTransactionRejection, Unit]
      } yield ()
    } else {
      EitherTUtil.unit
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
      mapping.participants.count(_.permission >= ParticipantPermission.Confirmation)

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
