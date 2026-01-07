// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction.checks

import cats.data.EitherT
import cats.instances.order.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{EncryptionPublicKey, KeyPurpose, SigningPublicKey}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSynchronizerParameters,
  OnboardingRestriction,
  StaticSynchronizerParameters,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.RequiredMapping as RequiredMappingRejection
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

object TopologyMappingChecks {

  class All(fst: TopologyMappingChecks, rest: TopologyMappingChecks*)(implicit
      executionContext: ExecutionContext
  ) extends TopologyMappingChecks {
    private val all = (fst +: rest).toList
    override def checkTransaction(
        effective: EffectiveTime,
        toValidate: GenericSignedTopologyTransaction,
        inStore: Option[GenericSignedTopologyTransaction],
        relaxChecksForBackwardsCompatibility: Boolean,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      MonadUtil.sequentialTraverse_(all)(
        _.checkTransaction(
          effective,
          toValidate,
          inStore,
          relaxChecksForBackwardsCompatibility,
        )
      )
  }

}

trait TopologyMappingChecks {
  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit]
}

object NoopTopologyMappingChecks extends TopologyMappingChecks {
  override def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    EitherTUtil.unitUS
}

abstract class TopologyMappingChecksWithStateLookup(
    stateLookups: TopologyStateLookup,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingChecks
    with NamedLogging {

  @VisibleForTesting
  private[transaction] def loadFromStoreByNamespace(
      effective: EffectiveTime,
      codes: Set[Code],
      filterNamespace: NonEmpty[Seq[Namespace]],
      op: TopologyChangeOp = TopologyChangeOp.Replace,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Seq[
    GenericSignedTopologyTransaction
  ]] =
    EitherT.right(
      stateLookups
        .lookupForNamespaces(
          asOf = effective,
          asOfInclusive = true, //  note is true because the cache includes the "pending" updates
          filterNamespace,
          codes,
          op = op,
        )
        .map(_.toSeq.flatMap { case (_, tx) => tx.map(_.transaction) })
    )

  @VisibleForTesting
  private[transaction] def loadFromStoreByUid(
      effective: EffectiveTime,
      codes: Set[Code],
      filterUid: NonEmpty[Seq[UniqueIdentifier]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Seq[
    SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]
  ]] = EitherT.right(
    stateLookups
      .lookupForUids(asOf = effective, asOfInclusive = true, filterUid, codes)
      .map(_.toSeq.flatMap { case (_, tx) =>
        tx.flatMap(_.transaction.selectOp[TopologyChangeOp.Replace].toList)
      })
  )
}

/** Topology mapping checks which verify invariants on the topology state
  *
  * The following checks must be passed by every transaction which is added to the topology state.
  *
  * @param parameters
  *   verify state against static domain parameters (if they are known). we use this to ensure that
  *   the signing key specs are correct on a synchronizer.
  */
class RequiredTopologyMappingChecks(
    parameters: Option[StaticSynchronizerParameters],
    stateLookup: TopologyStateLookup,
    loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingChecksWithStateLookup(stateLookup, loggerFactory) {

  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    // Allow removal of root certificates even without prior existing positive transaction
    // This makes it possible to block a namespace from being used in an authorizing position in any topology mapping.
    lazy val isRootCertificateRemoval =
      toValidate.select[Remove, NamespaceDelegation].exists(NamespaceDelegation.isRootCertificate)
    val checkFirstIsNotRemove = EitherTUtil
      .condUnitET[FutureUnlessShutdown](
        isRootCertificateRemoval || !(toValidate.operation == TopologyChangeOp.Remove && inStore.isEmpty),
        RequiredMappingRejection.NoCorrespondingActiveTxToRevoke(toValidate.mapping),
      )
    val checkReplaceIsNotMaxSerial = EitherTUtil.condUnitET[FutureUnlessShutdown](
      toValidate.operation == TopologyChangeOp.Remove ||
        (toValidate.operation == TopologyChangeOp.Replace && toValidate.serial < PositiveInt.MaxValue),
      RequiredMappingRejection.InvalidTopologyMapping(
        s"The serial for a REPLACE must be less than ${PositiveInt.MaxValue}."
      ),
    )

    def mappingMismatch(expected: TopologyMapping): Boolean = (toValidate.mapping, expected) match {
      // When removing the synchronizer trust certificate, no need to mandate that the removal mapping has the same
      // feature flags..
      case (
            removeCertificate: SynchronizerTrustCertificate,
            inStoreCertificate: SynchronizerTrustCertificate,
          ) =>
        removeCertificate.uniqueKey != inStoreCertificate.uniqueKey
      case _ =>
        toValidate.mapping != expected
    }

    val checkRemoveDoesNotChangeMapping = EitherT.fromEither[FutureUnlessShutdown](
      inStore
        .collect {
          case expected
              if toValidate.operation == TopologyChangeOp.Remove && mappingMismatch(
                expected.mapping
              ) =>
            RequiredMappingRejection
              .RemoveMustNotChangeMapping(toValidate.mapping, expected.mapping)
        }
        .toLeft(())
    )

    lazy val checkOpt = (toValidate.mapping.code, inStore.map(_.mapping.code)) match {
      case (Code.SynchronizerTrustCertificate, None | Some(Code.SynchronizerTrustCertificate)) =>
        val checkReplace = toValidate
          .select[TopologyChangeOp.Replace, SynchronizerTrustCertificate]
          .map(
            checkSynchronizerTrustCertificateReplace(
              effective,
              _,
              inStore.flatMap(_.selectMapping[SynchronizerTrustCertificate]),
            )
          )

        checkReplace

      case (Code.PartyToParticipant, None | Some(Code.PartyToParticipant)) =>
        toValidate
          .select[TopologyChangeOp.Replace, PartyToParticipant]
          .map(
            checkPartyToParticipant(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, PartyToParticipant]),
            )
          )

      case (Code.OwnerToKeyMapping, None | Some(Code.OwnerToKeyMapping)) =>
        val checkReplace = toValidate
          .select[TopologyChangeOp.Replace, OwnerToKeyMapping]
          .map(
            checkOwnerToKeyMappingReplace(_, inStore.flatMap(_.selectMapping[OwnerToKeyMapping]))
          )

        val checkRemove = toValidate
          .select[TopologyChangeOp.Remove, OwnerToKeyMapping]
          .map(
            checkOwnerToKeyMappingRemove(effective, _)
          )

        checkReplace.orElse(checkRemove)

      case (Code.MediatorSynchronizerState, None | Some(Code.MediatorSynchronizerState)) =>
        toValidate
          .select[TopologyChangeOp.Replace, MediatorSynchronizerState]
          .map(
            checkMediatorSynchronizerStateReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, MediatorSynchronizerState]),
              relaxChecksForBackwardsCompatibility,
            )
          )
      case (Code.SequencerSynchronizerState, None | Some(Code.SequencerSynchronizerState)) =>
        toValidate
          .select[TopologyChangeOp.Replace, SequencerSynchronizerState]
          .map(
            checkSequencerSynchronizerStateReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, SequencerSynchronizerState]),
              relaxChecksForBackwardsCompatibility,
            )
          )

      case (
            Code.DecentralizedNamespaceDefinition,
            None | Some(Code.DecentralizedNamespaceDefinition),
          ) =>
        toValidate
          .select[TopologyChangeOp.Replace, DecentralizedNamespaceDefinition]
          .map(
            checkDecentralizedNamespaceDefinitionReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp, DecentralizedNamespaceDefinition]),
            )
          )

      case (
            Code.NamespaceDelegation,
            None | Some(Code.NamespaceDelegation),
          ) =>
        toValidate
          .select[TopologyChangeOp.Replace, NamespaceDelegation]
          .map(
            checkNamespaceDelegationReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Remove, NamespaceDelegation]),
              relaxChecksForBackwardsCompatibility,
            )
          )

      case (Code.SynchronizerParametersState, None | Some(Code.SynchronizerParametersState)) =>
        toValidate
          .select[TopologyChangeOp.Remove, SynchronizerParametersState]
          .map(_ =>
            EitherT.leftT[FutureUnlessShutdown, Unit](
              RequiredMappingRejection
                .CannotRemoveMapping(Code.SynchronizerParametersState): TopologyTransactionRejection
            )
          )

      case (
            Code.SynchronizerUpgradeAnnouncement,
            None | Some(Code.SynchronizerUpgradeAnnouncement),
          ) =>
        toValidate
          .select[TopologyChangeOp.Replace, SynchronizerUpgradeAnnouncement]
          .map(checkSynchronizerUpgradeAnnouncement(effective, _))

      case _otherwise => None
    }

    for {
      _ <- checkFirstIsNotRemove
      _ <- checkReplaceIsNotMaxSerial
      _ <- checkRemoveDoesNotChangeMapping
      _ <- checkNoOngoingSynchronizerUpgrade(effective, toValidate)
      _ <- checkOpt.getOrElse(EitherTUtil.unitUS)
    } yield ()

  }

  private val mappingsAllowedDuringSynchronizerUpgrade =
    TopologyMapping.Code.logicalSynchronizerUpgradeMappings

  /** Check that the topology state is not frozen if this store is a synchronizer store. All other
    * stores are not subject to freezing the topology state.
    */
  private def checkNoOngoingSynchronizerUpgrade(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    stateLookup.synchronizerId.fold(EitherTUtil.unitUS[TopologyTransactionRejection]) {
      synchronizerId =>
        for {
          results <- loadFromStoreByUid(
            effective,
            Set(Code.SynchronizerUpgradeAnnouncement),
            filterUid = NonEmpty.mk(Seq, synchronizerId.uid),
          )
          announcements = NonEmpty.from(
            results.flatMap(_.selectMapping[SynchronizerUpgradeAnnouncement].toList)
          )
          _ <- announcements match {
            case None => EitherTUtil.unitUS[TopologyTransactionRejection]
            case Some(announcement) =>
              EitherTUtil.condUnitET[FutureUnlessShutdown](
                mappingsAllowedDuringSynchronizerUpgrade.contains(toValidate.mapping.code),
                RequiredMappingRejection.OngoingSynchronizerUpgrade(
                  announcement.head1.mapping.successorSynchronizerId.logical
                ): TopologyTransactionRejection,
              )
          }
        } yield {}
    }

  private def loadSynchronizerParameters(
      effective: EffectiveTime,
      synchronizerId: SynchronizerId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, DynamicSynchronizerParameters] =
    loadFromStoreByUid(
      effective,
      Set(Code.SynchronizerParametersState),
      filterUid = NonEmpty.mk(Seq, synchronizerId.uid),
    )
      .subflatMap { synchronizerParamCandidates =>
        val params = synchronizerParamCandidates.view
          .flatMap(_.selectMapping[SynchronizerParametersState])
          .map(_.mapping.parameters)
          .toList
        params match {
          case Nil =>
            logger.error(
              "Can not determine synchronizer parameters."
            )
            Left(RequiredMappingRejection.MissingSynchronizerParameters(effective))
          case param :: Nil => Right(param)
          case param :: rest =>
            logger.error(
              s"Multiple synchronizer parameters at $effective ${rest.size + 1}. Using first one: $param."
            )
            Right(param)
        }
      }

  private def checkSynchronizerTrustCertificateReplace(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, SynchronizerTrustCertificate],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp, SynchronizerTrustCertificate]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    // Checks if the participant is allowed to submit its synchronizer trust certificate
    val participantId = toValidate.mapping.participantId

    def loadOnboardingRestriction()
        : EitherT[FutureUnlessShutdown, TopologyTransactionRejection, OnboardingRestriction] =
      loadSynchronizerParameters(effective, toValidate.mapping.synchronizerId)
        .map(_.onboardingRestriction)

    def checkSynchronizerIsNotLocked(restriction: OnboardingRestriction) =
      EitherTUtil.condUnitET[FutureUnlessShutdown](
        restriction.isOpen, {
          logger.info(
            s"Synchronizer is locked at $effective. Rejecting onboarding of new participant ${toValidate.mapping}"
          )
          RequiredMappingRejection.OnboardingRestrictionInPlace(
            participantId,
            restriction,
            None,
          )
        },
      )

    def checkParticipantIsNotRestricted(
        restrictions: OnboardingRestriction
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      // using the flags to check for restrictions instead of == UnrestrictedOpen to be more
      // future proof in case we will add additional restrictions in the future and would miss a case,
      // because there is no exhaustiveness check without full pattern matching
      if (restrictions.isUnrestricted && restrictions.isOpen) {
        // No further checks to be done. any participant can join the synchronizer
        EitherTUtil.unitUS
      } else if (restrictions.isRestricted && restrictions.isOpen) {
        // Only participants with explicit permission may join the synchronizer
        loadFromStoreByUid(
          effective,
          Set(Code.ParticipantSynchronizerPermission),
          filterUid = NonEmpty(Seq, toValidate.mapping.participantId.uid),
        ).subflatMap { storedPermissions =>
          val isAllowlisted = storedPermissions.view
            .flatMap(_.selectMapping[ParticipantSynchronizerPermission])
            .collectFirst {
              case x if x.mapping.synchronizerId == toValidate.mapping.synchronizerId =>
                x.mapping.loginAfter
            }
          isAllowlisted match {
            case Some(Some(loginAfter)) if loginAfter > effective.value =>
              // this should not happen except under race conditions, as sequencers should not let participants login
              logger.warn(
                s"Rejecting onboarding of ${toValidate.mapping.participantId} as the participant still has a login ban until $loginAfter"
              )
              Left(
                RequiredMappingRejection
                  .OnboardingRestrictionInPlace(participantId, restrictions, Some(loginAfter))
              )
            case Some(_) =>
              logger.info(
                s"Accepting onboarding of ${toValidate.mapping.participantId} as it is allow listed"
              )
              Either.unit
            case None =>
              logger.info(
                s"Rejecting onboarding of ${toValidate.mapping.participantId} as it is not allow listed as of ${effective.value}"
              )
              Left(
                RequiredMappingRejection
                  .OnboardingRestrictionInPlace(participantId, restrictions, None)
              )
          }
        }
      } else {
        EitherT.leftT(
          RequiredMappingRejection
            .OnboardingRestrictionInPlace(participantId, restrictions, None)
        )
      }

    def checkPartyIdDoesntExist() = for {
      ptps <- loadFromStoreByUid(
        effective,
        Set(Code.PartyToParticipant),
        filterUid = NonEmpty(Seq, participantId.uid),
      )
      conflictingPartyIdO = ptps
        .flatMap(_.selectMapping[PartyToParticipant])
        .headOption
        .map(_.mapping)
      _ <- conflictingPartyIdO match {
        case Some(ptp) =>
          isExplicitAdminPartyAllocation(
            ptp,
            RequiredMappingRejection.ParticipantIdConflictWithPartyId(
              participantId,
              ptp.partyId,
            ),
          )
        case None => EitherTUtil.unitUS[TopologyTransactionRejection]
      }
    } yield ()

    def checkParticipantDoesNotRejoin() = EitherTUtil.condUnitET[FutureUnlessShutdown](
      inStore.forall(_.operation != TopologyChangeOp.Remove),
      RequiredMappingRejection.ParticipantCannotRejoinSynchronizer(
        toValidate.mapping.participantId
      ),
    )

    def participantHasKeys() =
      checkNewSynchronizerMembersHaveKeys(
        effective,
        newMembers = Set(participantId),
        skipCheck = false,
      )

    for {
      _ <- checkParticipantDoesNotRejoin()
      _ <- checkPartyIdDoesntExist()
      restriction <- loadOnboardingRestriction()
      _ <- checkSynchronizerIsNotLocked(restriction)
      _ <- checkParticipantIsNotRestricted(restriction)
      _ <- participantHasKeys()
    } yield ()
  }

  /** Checks the following:
    *   - threshold is less than or equal to the number of confirming participants
    *   - new participants have a valid DTC
    *   - new participants have an OTK (valid keys are checked as part of OTK checks)
    */
  private def checkPartyToParticipant(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    import toValidate.mapping
    def checkParticipants() = {
      val newParticipants = mapping.participants.map(_.participantId).toSet --
        inStore.toList.flatMap(_.mapping.participants.map(_.participantId))
      for {
        participantTransactions <- loadFromStoreByUid(
          effective,
          Set(Code.SynchronizerTrustCertificate, Code.OwnerToKeyMapping),
          filterUid = NonEmpty(Seq, mapping.partyId.uid) ++ newParticipants.toSeq.map(_.uid),
        )
        // if we found a DTC with the same uid as the partyId,
        // check that the PTP is an explicit admin party allocation, otherwise reject the PTP
        foundAdminPartyWithSameUID = participantTransactions
          .flatMap(_.selectMapping[SynchronizerTrustCertificate])
          .exists(_.mapping.participantId.uid == mapping.partyId.uid)
        _ <- EitherTUtil.ifThenET(foundAdminPartyWithSameUID)(
          isExplicitAdminPartyAllocation(
            mapping,
            RequiredMappingRejection.PartyIdConflictWithAdminParty(
              mapping.partyId
            ),
          )
        )

        // check that all participants are known on the synchronizer
        // note that this check does not provide strong guarantees as it is only
        // checked at time of creation. a removal of a participant may still
        // lead to dangling party to participant mappings.
        missingParticipantCertificates = newParticipants -- participantTransactions
          .flatMap(_.selectMapping[SynchronizerTrustCertificate])
          .map(_.mapping.participantId)

        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
          missingParticipantCertificates.isEmpty,
          RequiredMappingRejection.UnknownMembers(missingParticipantCertificates.toSeq),
        )

        // check that all known participants have keys registered
        // note same comment as above.
        participantsWithInsufficientKeys =
          newParticipants -- participantTransactions
            .flatMap(_.selectMapping[OwnerToKeyMapping])
            .map(_.mapping.member)
            .collect { case pid: ParticipantId => pid }

        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
          participantsWithInsufficientKeys.isEmpty,
          RequiredMappingRejection.InsufficientKeys(
            participantsWithInsufficientKeys.toSeq
          ),
        )
      } yield {
        ()
      }
    }

    // We disallow self signing with a key for which there's a revoked NamespaceDelegation
    def checkIsNotSelfSignedWithARevokedRootNSDKey() =
      for {
        revokedNamespaceDelegationsWithSameNamespace <- loadFromStoreByNamespace(
          effective,
          Set(Code.NamespaceDelegation),
          filterNamespace = NonEmpty(Seq, mapping.partyId.namespace),
          op = Remove,
        )
        hasRevokedRootNamespaceDelegationsWithSameNamespace =
          revokedNamespaceDelegationsWithSameNamespace
            .flatMap(_.selectMapping[NamespaceDelegation])
            .exists(NamespaceDelegation.isRootCertificate)
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
          !mapping.isSelfSigned || !hasRevokedRootNamespaceDelegationsWithSameNamespace,
          RequiredMappingRejection.NamespaceHasBeenRevoked(mapping.partyId.namespace),
        )
      } yield ()

    for {
      _ <- checkParticipants()
      _ <- checkIsNotSelfSignedWithARevokedRootNSDKey()
    } yield ()

  }

  /** Validate that OTK is no longer used by a synchronizer member */
  private def checkOwnerToKeyMappingRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Remove, OwnerToKeyMapping],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    (toValidate.mapping.member, stateLookup.synchronizerId) match {
      case (pid @ ParticipantId(uid), _) =>
        loadFromStoreByUid(
          effective,
          Set(Code.SynchronizerTrustCertificate),
          filterUid = NonEmpty.mk(Seq, uid),
        ).map(_.filterNot(_.isProposal).headOption).subflatMap {
          case None => Right(())
          case Some(tx) =>
            Left(RequiredMappingRejection.InvalidOwnerToKeyMappingRemoval(pid, tx.transaction))
        }
      case (mid: MediatorId, Some(synchronizerId)) =>
        loadFromStoreByUid(
          effective,
          Set(Code.MediatorSynchronizerState),
          filterUid = NonEmpty.mk(Seq, synchronizerId.uid),
        ).map(
          _.filterNot(_.isProposal)
            .flatMap(_.selectMapping[MediatorSynchronizerState])
            .find(_.mapping.allMediatorsInGroup.contains(mid))
        ).subflatMap {
          case None => Right(())
          case Some(tx) =>
            Left(RequiredMappingRejection.InvalidOwnerToKeyMappingRemoval(mid, tx.transaction))
        }
      case (sid: SequencerId, Some(synchronizerId)) =>
        loadFromStoreByUid(
          effective,
          Set(Code.SequencerSynchronizerState),
          filterUid = NonEmpty.mk(Seq, synchronizerId.uid),
        ).map(
          _.filterNot(_.isProposal)
            .flatMap(_.selectMapping[SequencerSynchronizerState])
            .find(_.mapping.allSequencers.contains(sid))
        ).subflatMap {
          case None => Right(())
          case Some(tx) =>
            Left(RequiredMappingRejection.InvalidOwnerToKeyMappingRemoval(sid, tx.transaction))
        }
      case (_, None) => EitherTUtil.unitUS
    }

  private def checkOwnerToKeyMappingReplace(
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, OwnerToKeyMapping],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp, OwnerToKeyMapping]],
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {

    // cannot re-add after remove
    val noAddingAfterRemove =
      EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
        inStore.forall(p => p.operation == TopologyChangeOp.Replace),
        TopologyTransactionRejection.RequiredMapping.CannotReregisterKeys(toValidate.mapping.member),
      )

    // check for at least 1 signing and 1 encryption key
    val keysByPurpose = toValidate.mapping.keys.forgetNE.groupBy(_.purpose)
    val allSigningKeys = keysByPurpose.getOrElse(KeyPurpose.Signing, Seq.empty)
    val signingKeys = allSigningKeys.collect {
      case c: SigningPublicKey
          if parameters.forall(_.requiredSigningSpecs.keys.contains(c.keySpec)) =>
        c
    }

    val minimumSigningKeyRequirement =
      EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
        // all nodes require signing keys
        signingKeys.nonEmpty,
        RequiredMappingRejection.InvalidOwnerToKeyMapping(
          toValidate.mapping.member,
          keyType = "signing",
          allSigningKeys,
          parameters
            .map(_.requiredSigningSpecs.keys.map(_.name).forgetNE.toSeq)
            .getOrElse(Seq("any spec")),
        ),
      )

    val allEncryptionKeys = keysByPurpose.getOrElse(KeyPurpose.Encryption, Seq.empty)
    val encryptionKeys = allEncryptionKeys.collect {
      case c: EncryptionPublicKey
          if parameters.forall(_.requiredEncryptionSpecs.keys.contains(c.keySpec)) =>
        c
    }
    val isParticipant = toValidate.mapping.member.code == ParticipantId.Code

    val minimumEncryptionKeyRequirement =
      EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
        // all nodes require signing keys
        // non-participants don't need encryption keys
        !isParticipant || encryptionKeys.nonEmpty,
        RequiredMappingRejection.InvalidOwnerToKeyMapping(
          toValidate.mapping.member,
          keyType = "encryption",
          provided = allEncryptionKeys,
          supported = parameters
            .map(_.requiredEncryptionSpecs.keys.map(_.name).forgetNE.toSeq)
            .getOrElse(Seq("any spec")),
        ),
      )
    noAddingAfterRemove
      .flatMap(_ => minimumSigningKeyRequirement)
      .flatMap(_ => minimumEncryptionKeyRequirement)
  }

  private def checkNewSynchronizerMembersHaveKeys(
      effective: EffectiveTime,
      newMembers: Set[Member],
      skipCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    if (skipCheck) EitherTUtil.unitUS
    else
      NonEmpty.from(newMembers).fold(EitherTUtil.unitUS[TopologyTransactionRejection]) { members =>
        loadFromStoreByUid(
          effective,
          Set(Code.OwnerToKeyMapping),
          filterUid = members.toSeq.map(_.uid),
        ).flatMap { stored =>
          val found = stored
            .flatMap(_.selectMapping[OwnerToKeyMapping])
            .filterNot(_.isProposal)
            .map(_.mapping.member)
            .toSet
          val noKeys = newMembers -- found
          EitherTUtil.condUnitET[FutureUnlessShutdown](
            noKeys.isEmpty,
            RequiredMappingRejection
              .InsufficientKeys(noKeys.toSeq): TopologyTransactionRejection,
          )
        }
      }

  private def checkMediatorSynchronizerStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState],
      inStore: Option[
        SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorSynchronizerState]
      ],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    val newMediators = (toValidate.mapping.allMediatorsInGroup.toSet -- inStore.toList.flatMap(
      _.mapping.allMediatorsInGroup
    )).map(identity[Member])

    def checkMediatorNotAlreadyAssignedToOtherGroup() =
      for {
        result <- loadFromStoreByUid(
          effectiveTime,
          Set(Code.MediatorSynchronizerState),
          filterUid = NonEmpty.mk(Seq, toValidate.mapping.synchronizerId.uid),
        )
        mediatorsAlreadyAssignedToGroups = result
          .flatMap(_.selectMapping[MediatorSynchronizerState])
          // only look at other groups to avoid a race between validating this proposal and
          // having persisted the same transaction as fully authorized from other synchronizer owners.
          .filter(_.mapping.group != toValidate.mapping.group)
          .flatMap(tx =>
            tx.mapping.allMediatorsInGroup.collect {
              case med if newMediators.contains(med) => med -> tx.mapping.group
            }
          )
          .toMap
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          mediatorsAlreadyAssignedToGroups.isEmpty,
          RequiredMappingRejection.MediatorsAlreadyInOtherGroups(
            toValidate.mapping.group,
            mediatorsAlreadyAssignedToGroups,
          ): TopologyTransactionRejection,
        )
      } yield ()

    val notAlreadyAssignedET = checkMediatorNotAlreadyAssignedToOtherGroup()
    val allNewHaveKeysET = checkNewSynchronizerMembersHaveKeys(
      effectiveTime,
      newMembers = newMediators,
      relaxChecksForBackwardsCompatibility,
    )

    for {
      _ <- notAlreadyAssignedET
      _ <- allNewHaveKeysET
    } yield ()
  }

  private def checkSequencerSynchronizerStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState],
      inStore: Option[
        SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerSynchronizerState]
      ],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    val newSequencers = (toValidate.mapping.allSequencers.toSet -- inStore.toList.flatMap(
      _.mapping.allSequencers
    )).map(identity[Member])

    checkNewSynchronizerMembersHaveKeys(
      effectiveTime,
      newMembers = newSequencers,
      relaxChecksForBackwardsCompatibility: Boolean,
    )

  }

  private def checkDecentralizedNamespaceDefinitionReplace(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[
        TopologyChangeOp.Replace,
        DecentralizedNamespaceDefinition,
      ],
      inStore: Option[SignedTopologyTransaction[
        TopologyChangeOp,
        DecentralizedNamespaceDefinition,
      ]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {

    def checkDecentralizedNamespaceDerivedFromOwners()
        : EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      if (inStore.isEmpty) {
        // The very first decentralized namespace definition must have namespace computed from the owners
        EitherTUtil.condUnitET(
          toValidate.mapping.namespace == DecentralizedNamespaceDefinition
            .computeNamespace(toValidate.mapping.owners),
          RequiredMappingRejection.InvalidTopologyMapping(
            s"The decentralized namespace ${toValidate.mapping.namespace} is not derived from the owners ${toValidate.mapping.owners.toSeq.sorted}"
          ),
        )
      } else {
        EitherTUtil.unitUS
      }

    def checkNoClashWithNamespaceDelegations()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      loadFromStoreByNamespace(
        effective,
        Set(Code.NamespaceDelegation),
        filterNamespace = NonEmpty(Seq, toValidate.mapping.namespace),
      ).flatMap { namespaceDelegations =>
        EitherTUtil.condUnitET(
          namespaceDelegations.isEmpty,
          RequiredMappingRejection.NamespaceAlreadyInUse(toValidate.mapping.namespace),
        )
      }

    def checkOwnersAreNormalNamespaces()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      loadFromStoreByNamespace(
        effective,
        Set(Code.NamespaceDelegation),
        filterNamespace = toValidate.mapping.owners.toSeq,
      ).flatMap { namespaceDelegations =>
        val foundNSDs = namespaceDelegations
          .filter(NamespaceDelegation.isRootCertificate)
          .map(_.mapping.namespace)
          .toSet
        val missingNSDs = toValidate.mapping.owners -- foundNSDs

        EitherTUtil.condUnitET(
          missingNSDs.isEmpty,
          RequiredMappingRejection.InvalidTopologyMapping(
            s"No root certificate found for ${missingNSDs.toSeq.sorted.mkString(", ")}"
          ),
        )
      }

    for {
      _ <- checkDecentralizedNamespaceDerivedFromOwners()
      _ <- checkNoClashWithNamespaceDelegations()
      _ <- checkOwnersAreNormalNamespaces()
    } yield ()
  }

  private def checkNamespaceDelegationReplace(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[
        TopologyChangeOp.Replace,
        NamespaceDelegation,
      ],
      revokedInStore: Option[SignedTopologyTransaction[
        TopologyChangeOp.Remove,
        NamespaceDelegation,
      ]],
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    def checkNoClashWithDecentralizedNamespaces()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      loadFromStoreByNamespace(
        effective,
        Set(Code.DecentralizedNamespaceDefinition),
        filterNamespace = NonEmpty(Seq, toValidate.mapping.namespace),
      ).flatMap { dns =>
        val foundDecentralizedNamespaceWithSameNamespace = dns.nonEmpty
        EitherTUtil.condUnitET(
          !foundDecentralizedNamespaceWithSameNamespace,
          RequiredMappingRejection.NamespaceAlreadyInUse(toValidate.mapping.namespace),
        )
      }

    def checkKeyWasNotPreviouslyRevoked()
        : EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      EitherT.cond(
        revokedInStore.isEmpty,
        (),
        RequiredMappingRejection.NamespaceHasBeenRevoked(toValidate.mapping.namespace),
      )

    for {
      _ <- checkNoClashWithDecentralizedNamespaces()
      _ <-
        if (relaxChecksForBackwardsCompatibility)
          EitherT.pure[FutureUnlessShutdown, TopologyTransactionRejection](())
        else checkKeyWasNotPreviouslyRevoked()
    } yield ()
  }

  private def checkSynchronizerUpgradeAnnouncement(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[
        TopologyChangeOp.Replace,
        SynchronizerUpgradeAnnouncement,
      ],
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    for {
      _ <- stateLookup.synchronizerId match {
        case Some(psid) =>
          EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
            psid < toValidate.mapping.successorSynchronizerId,
            RequiredMappingRejection.InvalidSynchronizerSuccessor(
              psid,
              toValidate.mapping.successorSynchronizerId,
            ),
          )
        case None => EitherTUtil.unitUS
      }
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown][TopologyTransactionRejection](
        toValidate.mapping.upgradeTime > effective.value,
        RequiredMappingRejection.InvalidUpgradeTime(
          toValidate.mapping.successorSynchronizerId.logical,
          effective = effective,
          upgradeTime = toValidate.mapping.upgradeTime,
        ),
      )

    } yield ()

  /** Checks whether the given PTP is considered an explicit admin party allocation. This is true if
    * all following conditions are met:
    *   - threshold == 1
    *   - there is only a single hosting participant
    *     - with Submission permission
    *     - participantId.adminParty == partyId
    *
    * We do need the admin party in the protocol such that the participant always sees all requests
    * and can thereby prevent replay of submissions.
    */
  private def isExplicitAdminPartyAllocation(
      ptp: PartyToParticipant,
      rejection: => TopologyTransactionRejection,
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    // check that the PTP doesn't try to allocate a party that is the same as an already existing admin party.
    // we allow an explicit allocation of an admin like party though on the same participant
    val singleHostingParticipant =
      ptp.participants.sizeCompare(1) == 0

    val partyIsAdminParty =
      ptp.participants.forall(participant =>
        participant.participantId.adminParty == ptp.partyId &&
          participant.permission == ParticipantPermission.Submission
      )

    // technically we don't need to check for threshold == 1, because we already require that there is only a single participant
    // and the threshold may not exceed the number of participants. this is checked in PartyToParticipant.create
    val threshold1 = ptp.threshold == PositiveInt.one

    EitherTUtil.condUnitET[FutureUnlessShutdown](
      singleHostingParticipant && partyIsAdminParty && threshold1,
      rejection,
    )
  }

}

object RequiredTopologyMappingChecks {
  def apply(
      parameters: Option[StaticSynchronizerParameters],
      stateLookup: TopologyStateLookup,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): RequiredTopologyMappingChecks =
    new RequiredTopologyMappingChecks(
      parameters,
      stateLookup,
      loggerFactory,
    )
}
