// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction.checks

import cats.Monad
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
import com.digitalasset.canton.topology.TopologyStateProcessor.MaybePending
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.RequiredMapping as RequiredMappingRejection
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import com.digitalasset.canton.topology.transaction.TopologyMapping.{Code, MappingHash}
import com.digitalasset.canton.topology.transaction.checks.TopologyMappingChecks.PendingChangesLookup
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*
import scala.reflect.ClassTag

object TopologyMappingChecks {
  type PendingChangesLookup = scala.collection.Map[MappingHash, MaybePending]

  class All(fst: TopologyMappingChecks, rest: TopologyMappingChecks*)(implicit
      executionContext: ExecutionContext
  ) extends TopologyMappingChecks {
    private val all = (fst +: rest).toList
    override def checkTransaction(
        effective: EffectiveTime,
        toValidate: GenericSignedTopologyTransaction,
        inStore: Option[GenericSignedTopologyTransaction],
        pendingChanges: PendingChangesLookup,
        relaxChecksForBackwardsCompatibility: Boolean,
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      MonadUtil.sequentialTraverse_(all)(
        _.checkTransaction(
          effective,
          toValidate,
          inStore,
          pendingChanges,
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
      pendingChanges: PendingChangesLookup,
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
      pendingChanges: PendingChangesLookup,
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    EitherTUtil.unitUS
}

trait MaybeEmptyTopologyStore {
  def store: TopologyStore[TopologyStoreId]
  def skipLoadingFromStore: Boolean
}

object MaybeEmptyTopologyStore {
  def apply(topologyStore: TopologyStore[TopologyStoreId]): MaybeEmptyTopologyStore =
    new MaybeEmptyTopologyStore {
      override val store: TopologyStore[TopologyStoreId] = topologyStore
      override val skipLoadingFromStore: Boolean = false
    }
}

abstract class TopologyMappingChecksWithStore(
    maybeStore: MaybeEmptyTopologyStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingChecks
    with NamedLogging {

  protected def store: TopologyStore[TopologyStoreId] = maybeStore.store

  @VisibleForTesting
  private[transaction] def loadFromStore[Op <: TopologyChangeOp](
      effective: EffectiveTime,
      codes: Set[Code],
      pendingChanges: Iterable[MaybePending],
      filterUid: Option[NonEmpty[Seq[UniqueIdentifier]]] = None,
      filterNamespace: Option[NonEmpty[Seq[Namespace]]] = None,
      op: Op = TopologyChangeOp.Replace,
  )(implicit
      traceContext: TraceContext,
      classTag: ClassTag[Op],
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Seq[
    SignedTopologyTransaction[Op, TopologyMapping]
  ]] = {
    val storeLookup = op match {
      case _ if maybeStore.skipLoadingFromStore =>
        FutureUnlessShutdown.pure(
          StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping](Seq.empty)
        )
      case _: TopologyChangeOp.Replace =>
        store
          .findPositiveTransactions(
            effective.value,
            asOfInclusive = false,
            isProposal = false,
            types = codes.toSeq,
            filterUid = filterUid,
            filterNamespace = filterNamespace,
          )
      case _: TopologyChangeOp.Remove =>
        store
          .findNegativeTransactions(
            effective.value,
            asOfInclusive = false,
            isProposal = false,
            types = codes.toSeq,
            filterUid = filterUid,
            filterNamespace = filterNamespace,
          )
    }
    EitherT
      .right[TopologyTransactionRejection](
        storeLookup
          .map { storedTxs =>
            val latestStored = storedTxs.collectLatestByUniqueKey.signedTransactions

            // we need to proactively look up the pending changes that match the filter,
            // because there might be a pending transaction that isn't in the store yet (eg. serial=1)
            val pendingChangesMatchingFilter =
              pendingChanges.view
                .filter { maybePending =>
                  val tx = maybePending.currentTx
                  // proposals shouldn't end up in PendingChangesLookup, but better to emulate what the store filter does
                  !tx.isProposal &&
                  codes.contains(tx.mapping.code) &&
                  filterNamespace.forall(_.exists(_ == tx.mapping.namespace)) &&
                  filterUid.forall(uids => tx.mapping.maybeUid.exists(uids.contains(_)))
                }
                .map(_.currentTx)
                .toSeq

            TopologyTransactions
              .collectLatestByUniqueKey(latestStored ++ pendingChangesMatchingFilter)
              .flatMap(_.selectOp[Op])
          }
      )
  }

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
    maybeStore: MaybeEmptyTopologyStore,
    parameters: Option[StaticSynchronizerParameters],
    loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingChecksWithStore(maybeStore, loggerFactory) {

  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      pendingChangesLookup: PendingChangesLookup,
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
              pendingChangesLookup,
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
              pendingChangesLookup,
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
            checkOwnerToKeyMappingRemove(effective, _, pendingChangesLookup)
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
              pendingChangesLookup,
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
              pendingChangesLookup,
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
              pendingChangesLookup,
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
              pendingChangesLookup,
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
      _ <- checkNoOngoingSynchronizerUpgrade(effective, toValidate, pendingChangesLookup)
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
      pendingChanges: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    val pendingSynchronizerAnnouncements = store.storeId.forSynchronizer.flatMap { synchronizerId =>
      pendingChanges.get(SynchronizerUpgradeAnnouncement.uniqueKey(synchronizerId.logical))
    }

    Monad[EitherT[FutureUnlessShutdown, TopologyTransactionRejection, *]].whenA(
      store.storeId.isSynchronizerStore
    )(for {
      results <- loadFromStore(
        effective,
        Set(Code.SynchronizerUpgradeAnnouncement),
        pendingSynchronizerAnnouncements.toList,
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
    } yield {})
  }

  private def loadSynchronizerParameters(
      effective: EffectiveTime,
      synchronizerId: SynchronizerId,
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, DynamicSynchronizerParameters] =
    loadFromStore(
      effective,
      Set(Code.SynchronizerParametersState),
      pendingChangesLookup.get(SynchronizerParametersState.uniqueKey(synchronizerId)).toList,
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
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    // Checks if the participant is allowed to submit its synchronizer trust certificate
    val participantId = toValidate.mapping.participantId

    def loadOnboardingRestriction()
        : EitherT[FutureUnlessShutdown, TopologyTransactionRejection, OnboardingRestriction] =
      loadSynchronizerParameters(effective, toValidate.mapping.synchronizerId, pendingChangesLookup)
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
        loadFromStore(
          effective,
          Set(Code.ParticipantSynchronizerPermission),
          pendingChangesLookup
            .get(
              ParticipantSynchronizerPermission.uniqueKey(
                toValidate.mapping.synchronizerId,
                toValidate.mapping.participantId,
              )
            )
            .toList,
          filterUid = Some(NonEmpty(Seq, toValidate.mapping.participantId.uid)),
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
      ptps <- loadFromStore(
        effective,
        Set(Code.PartyToParticipant),
        pendingChangesLookup.get(PartyToParticipant.uniqueKey(participantId.adminParty)).toList,
        filterUid = Some(NonEmpty(Seq, participantId.uid)),
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
        pendingChangesLookup = pendingChangesLookup,
        newMembers = Set(
          participantId
        ),
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
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    import toValidate.mapping
    def checkParticipants() = {
      val newParticipants = mapping.participants.map(_.participantId).toSet --
        inStore.toList.flatMap(_.mapping.participants.map(_.participantId))
      for {
        participantTransactions <- loadFromStore(
          effective,
          Set(Code.SynchronizerTrustCertificate, Code.OwnerToKeyMapping),
          (newParticipants.toSeq.map(_.uid) :+ mapping.partyId.uid).flatMap { uid =>
            val pid = ParticipantId(uid)
            val otks = pendingChangesLookup.get(OwnerToKeyMapping.uniqueKey(pid)).toList
            val dtcs = store.storeId.forSynchronizer.flatMap { synchronizerId =>
              pendingChangesLookup.get(
                SynchronizerTrustCertificate.uniqueKey(pid, synchronizerId.logical)
              )
            }
            otks ++ dtcs
          }.toList,
          filterUid = Some(NonEmpty(Seq, mapping.partyId.uid) ++ newParticipants.toSeq.map(_.uid)),
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
    def checkIsNotSelfSignedWithARevokedRootNSDKey() = {
      val pendingRevokedNamespaceDelegationsWithSameNamespaceKey = pendingChangesLookup
        .get(NamespaceDelegation.uniqueKey(mapping.namespace, mapping.namespace.fingerprint))
        .filter(_.currentTx.selectOp[Remove].isDefined)
        .toList

      for {
        revokedNamespaceDelegationsWithSameNamespace <- loadFromStore(
          effective,
          Set(Code.NamespaceDelegation),
          pendingRevokedNamespaceDelegationsWithSameNamespaceKey,
          filterNamespace = Some(NonEmpty(Seq, mapping.partyId.namespace)),
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
    }

    for {
      _ <- checkParticipants()
      _ <- checkIsNotSelfSignedWithARevokedRootNSDKey()
    } yield ()

  }

  /** Validate that OTK is no longer used by a synchronizer member */
  private def checkOwnerToKeyMappingRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Remove, OwnerToKeyMapping],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    toValidate.mapping.member match {
      case pid @ ParticipantId(uid) =>
        val pending = store.storeId.forSynchronizer.flatMap { synchronizerId =>
          pendingChangesLookup.get(
            SynchronizerTrustCertificate.uniqueKey(pid, synchronizerId.logical)
          )
        }.toList
        loadFromStore(
          effective,
          Set(Code.SynchronizerTrustCertificate),
          pending,
          filterUid = Some(NonEmpty.mk(Seq, uid)),
        ).map(_.filterNot(_.isProposal).headOption).subflatMap {
          case None => Right(())
          case Some(tx) =>
            Left(RequiredMappingRejection.InvalidOwnerToKeyMappingRemoval(pid, tx.transaction))
        }

      case mid: MediatorId =>
        loadFromStore(
          effective,
          Set(Code.MediatorSynchronizerState),
          pendingChangesLookup.values,
          filterUid =
            None, // synchronizer store will only show the ones of this synchronizer, but all groups
        ).map(
          _.filterNot(_.isProposal)
            .flatMap(_.selectMapping[MediatorSynchronizerState])
            .find(_.mapping.allMediatorsInGroup.contains(mid))
        ).subflatMap {
          case None => Right(())
          case Some(tx) =>
            Left(RequiredMappingRejection.InvalidOwnerToKeyMappingRemoval(mid, tx.transaction))
        }

      case sid: SequencerId =>
        loadFromStore(
          effective,
          Set(Code.SequencerSynchronizerState),
          pendingChangesLookup.values,
          filterUid = None, // synchronizer store will only show the ones of this synchronizer
        ).map(
          _.filterNot(_.isProposal)
            .flatMap(_.selectMapping[SequencerSynchronizerState])
            .find(_.mapping.allSequencers.contains(sid))
        ).subflatMap {
          case None => Right(())
          case Some(tx) =>
            Left(RequiredMappingRejection.InvalidOwnerToKeyMappingRemoval(sid, tx.transaction))
        }
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
      pendingChangesLookup: PendingChangesLookup,
      newMembers: Set[Member],
      skipCheck: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
    if (skipCheck) EitherTUtil.unitUS
    else
      NonEmpty.from(newMembers).fold(EitherTUtil.unitUS[TopologyTransactionRejection]) { members =>
        loadFromStore(
          effective,
          Set(Code.OwnerToKeyMapping),
          newMembers.flatMap { member =>
            pendingChangesLookup.get(OwnerToKeyMapping.uniqueKey(member)).toList
          }.toList,
          filterUid = Some(members.toSeq.map(_.uid)),
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
      pendingChangesLookup: PendingChangesLookup,
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    val newMediators = (toValidate.mapping.allMediatorsInGroup.toSet -- inStore.toList.flatMap(
      _.mapping.allMediatorsInGroup
    )).map(identity[Member])

    def checkMediatorNotAlreadyAssignedToOtherGroup() =
      for {
        result <- loadFromStore(
          effectiveTime,
          Set(Code.MediatorSynchronizerState),
          // TODO(#28232) this iterate over all should be gone once we have a proper state cache
          pendingChangesLookup.values,
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
      pendingChangesLookup,
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
      pendingChangesLookup: PendingChangesLookup,
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    val newSequencers = (toValidate.mapping.allSequencers.toSet -- inStore.toList.flatMap(
      _.mapping.allSequencers
    )).map(identity[Member])

    checkNewSynchronizerMembersHaveKeys(
      effectiveTime,
      pendingChangesLookup,
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
      pendingChangesLookup: PendingChangesLookup,
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
      loadFromStore(
        effective,
        Set(Code.NamespaceDelegation),
        pendingChangesLookup
          .get(
            NamespaceDelegation.uniqueKey(
              toValidate.mapping.namespace,
              toValidate.mapping.namespace.fingerprint,
            )
          )
          .toList,
        filterUid = None,
        filterNamespace = Some(NonEmpty(Seq, toValidate.mapping.namespace)),
      ).flatMap { namespaceDelegations =>
        EitherTUtil.condUnitET(
          namespaceDelegations.isEmpty,
          RequiredMappingRejection.NamespaceAlreadyInUse(toValidate.mapping.namespace),
        )
      }

    def checkOwnersAreNormalNamespaces()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      loadFromStore(
        effective,
        Set(Code.NamespaceDelegation),
        toValidate.mapping.owners.forgetNE.flatMap(ns =>
          pendingChangesLookup.get(NamespaceDelegation.uniqueKey(ns, ns.fingerprint))
        ),
        filterUid = None,
        filterNamespace = Some(toValidate.mapping.owners.toSeq),
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
      pendingChangesLookup: PendingChangesLookup,
      relaxChecksForBackwardsCompatibility: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = {
    def checkNoClashWithDecentralizedNamespaces()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] =
      loadFromStore(
        effective,
        Set(Code.DecentralizedNamespaceDefinition),
        pendingChangesLookup
          .get(
            DecentralizedNamespaceDefinition.uniqueKey(toValidate.mapping.namespace)
          )
          .toList,
        filterUid = None,
        filterNamespace = Some(NonEmpty(Seq, toValidate.mapping.namespace)),
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
  ): EitherT[FutureUnlessShutdown, TopologyTransactionRejection, Unit] = for {
    _ <- store.storeId.forSynchronizer match {
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
      store: TopologyStore[TopologyStoreId],
      parameters: Option[StaticSynchronizerParameters],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): RequiredTopologyMappingChecks =
    new RequiredTopologyMappingChecks(
      MaybeEmptyTopologyStore(store),
      parameters,
      loggerFactory,
    )
}
