// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.future.*
import cats.instances.order.*
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.KeyPurpose
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, OnboardingRestriction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.*
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.{
  InvalidTopologyMapping,
  NamespaceAlreadyInUse,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.{Code, MappingHash}
import com.digitalasset.canton.topology.transaction.TopologyMappingChecks.PendingChangesLookup
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

object TopologyMappingChecks {
  type PendingChangesLookup = Map[MappingHash, GenericSignedTopologyTransaction]
}

trait TopologyMappingChecks {
  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      pendingChanges: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit]
}

object NoopTopologyMappingChecks extends TopologyMappingChecks {
  override def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      pendingChanges: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] =
    EitherTUtil.unit
}

class ValidatingTopologyMappingChecks(
    store: TopologyStore[TopologyStoreId],
    val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends TopologyMappingChecks
    with NamedLogging {

  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    val checkFirstIsNotRemove = EitherTUtil
      .condUnitET(
        !(toValidate.operation == TopologyChangeOp.Remove && inStore.isEmpty),
        TopologyTransactionRejection.NoCorrespondingActiveTxToRevoke(toValidate.mapping),
      )
    val checkRemoveDoesNotChangeMapping = EitherT.fromEither[Future](
      inStore
        .collect {
          case expected
              if toValidate.operation == TopologyChangeOp.Remove && toValidate.mapping != expected.mapping =>
            TopologyTransactionRejection
              .RemoveMustNotChangeMapping(toValidate.mapping, expected.mapping)
        }
        .toLeft(())
    )

    lazy val checkOpt = (toValidate.mapping.code, inStore.map(_.mapping.code)) match {
      case (Code.DomainTrustCertificate, None | Some(Code.DomainTrustCertificate)) =>
        val checkReplace = toValidate
          .select[TopologyChangeOp.Replace, DomainTrustCertificate]
          .map(
            checkDomainTrustCertificateReplace(
              effective,
              _,
              inStore.flatMap(_.selectMapping[DomainTrustCertificate]),
              pendingChangesLookup,
            )
          )

        val checkRemove = toValidate
          .select[TopologyChangeOp.Remove, DomainTrustCertificate]
          .map(checkDomainTrustCertificateRemove(effective, _, pendingChangesLookup))

        checkReplace.orElse(checkRemove)

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
          .map(checkOwnerToKeyMappingReplace)

        val checkRemove = toValidate
          .select[TopologyChangeOp.Remove, OwnerToKeyMapping]
          .map(
            checkOwnerToKeyMappingRemove(
              effective,
              _,
              pendingChangesLookup,
            )
          )

        checkReplace.orElse(checkRemove)

      case (Code.MediatorDomainState, None | Some(Code.MediatorDomainState)) =>
        toValidate
          .select[TopologyChangeOp.Replace, MediatorDomainState]
          .map(
            checkMediatorDomainStateReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, MediatorDomainState]),
              pendingChangesLookup,
            )
          )
      case (Code.SequencerDomainState, None | Some(Code.SequencerDomainState)) =>
        toValidate
          .select[TopologyChangeOp.Replace, SequencerDomainState]
          .map(
            checkSequencerDomainStateReplace(
              effective,
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, SequencerDomainState]),
              pendingChangesLookup,
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
          .map(checkNamespaceDelegationReplace(effective, _, pendingChangesLookup))

      case (Code.DomainParametersState, None | Some(Code.DomainParametersState)) =>
        toValidate
          .select[TopologyChangeOp.Remove, DomainParametersState]
          .map(_ =>
            EitherT.leftT[Future, Unit](
              TopologyTransactionRejection
                .Other(
                  "Removal of DomainParameterState is not supported. Use Replace instead."
                ): TopologyTransactionRejection
            )
          )

      case _otherwise => None
    }

    for {
      _ <- checkFirstIsNotRemove
      _ <- checkRemoveDoesNotChangeMapping
      _ <- checkOpt.getOrElse(EitherTUtil.unit)
    } yield ()

  }

  private def loadHistoryFromStore(
      effectiveTime: EffectiveTime,
      code: Code,
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, Seq[GenericSignedTopologyTransaction]] =
    EitherT.right[TopologyTransactionRejection](
      store
        .inspect(
          proposals = false,
          // effective time has exclusive semantics, but TimeQuery.Range.until has always had inclusive semantics.
          // therefore, we take the immediatePredecessor here
          timeQuery =
            TimeQuery.Range(from = None, until = Some(effectiveTime.value.immediatePredecessor)),
          asOfExclusiveO = None,
          op = None,
          types = Seq(code),
          idFilter = None,
          namespaceFilter = None,
        )
        .map { storedTxs =>
          val pending = pendingChangesLookup.values
            .filter(pendingTx =>
              !pendingTx.isProposal && pendingTx.transaction.mapping.code == code
            )
          (storedTxs.result.map(_.transaction) ++ pending)
        }
    )

  @VisibleForTesting
  private[transaction] def loadFromStore(
      effective: EffectiveTime,
      codes: Set[Code],
      pendingChangesLookup: PendingChangesLookup,
      filterUid: Option[Seq[UniqueIdentifier]] = None,
      filterNamespace: Option[Seq[Namespace]] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, Seq[
    SignedTopologyTransaction[TopologyChangeOp.Replace, TopologyMapping]
  ]] =
    EitherT
      .right[TopologyTransactionRejection](
        store
          .findPositiveTransactions(
            effective.value,
            asOfInclusive = false,
            isProposal = false,
            types = codes.toSeq,
            filterUid = filterUid,
            filterNamespace = filterNamespace,
          )
          .map { storedTxs =>
            val latestStored = storedTxs.collectLatestByUniqueKey.signedTransactions

            // we need to proactively lookup the pending changes that match the filter,
            // because there might be a pending transaction that isn't in the store yet (eg. serial=1)
            val pendingChangesMatchingFilter =
              pendingChangesLookup.values.filter { tx =>
                // proposals shouldn't end up in PendingChangesLookup, but better to emulate what the store filter does
                !tx.isProposal &&
                codes.contains(tx.mapping.code) &&
                filterNamespace.forall(_.exists(_ == tx.mapping.namespace)) &&
                filterUid.forall(uids => tx.mapping.maybeUid.exists(uids.contains(_)))
              }

            TopologyTransactions
              .collectLatestByUniqueKey(
                Seq.empty[GenericSignedTopologyTransaction] ++
                  latestStored.result ++ pendingChangesMatchingFilter
              )
              .flatMap(_.selectOp[TopologyChangeOp.Replace])
          }
      )

  private def ensureParticipantDoesNotHostParties(
      effective: EffectiveTime,
      participantId: ParticipantId,
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext) =
    for {
      storedPartyToParticipantMappings <- loadFromStore(
        effective,
        Set(Code.PartyToParticipant),
        pendingChangesLookup,
      )
      participantHostsParties = storedPartyToParticipantMappings.view
        .flatMap(_.selectMapping[PartyToParticipant])
        .collect {
          case tx if tx.mapping.participants.exists(_.participantId == participantId) =>
            tx.mapping.partyId
        }
        .toSeq
      _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        participantHostsParties.isEmpty,
        TopologyTransactionRejection.ParticipantStillHostsParties(
          participantId,
          participantHostsParties,
        ),
      )
    } yield ()

  private def checkDomainTrustCertificateRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp, DomainTrustCertificate],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] =
    /* Checks that the DTC is not being removed if the participant still hosts a party.
     * This check is potentially quite expensive: we have to fetch all party to participant mappings, because
     * we cannot index by the hosting participants.
     */
    ensureParticipantDoesNotHostParties(
      effective,
      toValidate.mapping.participantId,
      pendingChangesLookup,
    )

  private def loadDomainParameters(
      effective: EffectiveTime,
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, DynamicDomainParameters] =
    loadFromStore(effective, Set(Code.DomainParametersState), pendingChangesLookup).subflatMap {
      domainParamCandidates =>
        val params = domainParamCandidates.view
          .flatMap(_.selectMapping[DomainParametersState])
          .map(_.mapping.parameters)
          .toList
        params match {
          case Nil =>
            logger.error(
              "Can not determine domain parameters."
            )
            Left(TopologyTransactionRejection.MissingDomainParameters(effective))
          case param :: Nil => Right(param)
          case param :: rest =>
            logger.error(
              s"Multiple domain parameters at $effective ${rest.size + 1}. Using first one: $param."
            )
            Right(param)
        }
    }

  private def checkDomainTrustCertificateReplace(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, DomainTrustCertificate],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp, DomainTrustCertificate]],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    // Checks if the participant is allowed to submit its domain trust certificate
    val participantId = toValidate.mapping.participantId

    def loadOnboardingRestriction()
        : EitherT[Future, TopologyTransactionRejection, OnboardingRestriction] =
      loadDomainParameters(effective, pendingChangesLookup).map(_.onboardingRestriction)

    def checkDomainIsNotLocked(restriction: OnboardingRestriction) =
      EitherTUtil.condUnitET(
        restriction.isOpen, {
          logger.info(
            s"Domain is locked at $effective. Rejecting onboarding of new participant ${toValidate.mapping}"
          )
          TopologyTransactionRejection
            .OnboardingRestrictionInPlace(
              participantId,
              restriction,
              None,
            )
        },
      )

    def checkParticipantIsNotRestricted(
        restrictions: OnboardingRestriction
    ): EitherT[Future, TopologyTransactionRejection, Unit] =
      // using the flags to check for restrictions instead of == UnrestrictedOpen to be more
      // future proof in case we will add additional restrictions in the future and would miss a case,
      // because there is no exhaustiveness check without full pattern matching
      if (restrictions.isUnrestricted && restrictions.isOpen) {
        // No further checks to be done. any participant can join the domain
        EitherTUtil.unit
      } else if (restrictions.isRestricted && restrictions.isOpen) {
        // Only participants with explicit permission may join the domain
        loadFromStore(
          effective,
          Set(Code.ParticipantDomainPermission),
          pendingChangesLookup,
          filterUid = Some(Seq(toValidate.mapping.participantId.uid)),
        ).subflatMap { storedPermissions =>
          val isAllowlisted = storedPermissions.view
            .flatMap(_.selectMapping[ParticipantDomainPermission])
            .collectFirst {
              case x if x.mapping.domainId == toValidate.mapping.domainId =>
                x.mapping.loginAfter
            }
          isAllowlisted match {
            case Some(Some(loginAfter)) if loginAfter > effective.value =>
              // this should not happen except under race conditions, as sequencers should not let participants login
              logger.warn(
                s"Rejecting onboarding of ${toValidate.mapping.participantId} as the participant still has a login ban until $loginAfter"
              )
              Left(
                TopologyTransactionRejection
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
                TopologyTransactionRejection
                  .OnboardingRestrictionInPlace(participantId, restrictions, None)
              )
          }
        }
      } else {
        EitherT.leftT(
          TopologyTransactionRejection
            .OnboardingRestrictionInPlace(participantId, restrictions, None)
        )
      }

    def checkPartyIdDoesntExist() = for {
      ptps <- loadFromStore(
        effective,
        Set(Code.PartyToParticipant),
        pendingChangesLookup,
        filterUid = Some(Seq(participantId.uid)),
      )
      conflictingPartyIdO = ptps
        .flatMap(_.selectMapping[PartyToParticipant])
        .headOption
        .map(_.mapping)
      _ <- conflictingPartyIdO match {
        case Some(ptp) =>
          isExplicitAdminPartyAllocation(
            ptp,
            TopologyTransactionRejection.ParticipantIdConflictWithPartyId(
              participantId,
              ptp.partyId,
            ),
          )
        case None => EitherTUtil.unit[TopologyTransactionRejection]
      }
    } yield ()

    def checkParticipantDoesNotRejoin() = EitherTUtil.condUnitET(
      inStore.forall(_.operation != TopologyChangeOp.Remove),
      TopologyTransactionRejection.MembersCannotRejoinDomain(Seq(toValidate.mapping.participantId)),
    )

    for {
      _ <- checkParticipantDoesNotRejoin()
      _ <- checkPartyIdDoesntExist()
      restriction <- loadOnboardingRestriction()
      _ <- checkDomainIsNotLocked(restriction)
      _ <- checkParticipantIsNotRestricted(restriction)
    } yield ()
  }
  private val requiredKeyPurposes = Set(KeyPurpose.Encryption, KeyPurpose.Signing)

  /** Checks the following:
    * - threshold is less than or equal to the number of confirming participants
    * - new participants have a valid DTC
    * - new participants have an OTK with at least 1 signing key and 1 encryption key
    */
  private def checkPartyToParticipant(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, Unit] = {
    import toValidate.mapping
    def checkParticipants() = {
      val newParticipants = mapping.participants.map(_.participantId).toSet --
        inStore.toList.flatMap(_.mapping.participants.map(_.participantId))
      for {
        participantTransactions <- loadFromStore(
          effective,
          Set(Code.DomainTrustCertificate, Code.OwnerToKeyMapping),
          pendingChangesLookup,
          filterUid = Some(newParticipants.toSeq.map(_.uid) :+ mapping.partyId.uid),
        )

        // if we found a DTC with the same uid as the partyId,
        // check that the PTP is an explicit admin party allocation, otherwise reject the PTP
        foundAdminPartyWithSameUID = participantTransactions
          .flatMap(_.selectMapping[DomainTrustCertificate])
          .exists(_.mapping.participantId.uid == mapping.partyId.uid)
        _ <- EitherTUtil.ifThenET(foundAdminPartyWithSameUID)(
          isExplicitAdminPartyAllocation(
            mapping,
            TopologyTransactionRejection.PartyIdConflictWithAdminParty(
              mapping.partyId
            ),
          )
        )

        // check that all participants are known on the domain
        missingParticipantCertificates = newParticipants -- participantTransactions
          .flatMap(_.selectMapping[DomainTrustCertificate])
          .map(_.mapping.participantId)

        _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
          missingParticipantCertificates.isEmpty,
          TopologyTransactionRejection.UnknownMembers(missingParticipantCertificates.toSeq),
        )

        // check that all known participants have keys registered
        participantsWithInsufficientKeys =
          newParticipants -- participantTransactions
            .flatMap(_.selectMapping[OwnerToKeyMapping])
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

    for {
      _ <- checkParticipants()
    } yield ()

  }

  private def checkOwnerToKeyMappingReplace(
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, OwnerToKeyMapping]
  ): EitherT[Future, TopologyTransactionRejection, Unit] = {
    // check for at least 1 signing and 1 encryption key
    val keysByPurpose = toValidate.mapping.keys.forgetNE.groupBy(_.purpose)
    val signingKeys = keysByPurpose.getOrElse(KeyPurpose.Signing, Seq.empty)

    val minimumSigningKeyRequirement =
      EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        // all nodes require signing keys
        signingKeys.nonEmpty,
        TopologyTransactionRejection.InvalidTopologyMapping(
          "OwnerToKeyMapping must contain at least 1 signing key."
        ),
      )

    val encryptionKeys = keysByPurpose.getOrElse(KeyPurpose.Encryption, Seq.empty)
    val isParticipant = toValidate.mapping.member.code == ParticipantId.Code

    val minimumEncryptionKeyRequirement =
      EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        // all nodes require signing keys
        // non-participants don't need encryption keys
        !isParticipant || encryptionKeys.nonEmpty,
        TopologyTransactionRejection.InvalidTopologyMapping(
          "OwnerToKeyMapping for participants must contain at least 1 encryption key."
        ),
      )
    minimumSigningKeyRequirement.flatMap(_ => minimumEncryptionKeyRequirement)
  }

  private def checkOwnerToKeyMappingRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Remove, OwnerToKeyMapping],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] =
    toValidate.mapping.member match {
      case participantId: ParticipantId =>
        ensureParticipantDoesNotHostParties(effective, participantId, pendingChangesLookup)
      case _ => EitherTUtil.unit
    }

  private def checkMediatorDomainStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorDomainState],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorDomainState]],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    val newMediators = (toValidate.mapping.allMediatorsInGroup.toSet -- inStore.toList.flatMap(
      _.mapping.allMediatorsInGroup
    )).map(identity[Member])

    def checkMediatorNotAlreadyAssignedToOtherGroup() =
      for {
        result <- loadFromStore(effectiveTime, Set(Code.MediatorDomainState), pendingChangesLookup)
        mediatorsAlreadyAssignedToGroups = result
          .flatMap(_.selectMapping[MediatorDomainState])
          .flatMap(tx =>
            tx.mapping.allMediatorsInGroup.collect {
              case med if newMediators.contains(med) => med -> tx.mapping.group
            }
          )
          .toMap
        _ <- EitherTUtil.condUnitET[Future](
          mediatorsAlreadyAssignedToGroups.isEmpty,
          TopologyTransactionRejection.MediatorsAlreadyInOtherGroups(
            toValidate.mapping.group,
            mediatorsAlreadyAssignedToGroups,
          ): TopologyTransactionRejection,
        )
      } yield ()

    def checkMediatorsDontRejoin(): EitherT[Future, TopologyTransactionRejection, Unit] =
      loadHistoryFromStore(effectiveTime, code = Code.MediatorDomainState, pendingChangesLookup)
        .flatMap { mdsHistory =>
          val allMediatorsPreviouslyOnDomain = mdsHistory.view
            .flatMap(_.selectMapping[MediatorDomainState])
            .flatMap(_.mapping.allMediatorsInGroup)
            .toSet[Member]
          val rejoiningMediators = newMediators.intersect(allMediatorsPreviouslyOnDomain)
          EitherTUtil.condUnitET(
            rejoiningMediators.isEmpty,
            TopologyTransactionRejection.MembersCannotRejoinDomain(rejoiningMediators.toSeq),
          )
        }

    for {
      _ <- checkMediatorNotAlreadyAssignedToOtherGroup()
      _ <- checkMediatorsDontRejoin()
    } yield ()
  }

  private def checkSequencerDomainStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerDomainState],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerDomainState]],
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    val newSequencers = (toValidate.mapping.allSequencers.toSet -- inStore.toList.flatMap(
      _.mapping.allSequencers
    )).map(identity[Member])

    def checkSequencersDontRejoin(): EitherT[Future, TopologyTransactionRejection, Unit] =
      loadHistoryFromStore(
        effectiveTime,
        code = Code.SequencerDomainState,
        pendingChangesLookup,
      )
        .flatMap { sdsHistory =>
          val allSequencersPreviouslyOnDomain = sdsHistory.view
            .flatMap(_.selectMapping[SequencerDomainState])
            .flatMap(_.mapping.allSequencers)
            .toSet[Member]
          val rejoiningSequencers = newSequencers.intersect(allSequencersPreviouslyOnDomain)
          EitherTUtil.condUnitET(
            rejoiningSequencers.isEmpty,
            TopologyTransactionRejection
              .MembersCannotRejoinDomain(rejoiningSequencers.toSeq),
          )
        }

    checkSequencersDontRejoin()
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
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {

    def checkDecentralizedNamespaceDerivedFromOwners()
        : EitherT[Future, TopologyTransactionRejection, Unit] =
      if (inStore.isEmpty) {
        // The very first decentralized namespace definition must have namespace computed from the owners
        EitherTUtil.condUnitET(
          toValidate.mapping.namespace == DecentralizedNamespaceDefinition
            .computeNamespace(toValidate.mapping.owners),
          InvalidTopologyMapping(
            s"The decentralized namespace ${toValidate.mapping.namespace} is not derived from the owners ${toValidate.mapping.owners.toSeq.sorted}"
          ),
        )
      } else {
        EitherTUtil.unit
      }

    def checkNoClashWithNamespaceDelegations()(implicit
        traceContext: TraceContext
    ): EitherT[Future, TopologyTransactionRejection, Unit] =
      loadFromStore(
        effective,
        Set(Code.NamespaceDelegation),
        pendingChangesLookup,
        filterUid = None,
        filterNamespace = Some(Seq(toValidate.mapping.namespace)),
      ).flatMap { namespaceDelegations =>
        EitherTUtil.condUnitET(
          namespaceDelegations.isEmpty,
          NamespaceAlreadyInUse(toValidate.mapping.namespace),
        )
      }

    def checkOwnersAreNormalNamespaces()(implicit
        traceContext: TraceContext
    ): EitherT[Future, TopologyTransactionRejection, Unit] =
      loadFromStore(
        effective,
        Set(Code.NamespaceDelegation),
        pendingChangesLookup,
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
          InvalidTopologyMapping(
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
      pendingChangesLookup: PendingChangesLookup,
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    def checkNoClashWithDecentralizedNamespaces()(implicit
        traceContext: TraceContext
    ): EitherT[Future, TopologyTransactionRejection, Unit] =
      loadFromStore(
        effective,
        Set(Code.DecentralizedNamespaceDefinition),
        pendingChangesLookup,
        filterUid = None,
        filterNamespace = Some(Seq(toValidate.mapping.namespace)),
      ).flatMap { dns =>
        val foundDecentralizedNamespaceWithSameNamespace = dns.nonEmpty
        EitherTUtil.condUnitET(
          !foundDecentralizedNamespaceWithSameNamespace,
          NamespaceAlreadyInUse(toValidate.mapping.namespace),
        )
      }

    checkNoClashWithDecentralizedNamespaces()
  }

  /** Checks whether the given PTP is considered an explicit admin party allocation. This is true if all following conditions are met:
    * <ul>
    *   <li>threshold == 1</li>
    *   <li>there is only a single hosting participant<li>
    *     <ul>
    *       <li>with Submission permission</li>
    *       <li>participantId.adminParty == partyId</li>
    *     </ul>
    *   </li>
    * </ul
    */
  private def isExplicitAdminPartyAllocation(
      ptp: PartyToParticipant,
      rejection: => TopologyTransactionRejection,
  ): EitherT[Future, TopologyTransactionRejection, Unit] = {
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

    EitherTUtil.condUnitET[Future](
      singleHostingParticipant && partyIsAdminParty && threshold1,
      rejection,
    )
  }

}
