// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.data.EitherT
import cats.instances.future.*
import cats.instances.order.*
import cats.syntax.semigroup.*
import com.digitalasset.canton.crypto.KeyPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, OnboardingRestriction}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.StoredTopologyTransactions.PositiveStoredTopologyTransactions
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.{
  InvalidTopologyMapping,
  NamespaceAlreadyInUse,
}
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

trait TopologyMappingChecks {
  def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit]
}

object NoopTopologyMappingChecks extends TopologyMappingChecks {
  override def checkTransaction(
      effective: EffectiveTime,
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
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
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    val checkFirstIsNotRemove = EitherTUtil
      .condUnitET(
        !(toValidate.operation == TopologyChangeOp.Remove && inStore.isEmpty),
        TopologyTransactionRejection.NoCorrespondingActiveTxToRevoke(toValidate.mapping),
      )

    lazy val checkOpt = (toValidate.mapping.code, inStore.map(_.mapping.code)) match {
      case (Code.DomainTrustCertificate, None | Some(Code.DomainTrustCertificate)) =>
        val checkReplace = toValidate
          .select[TopologyChangeOp.Replace, DomainTrustCertificate]
          .map(checkDomainTrustCertificateReplace(effective, _))

        val checkRemove = toValidate
          .select[TopologyChangeOp.Remove, DomainTrustCertificate]
          .map(checkDomainTrustCertificateRemove(effective, _))

        checkReplace.orElse(checkRemove)

      case (Code.PartyToParticipant, None | Some(Code.PartyToParticipant)) =>
        toValidate
          .select[TopologyChangeOp.Replace, PartyToParticipant]
          .map(
            checkPartyToParticipant(
              _,
              inStore.flatMap(_.select[TopologyChangeOp.Replace, PartyToParticipant]),
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
            )
          )

      case (Code.AuthorityOf, None | Some(Code.AuthorityOf)) =>
        toValidate
          .select[TopologyChangeOp.Replace, AuthorityOf]
          .map(checkAuthorityOf(effective, _))

      case (
            Code.DecentralizedNamespaceDefinition,
            None | Some(Code.DecentralizedNamespaceDefinition),
          ) =>
        toValidate
          .select[TopologyChangeOp.Replace, DecentralizedNamespaceDefinition]
          .map(
            checkDecentralizedNamespaceDefinitionReplace(
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
          .map(checkNamespaceDelegationReplace)

      case otherwise => None
    }

    checkFirstIsNotRemove
      .flatMap(_ => checkOpt.getOrElse(EitherTUtil.unit))
  }

  private def loadFromStore(
      effective: EffectiveTime,
      code: Code,
      filterUid: Option[Seq[UniqueIdentifier]] = None,
      filterNamespace: Option[Seq[Namespace]] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, PositiveStoredTopologyTransactions] =
    EitherT
      .right[TopologyTransactionRejection](
        store
          .findPositiveTransactions(
            effective.value,
            asOfInclusive = false,
            isProposal = false,
            types = Seq(code),
            filterUid = filterUid,
            filterNamespace = filterNamespace,
          )
      )

  private def ensureParticipantDoesNotHostParties(
      effective: EffectiveTime,
      participantId: ParticipantId,
  )(implicit traceContext: TraceContext) = {
    for {
      storedPartyToParticipantMappings <- loadFromStore(effective, PartyToParticipant.code, None)
      participantHostsParties = storedPartyToParticipantMappings.result.view
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

  }

  private def checkDomainTrustCertificateRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp, DomainTrustCertificate],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    /* Checks that the DTC is not being removed if the participant still hosts a party.
     * This check is potentially quite expensive: we have to fetch all party to participant mappings, because
     * we cannot index by the hosting participants.
     */
    ensureParticipantDoesNotHostParties(effective, toValidate.mapping.participantId)
  }

  private def loadDomainParameters(
      effective: EffectiveTime
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, DynamicDomainParameters] = {
    loadFromStore(effective, DomainParametersState.code).subflatMap { domainParamCandidates =>
      val params = domainParamCandidates.result.view
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
            s"Multiple domain parameters at ${effective} ${rest.size + 1}. Using first one: $param."
          )
          Right(param)
      }
    }

  }

  private def checkDomainTrustCertificateReplace(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp, DomainTrustCertificate],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    // Checks if the participant is allowed to submit its domain trust certificate
    val participantId = toValidate.mapping.participantId

    def loadOnboardingRestriction()
        : EitherT[Future, TopologyTransactionRejection, OnboardingRestriction] = {
      loadDomainParameters(effective).map(_.onboardingRestriction)
    }

    def checkDomainIsNotLocked(restriction: OnboardingRestriction) = {
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
    }

    def checkParticipantIsNotRestricted(
        restrictions: OnboardingRestriction
    ): EitherT[Future, TopologyTransactionRejection, Unit] = {
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
          ParticipantDomainPermission.code,
          filterUid = Some(Seq(toValidate.mapping.participantId.uid)),
        ).subflatMap { storedPermissions =>
          val isAllowlisted = storedPermissions.result.view
            .flatMap(_.selectMapping[ParticipantDomainPermission])
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
      } else {
        EitherT.leftT(
          TopologyTransactionRejection
            .OnboardingRestrictionInPlace(participantId, restrictions, None)
        )
      }
    }

    for {
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
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, Unit] = {
    import toValidate.mapping
    def checkParticipants() = {
      val newParticipants = mapping.participants.map(_.participantId).toSet --
        inStore.toList.flatMap(_.mapping.participants.map(_.participantId))
      for {
        participantTransactions <- EitherT.right[TopologyTransactionRejection](
          store
            .findPositiveTransactions(
              CantonTimestamp.MaxValue,
              asOfInclusive = false,
              isProposal = false,
              types = Seq(DomainTrustCertificate.code, OwnerToKeyMapping.code),
              filterUid = Some(newParticipants.toSeq.map(_.uid)),
              filterNamespace = None,
            )
        )

        // check that all participants are known on the domain
        missingParticipantCertificates = newParticipants -- participantTransactions
          .collectOfMapping[DomainTrustCertificate]
          .result
          .map(_.mapping.participantId)

        _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
          missingParticipantCertificates.isEmpty,
          TopologyTransactionRejection.UnknownMembers(missingParticipantCertificates.toSeq),
        )

        // check that all known participants have keys registered
        participantsWithInsufficientKeys =
          newParticipants -- participantTransactions
            .collectOfMapping[OwnerToKeyMapping]
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

    def checkHostingLimits(effective: EffectiveTime) = for {
      hostingLimitsCandidates <- loadFromStore(
        effective,
        code = PartyHostingLimits.code,
        filterUid = Some(Seq(toValidate.mapping.partyId.uid)),
      )
      hostingLimits = hostingLimitsCandidates.result.view
        .flatMap(_.selectMapping[PartyHostingLimits])
        .map(_.mapping.quota)
        .toList
      partyHostingLimit = hostingLimits match {
        case Nil => // No hosting limits found. This is expected if no restrictions are in place
          None
        case quota :: Nil => Some(quota)
        case multiple @ (quota :: _) =>
          logger.error(
            s"Multiple PartyHostingLimits at ${effective} ${multiple.size}. Using first one with quota $quota."
          )
          Some(quota)
      }
      // TODO(#14050) load default party hosting limits from dynamic domain parameters in case the party
      //              doesn't have a specific PartyHostingLimits mapping issued by the domain.
      _ <- partyHostingLimit match {
        case Some(limit) =>
          EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
            toValidate.mapping.participants.size <= limit,
            TopologyTransactionRejection.PartyExceedsHostingLimit(
              toValidate.mapping.partyId,
              limit,
              toValidate.mapping.participants.size,
            ),
          )
        case None => EitherTUtil.unit[TopologyTransactionRejection]
      }
    } yield ()

    for {
      _ <- checkParticipants()
      _ <- checkHostingLimits(EffectiveTime.MaxValue)
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
        (!isParticipant || encryptionKeys.nonEmpty),
        TopologyTransactionRejection.InvalidTopologyMapping(
          "OwnerToKeyMapping for participants must contain at least 1 encryption key."
        ),
      )
    minimumSigningKeyRequirement.flatMap(_ => minimumEncryptionKeyRequirement)
  }

  private def checkOwnerToKeyMappingRemove(
      effective: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Remove, OwnerToKeyMapping],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    toValidate.mapping.member match {
      case participantId: ParticipantId =>
        ensureParticipantDoesNotHostParties(effective, participantId)
      case _ => EitherTUtil.unit
    }
  }

  private def checkMissingNsdAndOtkMappings(
      effectiveTime: EffectiveTime,
      members: Set[Member],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {

    val otks = loadFromStore(
      effectiveTime,
      OwnerToKeyMapping.code,
      filterUid = Some(members.toSeq.map(_.uid)),
    )

    val nsds = loadFromStore(
      effectiveTime,
      NamespaceDelegation.code,
      filterNamespace = Some(members.toSeq.map(_.namespace)),
    )

    for {
      otks <- otks
      nsds <- nsds

      membersWithOTK = otks.result.flatMap(
        _.selectMapping[OwnerToKeyMapping].map(_.mapping.member)
      )
      missingOTK = members -- membersWithOTK

      rootCertificates = nsds.result
        .flatMap(_.selectMapping[NamespaceDelegation].filter(_.mapping.isRootDelegation))
        .map(_.mapping.namespace)
        .toSet
      missingNSD = members.filter(med => !rootCertificates.contains(med.namespace))
      otk = missingOTK.map(_ -> Seq(OwnerToKeyMapping.code)).toMap
      nsd = missingNSD.map(_ -> Seq(NamespaceDelegation.code)).toMap
      missingMappings = otk.combine(nsd)
      _ <- EitherTUtil.condUnitET[Future][TopologyTransactionRejection](
        missingOTK.isEmpty && missingNSD.isEmpty,
        TopologyTransactionRejection.MissingMappings(
          missingMappings.view.mapValues(_.sortBy(_.dbInt)).toMap
        ),
      )
    } yield {}
  }

  private def checkMediatorDomainStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorDomainState],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp.Replace, MediatorDomainState]],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    val newMediators = (toValidate.mapping.allMediatorsInGroup.toSet -- inStore.toList.flatMap(
      _.mapping.allMediatorsInGroup
    )).map(identity[Member])
    checkMissingNsdAndOtkMappings(effectiveTime, newMediators)
  }

  private def checkSequencerDomainStateReplace(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerDomainState],
      inStore: Option[SignedTopologyTransaction[TopologyChangeOp.Replace, SequencerDomainState]],
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    val newSequencers = (toValidate.mapping.allSequencers.toSet -- inStore.toList.flatMap(
      _.mapping.allSequencers
    )).map(identity[Member])

    checkMissingNsdAndOtkMappings(effectiveTime, newSequencers)
  }

  private def checkAuthorityOf(
      effectiveTime: EffectiveTime,
      toValidate: SignedTopologyTransaction[TopologyChangeOp.Replace, AuthorityOf],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TopologyTransactionRejection, Unit] = {
    def checkPartiesAreKnown(): EitherT[Future, TopologyTransactionRejection, Unit] = {
      val allPartiesToLoad = toValidate.mapping.partyId +: toValidate.mapping.parties
      loadFromStore(
        effectiveTime,
        Code.PartyToParticipant,
        filterUid = Some(allPartiesToLoad.map(_.uid)),
      ).flatMap { partyMappings =>
        val knownParties = partyMappings.result
          .flatMap(_.selectMapping[PartyToParticipant])
          .map(_.mapping.partyId)

        val missingParties = allPartiesToLoad.toSet -- knownParties

        EitherTUtil.condUnitET(
          missingParties.isEmpty,
          TopologyTransactionRejection.UnknownParties(missingParties.toSeq.sorted),
        )
      }
    }

    checkPartiesAreKnown()
  }

  private def checkDecentralizedNamespaceDefinitionReplace(
      toValidate: SignedTopologyTransaction[
        TopologyChangeOp.Replace,
        DecentralizedNamespaceDefinition,
      ],
      inStore: Option[SignedTopologyTransaction[
        TopologyChangeOp,
        DecentralizedNamespaceDefinition,
      ]],
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

    def checkNoClashWithRootCertificates()(implicit
        traceContext: TraceContext
    ): EitherT[Future, TopologyTransactionRejection, Unit] = {
      loadFromStore(
        EffectiveTime.MaxValue,
        Code.NamespaceDelegation,
        filterUid = None,
        filterNamespace = Some(Seq(toValidate.mapping.namespace)),
      ).flatMap { namespaceDelegations =>
        val foundRootCertWithSameNamespace = namespaceDelegations.result.exists(stored =>
          NamespaceDelegation.isRootCertificate(stored.transaction)
        )
        EitherTUtil.condUnitET(
          !foundRootCertWithSameNamespace,
          NamespaceAlreadyInUse(toValidate.mapping.namespace),
        )
      }
    }

    for {
      _ <- checkDecentralizedNamespaceDerivedFromOwners()
      _ <- checkNoClashWithRootCertificates()
    } yield ()
  }

  private def checkNamespaceDelegationReplace(
      toValidate: SignedTopologyTransaction[
        TopologyChangeOp.Replace,
        NamespaceDelegation,
      ]
  )(implicit traceContext: TraceContext): EitherT[Future, TopologyTransactionRejection, Unit] = {
    def checkNoClashWithDecentralizedNamespaces()(implicit
        traceContext: TraceContext
    ): EitherT[Future, TopologyTransactionRejection, Unit] = {
      EitherTUtil.ifThenET(NamespaceDelegation.isRootCertificate(toValidate)) {
        loadFromStore(
          EffectiveTime.MaxValue,
          Code.DecentralizedNamespaceDefinition,
          filterUid = None,
          filterNamespace = Some(Seq(toValidate.mapping.namespace)),
        ).flatMap { dns =>
          val foundDecentralizedNamespaceWithSameNamespace = dns.result.nonEmpty
          EitherTUtil.condUnitET(
            !foundDecentralizedNamespaceWithSameNamespace,
            NamespaceAlreadyInUse(toValidate.mapping.namespace),
          )
        }
      }
    }

    checkNoClashWithDecentralizedNamespaces()
  }
}
