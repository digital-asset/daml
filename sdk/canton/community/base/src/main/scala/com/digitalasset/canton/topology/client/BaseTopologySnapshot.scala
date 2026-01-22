// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{KeyPurpose, SigningKeyUsage, SigningKeysWithThreshold}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{
  DynamicSequencingParametersWithValidity,
  DynamicSynchronizerParameters,
  DynamicSynchronizerParametersWithValidity,
}
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient.PartyInfo
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.{
  PackageDependencyResolver,
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  UnknownOrUnvettedPackages,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.topology.transaction.{
  DynamicSequencingParametersState,
  HostingParticipant,
  MediatorSynchronizerState,
  OwnerToKeyMapping,
  ParticipantAttributes,
  ParticipantPermission,
  ParticipantSynchronizerPermission,
  PartyToKeyMapping,
  PartyToParticipant,
  SequencerSynchronizerState,
  SynchronizerParametersState,
  SynchronizerTrustCertificate,
  SynchronizerUpgradeAnnouncement,
  TopologyChangeOp,
  TopologyMapping,
  VettedPackage,
  VettedPackages,
}
import com.digitalasset.canton.topology.{
  KeyCollection,
  MediatorGroup,
  MediatorId,
  Member,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SequencerGroup,
  SequencerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.daml.lf.data.Ref.PackageId

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/** Base topology snapshot loader to share implementation between StoreBased- and
  * WriteThroughCache-TopologySnapshot.
  *
  * @param psid
  *   the physical synchronizer id
  * @param packageDependencyResolver
  *   provides a way determine the direct and indirect package dependencies.
  */
abstract class BaseTopologySnapshot(
    psid: PhysicalSynchronizerId,
    packageDependencyResolver: PackageDependencyResolver,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologySnapshotLoader
    with NamedLogging {

  protected def findTransactionsByUids(
      types: Seq[TopologyMapping.Code],
      filterUid: NonEmpty[Seq[UniqueIdentifier]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[StoredTopologyTransactions[TopologyChangeOp.Replace, TopologyMapping]]

  override def loadVettedPackages(
      participant: ParticipantId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[PackageId, VettedPackage]] =
    findTransactionsByUids(
      types = Seq(TopologyMapping.Code.VettedPackages),
      filterUid = NonEmpty(Seq, participant.uid),
    ).map { transactions =>
      collectLatestMapping(
        TopologyMapping.Code.VettedPackages,
        transactions.collectOfMapping[VettedPackages].result,
      ).toList.flatMap(_.packages.map(vp => (vp.packageId, vp))).toMap
    }

  override def loadVettedPackages(participants: Set[ParticipantId])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, Map[PackageId, VettedPackage]]] =
    NonEmpty
      .from(participants.map(_.uid))
      .map { participantsNE =>
        findTransactionsByUids(
          types = Seq(TopologyMapping.Code.VettedPackages),
          filterUid = participantsNE.toSeq,
        ).map { transactions =>
          transactions
            .collectOfMapping[VettedPackages]
            .result
            .groupBy(_.mapping.participantId)
            .view
            .mapValues { txs =>
              collectLatestMapping(TopologyMapping.Code.VettedPackages, txs).toList
                .flatMap(_.packages.map(vp => (vp.packageId, vp)))
                .toMap
            }
            .toMap
        }
      }
      .getOrElse(FutureUnlessShutdown.pure(Map.empty))

  override private[client] def findUnvettedPackagesOrDependencies(
      participant: ParticipantId,
      packages: Set[PackageId],
      ledgerTime: CantonTimestamp,
      vettedPackages: Map[PackageId, VettedPackage],
  )(implicit traceContext: TraceContext): UnknownOrUnvettedPackages = {
    def isValid(pkg: PackageId): Boolean =
      vettedPackages.get(pkg).exists(_.validAt(ledgerTime))

    val invalidPackages = packages.filterNot(isValid)
    val validPackages = packages -- invalidPackages
    packageDependencyResolver.packageDependencies(validPackages) match {
      case Left(unknownPackages) =>
        UnknownOrUnvettedPackages(
          unknown = Map(unknownPackages),
          unvetted = if (invalidPackages.isEmpty) Map.empty else Map(participant -> invalidPackages),
        )
      case Right(dependencies) =>
        UnknownOrUnvettedPackages.unvetted(
          participant,
          invalidPackages ++ dependencies.filterNot(isValid),
        )
    }
  }

  override def findDynamicSynchronizerParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSynchronizerParametersWithValidity]] =
    findTransactionsByUids(
      types = Seq(TopologyMapping.Code.SynchronizerParametersState),
      filterUid = NonEmpty(Seq, psid.uid),
    ).map { transactions =>
      for {
        storedTx <- collectLatestTransaction(
          TopologyMapping.Code.SynchronizerParametersState,
          transactions
            .collectOfMapping[SynchronizerParametersState]
            .result,
        ).toRight(s"Unable to fetch synchronizer parameters at $timestamp")

        synchronizerParameters = {
          val mapping = storedTx.mapping
          DynamicSynchronizerParametersWithValidity(
            mapping.parameters,
            storedTx.validFrom.value,
            storedTx.validUntil.map(_.value),
          )
        }
      } yield synchronizerParameters
    }

  override def findDynamicSequencingParameters()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, DynamicSequencingParametersWithValidity]] =
    findTransactionsByUids(
      types = Seq(TopologyMapping.Code.SequencingDynamicParametersState),
      filterUid = NonEmpty(Seq, psid.uid),
    ).map { transactions =>
      for {
        storedTx <- collectLatestTransaction(
          TopologyMapping.Code.SequencingDynamicParametersState,
          transactions
            .collectOfMapping[DynamicSequencingParametersState]
            .result,
        ).toRight(s"Unable to fetch sequencing parameters at $timestamp")

        mapping = storedTx.mapping
      } yield DynamicSequencingParametersWithValidity(
        mapping.parameters,
        storedTx.validFrom.value,
        storedTx.validUntil.map(_.value),
        mapping.synchronizerId,
      )
    }

  override private[client] def loadActiveParticipantsOf(
      party: PartyId,
      participantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[PartyInfo] =
    loadBatchActiveParticipantsOf(Seq(party), participantStates).map(
      _.getOrElse(party, PartyInfo.EmptyPartyInfo)
    )

  override private[client] def loadBatchActiveParticipantsOf(
      parties: Seq[PartyId],
      loadParticipantStates: Seq[ParticipantId] => FutureUnlessShutdown[
        Map[ParticipantId, ParticipantAttributes]
      ],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[PartyId, PartyInfo]] = {

    def collectLatestByType[M <: TopologyMapping: ClassTag](
        storedTransactions: StoredTopologyTransactions[
          TopologyChangeOp.Replace,
          TopologyMapping,
        ],
        code: TopologyMapping.Code,
    ): Seq[M] =
      storedTransactions
        .collectOfMapping[M]
        .result
        .groupBy(_.mapping.uniqueKey)
        .map { case (_, seq) =>
          collectLatestMapping[M](
            code,
            seq.sortBy(_.validFrom),
          ).getOrElse(
            throw new IllegalStateException(
              "Group-by would not have produced empty PartyToParticipant seq"
            )
          )
        }
        .toSeq

    for {
      // get all party to participant mappings and also participant states for this uid (latter to mix in admin parties)
      partyData <- NonEmpty
        .from(parties.map(_.uid))
        .map { partiesNE =>
          findTransactionsByUids(
            types = Seq(
              TopologyMapping.Code.PartyToParticipant,
              TopologyMapping.Code.SynchronizerTrustCertificate,
            ),
            filterUid = partiesNE,
          )
            .map { storedTransactions =>
              // find normal party declarations
              val partyToParticipantMappings = collectLatestByType[PartyToParticipant](
                storedTransactions,
                TopologyMapping.Code.PartyToParticipant,
              ).map { ptp =>
                ptp.partyId -> (ptp.threshold, ptp.participants.map {
                  case HostingParticipant(participantId, partyPermission, onboarding) =>
                    participantId -> (partyPermission, onboarding)
                }.toMap)
              }.toMap

              // admin parties are implicitly defined by the fact that a participant is available on a synchronizer.
              // admin parties have the same UID as their participant
              val synchronizerTrustCerts = collectLatestByType[SynchronizerTrustCertificate](
                storedTransactions,
                TopologyMapping.Code.SynchronizerTrustCertificate,
              ).map(cert => cert.participantId)

              (partyToParticipantMappings, synchronizerTrustCerts)
            }
        }
        .getOrElse(
          FutureUnlessShutdown.pure(
            Map.empty[
              PartyId,
              (PositiveInt, Map[ParticipantId, (ParticipantPermission, Boolean)]),
            ] -> Seq.empty
          )
        )
      (partyToParticipantMap, adminPartyParticipants) = partyData

      // fetch all admin parties
      participantIds = partyToParticipantMap.values.flatMap { case (_, participants) =>
        participants.keys
      }.toSeq ++ adminPartyParticipants

      participantToAttributesMap <- loadParticipantStates(participantIds)

      adminPartiesMap = adminPartyParticipants
        .mapFilter(participantId =>
          participantToAttributesMap
            .get(participantId)
            .map(attrs =>
              // participant admin parties are never consortium parties
              participantId.adminParty -> PartyInfo
                .nonConsortiumPartyInfo(Map(participantId -> attrs))
            )
        )
        .toMap

      // In case the party->participant mapping contains participants missing from map returned
      // by loadParticipantStates, filter out participants with "empty" permissions and transitively
      // parties whose participants have all been filtered out this way.
      // this can only affect participants that have left the synchronizer
      partiesToPartyInfos = {
        val p2pMappings = partyToParticipantMap.toSeq.mapFilter {
          case (partyId, (threshold, participantToPermissionsMap)) =>
            val participantIdToAttribs = participantToPermissionsMap.toSeq.mapFilter {
              case (participantId, (partyPermission, onboarding)) =>
                participantToAttributesMap
                  .get(participantId)
                  .map { participantAttributes =>
                    // Use the lower permission between party and the permission granted to the participant by the synchronizer
                    val reducedPermission =
                      ParticipantPermission.lowerOf(
                        partyPermission,
                        participantAttributes.permission,
                      )
                    participantId -> ParticipantAttributes(
                      reducedPermission,
                      participantAttributes.loginAfter,
                      onboarding = onboarding,
                    )
                  }
            }.toMap
            if (participantIdToAttribs.isEmpty) None
            else Some(partyId -> PartyInfo(threshold, participantIdToAttribs))
        }.toMap
        // In case of conflicting mappings, the admin party takes higher precedence
        // Note that conflicts are prevented in TopologyMappingChecks.
        p2pMappings ++ adminPartiesMap
      }
      // For each party we must return a result to satisfy the expectations of the
      // calling CachingTopologySnapshot's caffeine partyCache per findings in #11598.
      // This includes parties not found in the topology store or parties filtered out
      // above, e.g. parties whose participants have left the synchronizer.
      fullySpecifiedPartyMap = parties.map { party =>
        party -> partiesToPartyInfos.getOrElse(party, PartyInfo.EmptyPartyInfo)
      }.toMap
    } yield fullySpecifiedPartyMap
  }

  // TODO(#28232) this can be removed as this is now an invariant enforced on the topology store
  private def findMembersWithoutSigningKeys[T <: Member](members: Seq[T])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[T]] =
    signingKeys(members, SigningKeyUsage.All).map(keys =>
      members.filter(keys.get(_).forall(_.isEmpty)).toSet
    )

  /** returns the list of currently known mediator groups */
  override def mediatorGroups()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[MediatorGroup]] = {
    def fetchMediatorSynchronizerStates()
        : FutureUnlessShutdown[Seq[StoredTopologyTransaction[Replace, MediatorSynchronizerState]]] =
      findTransactionsByUids(
        types = Seq(TopologyMapping.Code.MediatorSynchronizerState),
        filterUid = NonEmpty(Seq, psid.uid),
      ).map(_.collectOfMapping[MediatorSynchronizerState].result)

    for {
      transactions <- fetchMediatorSynchronizerStates()
      mediatorsInAllGroups = transactions.flatMap(_.mapping.allMediatorsInGroup)
      mediatorsWithoutSigningKeys <- findMembersWithoutSigningKeys(mediatorsInAllGroups)
    } yield {
      transactions
        .groupBy(_.mapping.group)
        .map { case (groupId, seq) =>
          val mds = collectLatestMapping(
            TopologyMapping.Code.MediatorSynchronizerState,
            seq.sortBy(_.validFrom),
          )
            .getOrElse(
              throw new IllegalStateException("Group-by would not have produced empty seq")
            )
          MediatorGroup(
            groupId,
            mds.active.filterNot(mediatorsWithoutSigningKeys),
            mds.observers.filterNot(mediatorsWithoutSigningKeys),
            mds.threshold,
          )
        }
        .toSeq
        .sortBy(_.index)
    }
  }

  override def sequencerGroup()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SequencerGroup]] = {
    def fetchSequencerSynchronizerState() = findTransactionsByUids(
      types = Seq(TopologyMapping.Code.SequencerSynchronizerState),
      filterUid = NonEmpty(Seq, psid.uid),
    ).map { transactions =>
      collectLatestMapping(
        TopologyMapping.Code.SequencerSynchronizerState,
        transactions.collectOfMapping[SequencerSynchronizerState].result,
      )
    }
    for {
      sds <- fetchSequencerSynchronizerState()
      allSequencers = sds.toList.flatMap(_.allSequencers)
      sequencersWithoutSigningKeys <- findMembersWithoutSigningKeys(allSequencers)
    } yield {
      sds.map { (sds: SequencerSynchronizerState) =>
        SequencerGroup(
          sds.active.filterNot(sequencersWithoutSigningKeys),
          sds.observers.filterNot(sequencersWithoutSigningKeys),
          sds.threshold,
        )
      }
    }
  }

  private val keysRequiredForParticipants = Set(KeyPurpose.Signing, KeyPurpose.Encryption)

  private def getParticipantsWithCertificates(
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping]
  )(implicit traceContext: TraceContext): Map[ParticipantId, SynchronizerTrustCertificate] =
    storedTxs
      .collectOfMapping[SynchronizerTrustCertificate]
      .result
      .groupBy(_.mapping.participantId)
      .flatMap { case (pid, seq) =>
        collectLatestMapping(
          TopologyMapping.Code.SynchronizerTrustCertificate,
          seq.sortBy(_.validFrom),
        ).map(pid -> _)
      }

  // TODO(#28232) this can be removed as this is now an invariant enforced on the topology store
  private def getParticipantsWithCertAndKeys(
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping],
      participantsWithCertificates: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Set[ParticipantId] =
    storedTxs
      .collectOfMapping[OwnerToKeyMapping]
      .result
      .groupBy(_.mapping.member)
      .collect {
        case (pid: ParticipantId, seq)
            if participantsWithCertificates(pid) && collectLatestMapping(
              TopologyMapping.Code.OwnerToKeyMapping,
              seq.sortBy(_.validFrom),
            ).exists(otk =>
              keysRequiredForParticipants.diff(otk.keys.forgetNE.map(_.purpose).toSet).isEmpty
            ) =>
          pid
      }
      .toSet

  private def getParticipantSynchronizerPermissions(
      storedTxs: StoredTopologyTransactions[Replace, TopologyMapping],
      participantsWithCertAndKeys: Set[ParticipantId],
  )(implicit traceContext: TraceContext): Map[ParticipantId, ParticipantSynchronizerPermission] =
    storedTxs
      .collectOfMapping[ParticipantSynchronizerPermission]
      .result
      .groupBy(_.mapping.participantId)
      .collect {
        case (pid, seq) if participantsWithCertAndKeys(pid) =>
          val mapping =
            collectLatestMapping(
              TopologyMapping.Code.ParticipantSynchronizerPermission,
              seq.sortBy(_.validFrom),
            )
              .getOrElse(
                throw new IllegalStateException("Group-by would not have produced empty seq")
              )
          pid -> mapping
      }

  private def loadParticipantStatesHelper(
      participantsFilter: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    for {
      // Looks up synchronizer parameters for default rate limits.
      synchronizerParametersState <- findTransactionsByUids(
        types = Seq(
          TopologyMapping.Code.SynchronizerParametersState
        ),
        filterUid = NonEmpty(Seq, psid.uid),
      ).map(transactions =>
        collectLatestMapping(
          TopologyMapping.Code.SynchronizerParametersState,
          transactions.collectOfMapping[SynchronizerParametersState].result,
        ).getOrElse(
          throw new IllegalStateException(
            s"Unable to locate synchronizer parameters state at $timestamp"
          )
        )
      )
      storedTxs <- NonEmpty
        .from(participantsFilter.map(_.uid))
        .map { participantsNE =>
          findTransactionsByUids(
            types = Seq(
              TopologyMapping.Code.SynchronizerTrustCertificate,
              // TODO(#28232) this can be removed as this is now an invariant enforced on the topology store
              TopologyMapping.Code.OwnerToKeyMapping,
              TopologyMapping.Code.ParticipantSynchronizerPermission,
            ),
            filterUid = participantsNE,
          )
        }
        .getOrElse(FutureUnlessShutdown.pure(StoredTopologyTransactions.empty))

    } yield {
      // 1. Participant needs to have requested access to synchronizer by issuing a synchronizer trust certificate
      val participantsWithCertificates = getParticipantsWithCertificates(storedTxs)
      val participantsIdsWithCertificates = participantsWithCertificates.keySet
      // 2. Participant needs to have keys registered on the synchronizer
      // TODO(#28232) this can be removed as this is now an invariant enforced on the topology store
      val participantsWithCertAndKeys =
        getParticipantsWithCertAndKeys(storedTxs, participantsIdsWithCertificates)
      // Warn about participants with cert but no keys
      (participantsIdsWithCertificates -- participantsWithCertAndKeys).foreach { pid =>
        logger.warn(
          s"Participant $pid has a synchronizer trust certificate, but no keys on synchronizer ${synchronizerParametersState.synchronizerId}"
        )
      }
      // 3. Attempt to look up permissions/trust from participant synchronizer permission
      val participantSynchronizerPermissions =
        getParticipantSynchronizerPermissions(storedTxs, participantsWithCertAndKeys)

      participantsWithCertAndKeys.toSeq.mapFilter { pid =>
        val supportedFeatures =
          participantsWithCertificates.get(pid).toList.flatMap(_.featureFlags)
        if (
          synchronizerParametersState.parameters.onboardingRestriction.isRestricted && !participantSynchronizerPermissions
            .contains(pid)
        ) {
          // 4a. If the synchronizer is restricted, we must have found a ParticipantSynchronizerPermission for the participants, otherwise
          // the participants shouldn't have been able to onboard to the synchronizer in the first place.
          // In case we don't find a ParticipantSynchronizerPermission, we don't return the participant with default permissions, but we skip it.
          logger.warn(
            s"Unable to find ParticipantSynchronizerPermission for participant $pid on synchronizer ${synchronizerParametersState.synchronizerId} with onboarding restrictions ${synchronizerParametersState.parameters.onboardingRestriction} at $referenceTime"
          )
          None
        } else {
          val permissions = participantSynchronizerPermissions
            .getOrElse(
              pid,
              ParticipantSynchronizerPermission
                .default(synchronizerParametersState.synchronizerId, pid),
            )
            .setDefaultLimitIfNotSet(
              DynamicSynchronizerParameters.defaultParticipantSynchronizerLimits
            )
          // 4b. Apply default permissions/trust of submission/ordinary if missing participant synchronizer permission and
          // grab rate limits from dynamic synchronizer parameters if not specified
          Some(
            pid -> ParticipantAttributes(
              permissions.permission,
              permissions.loginAfter,
              supportedFeatures,
            )
          )
        }
      }.toMap
    }

  override def loadParticipantStates(
      participants: Seq[ParticipantId]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[ParticipantId, ParticipantAttributes]] =
    if (participants.isEmpty)
      FutureUnlessShutdown.pure(Map())
    else
      loadParticipantStatesHelper(participants)

  /** abstract loading function used to obtain the full key collection for a key owner */
  override def allKeys(owner: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[KeyCollection] =
    allKeys(Seq(owner)).map(_.getOrElse(owner, KeyCollection.empty))

  override def allKeys(
      members: Seq[Member]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Map[Member, KeyCollection]] =
    NonEmpty
      .from(members)
      .map(membersNE =>
        findTransactionsByUids(
          types = Seq(TopologyMapping.Code.OwnerToKeyMapping),
          filterUid = membersNE.map(_.uid),
        ).map { transactions =>
          transactions
            .collectOfMapping[OwnerToKeyMapping]
            .result
            .groupBy(_.mapping.member)
            .map { case (member, otks) =>
              val keys = collectLatestMapping[OwnerToKeyMapping](
                TopologyMapping.Code.OwnerToKeyMapping,
                otks.sortBy(_.validFrom),
              ).toList.flatMap(_.keys.forgetNE)
              member -> KeyCollection.empty.addAll(keys)
            }
        }
      )
      .getOrElse(FutureUnlessShutdown.pure(Map.empty))

  override def isMemberKnown(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    areMembersKnown(Set(member)).map(_.nonEmpty)

  override def areMembersKnown(
      members: Set[Member]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Set[Member]] = {
    val participants = members.collect { case ParticipantId(uid) => uid }
    val mediators = members.collect { case MediatorId(uid) => uid }
    val sequencers = members.collect { case SequencerId(uid) => uid }

    val knownParticipantsF = NonEmpty
      .from(participants)
      .map { participantsNE =>
        findTransactionsByUids(
          types = Seq(SynchronizerTrustCertificate.code),
          filterUid = participantsNE.toSeq,
        ).map { txs =>
          txs
            .collectOfMapping[SynchronizerTrustCertificate]
            .result
            .map(_.mapping.participantId: Member)
            .toSet
        }
      }
      .getOrElse(FutureUnlessShutdown.pure(Set.empty[Member]))

    val knownMediatorsF = if (mediators.nonEmpty) {
      findTransactionsByUids(
        types = Seq(MediatorSynchronizerState.code),
        filterUid = NonEmpty(Seq, psid.uid),
      ).map(
        _.collectOfMapping[MediatorSynchronizerState].result
          .flatMap(_.mapping.allMediatorsInGroup.collect {
            case med if mediators.contains(med.uid) => med: Member
          })
          .toSet
      )
    } else FutureUnlessShutdown.pure(Set.empty[Member])

    val knownSequencersF = if (sequencers.nonEmpty) {
      findTransactionsByUids(
        types = Seq(SequencerSynchronizerState.code),
        filterUid = NonEmpty(Seq, psid.uid),
      ).map(
        _.collectOfMapping[SequencerSynchronizerState].result
          .flatMap(_.mapping.allSequencers.collect {
            case seq if sequencers.contains(seq.uid) => seq: Member
          })
          .toSet
      )
    } else FutureUnlessShutdown.pure(Set.empty[Member])

    for {
      knownParticipants <- knownParticipantsF
      knownMediators <- knownMediatorsF
      knownSequencers <- knownSequencersF
    } yield knownParticipants ++ knownMediators ++ knownSequencers
  }

  override def signingKeysWithThreshold(party: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[SigningKeysWithThreshold]] =
    findTransactionsByUids(
      types = Seq(TopologyMapping.Code.PartyToKeyMapping, Code.PartyToParticipant),
      filterUid = NonEmpty(Seq, party.uid),
    ).map { transactions =>
      collectLatestMapping[PartyToParticipant](
        Code.PartyToParticipant,
        transactions.collectOfMapping[PartyToParticipant].result,
      )
        .flatMap(_.partySigningKeysWithThreshold)
        .orElse {
          collectLatestMapping[PartyToKeyMapping](
            Code.PartyToKeyMapping,
            transactions.collectOfMapping[PartyToKeyMapping].result,
          )
            .map(_.signingKeysWithThreshold)
        }
    }

  override def synchronizerUpgradeOngoing()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(SynchronizerSuccessor, EffectiveTime)]] =
    findTransactionsByUids(
      types = Seq(TopologyMapping.Code.SynchronizerUpgradeAnnouncement),
      filterUid = NonEmpty(Seq, psid.uid),
    ).map(_.collectOfMapping[SynchronizerUpgradeAnnouncement].result.toList match {
      case atMostOne @ (_ :: Nil | Nil) =>
        atMostOne
          .map(tx =>
            (
              SynchronizerSuccessor(tx.mapping.successorSynchronizerId, tx.mapping.upgradeTime),
              tx.validFrom,
            )
          )
          .headOption
      case _moreThanOne =>
        ErrorUtil.invalidState("Found more than one SynchronizerUpgradeAnnouncement mapping")
    })

  protected def collectLatestMapping[T <: TopologyMapping](
      typ: TopologyMapping.Code,
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp.Replace, T]],
  )(implicit traceContext: TraceContext): Option[T] =
    collectLatestTransaction(typ, transactions).map(_.mapping)

  protected def collectLatestTransaction[T <: TopologyMapping](
      typ: TopologyMapping.Code,
      transactions: Seq[StoredTopologyTransaction[TopologyChangeOp.Replace, T]],
  )(implicit
      traceContext: TraceContext
  ): Option[StoredTopologyTransaction[TopologyChangeOp.Replace, T]] = {
    val result = transactions.maxByOption(_.validFrom)
    if (transactions.sizeIs > 1) {
      logger.error(
        show"Expected unique \"${typ.code}\" at $referenceTime, but found multiple instances. Using last one with serial ${result
            .map(_.serial)}"
      )
    }
    result
  }
}
